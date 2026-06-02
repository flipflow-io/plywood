/*
 * Copyright 2015-2020 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Duration, Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import {
  immutableArraysEqual,
  immutableLookupsEqual,
  NamedArray,
  SimpleArray,
} from 'immutable-class';
import { PlywoodRequester } from 'plywood-base-api';
import { PassThrough, ReadableStream, Transform, Writable } from 'readable-stream';

import {
  AttributeInfo,
  AttributeJSs,
  Attributes,
  Dataset,
  Datum,
  NumberRange,
  PlywoodValue,
  PlywoodValueBuilder,
} from '../datatypes';
import { Ip } from '../datatypes/ip';
import { Set } from '../datatypes/set';
import { StringRange } from '../datatypes/stringRange';
import { TimeRange } from '../datatypes/timeRange';
import { iteratorFactory, PlyBit } from '../datatypes/valueStream';
import { SQLDialect } from '../dialect/baseDialect';
import {
  $,
  AndExpression,
  ApplyExpression,
  ChainableExpression,
  ChainableUnaryExpression,
  Expression,
  ExternalExpression,
  FallbackExpression,
  FilterExpression,
  LimitExpression,
  LiteralExpression,
  NumberBucketExpression,
  OverlapExpression,
  r,
  RefExpression,
  SelectExpression,
  SortExpression,
  SplitExpression,
  SqlRefExpression,
  ThenExpression,
  TimeBucketExpression,
  TimeFloorExpression,
  TimeShiftExpression,
} from '../expressions';
import { ExpressionJS } from '../expressions/baseExpression';
import { ReadableError } from '../helper/streamBasics';
import { StreamConcat } from '../helper/streamConcat';
import { nonEmptyLookup, pipeWithError, safeRange } from '../helper/utils';
import { DatasetFullType, FullType, PlyType, PlyTypeSimple } from '../types';

import { CustomDruidAggregations, CustomDruidTransforms } from './utils/druidTypes';

/**
 * Render the SQL for an aggregate expression detached from its
 * External, qualifying column refs to a given table alias. Used by
 * the native-JOIN path which renders measure SQL without going
 * through External.addExpression (the aggregate's operand is still
 * a literal `$main` dataset ref, which `apply.getSQL` would try to
 * render and fail with "unsupported type DATASET").
 *
 * The aggregate's `_getSQLChainableUnaryHelper` is protected. We
 * call through a public façade: the dialect's helpers know how to
 * render each Aggregate subclass once we hand them the right pieces:
 *   - operandSQL: stub (any string without ' WHERE '). The aggregate
 *     uses it only to detect per-apply filters; native-JOIN has none.
 *   - expressionSQL: the inner ref's SQL, qualified to `tableAlias`.
 *
 * Returns null if the apply contains an aggregator the renderer
 * doesn't know how to handle (in which case the gate should never
 * have routed it here — we leave the diagnostic to the caller).
 */
function renderAggregateSQL(
  applyValueExpr: Expression,
  dialect: SQLDialect,
  tableAlias: string,
): string {
  // The apply's expression is typically the aggregate directly
  // (countDistinct, sum, etc.) or a binary chain (sum(a) + sum(b)).
  // For v1 we handle the simple-aggregate case — the gate routes
  // away non-decomposable measures before the chain rewrite would
  // even apply.
  const escapeQualified = (refName: string) => `${tableAlias}.${dialect.escapeName(refName)}`;
  const renderInnerRef = (ex: Expression): string => {
    if (ex instanceof RefExpression) return escapeQualified(ex.name);
    // Fallback: emit via getSQL — works for non-DATASET expressions
    // because the aggregate's inner is typically a single ref. Set
    // table context so any nested ref qualifies correctly.
    const prev = (dialect as any).table;
    (dialect as any).setTable(tableAlias);
    try {
      return ex.getSQL(dialect);
    } finally {
      (dialect as any).setTable(prev);
    }
  };

  const ex: any = applyValueExpr;
  if (!ex || !ex.op) {
    throw new PlywoodUnsupportedNativeJoinShape(
      `renderAggregateSQL: measure expression has no op (${
        applyValueExpr ? applyValueExpr.toString() : String(applyValueExpr)
      }); cannot render as a native-JOIN aggregate.`,
    );
  }
  const op = ex.op;
  // Stub operandSQL: any string lacking ' WHERE ' satisfies
  // aggregateFilterIfNeeded's "no per-apply filter" branch.
  const stubOperand = `${tableAlias}`;
  switch (op) {
    // Arithmetic composition (Ismael's RP/PVP-diff bug). A derived measure such as
    // `(avg(price) - avg(pvp)) / avg(pvp)` has a root op of `divide`/`subtract`/etc,
    // not a single aggregate — yet it is perfectly renderable on the native-JOIN
    // path. The native JOIN is ONE GROUP BY over the already-joined rows (each main
    // row maps to exactly one lookup row via the inner join, so no fan-out), so
    // AVG/SUM/COUNT(DISTINCT) are computed natively at the split grain and a ratio
    // of those aggregates is just SQL arithmetic over them. We recurse into each
    // operand (which bottoms out in a single aggregate this switch already renders,
    // a nested arithmetic op, or a literal) and combine via the expression's OWN
    // `_getSQLChainableUnaryHelper` — so the SQL (and the div-by-zero behaviour, e.g.
    // Druid's `floatDivision` num*1.0/den) is identical to what plywood emits
    // everywhere else; we add no CASE WHEN of our own. A genuinely unrenderable leaf
    // (quantile/sqlAggregate/custom) still hits the default-throw below during the
    // recursion, naming THAT op — composing arithmetic never silences a bad leaf.
    case 'divide':
    case 'subtract':
    case 'multiply':
    case 'add':
    case 'power': {
      const chain = ex as ChainableUnaryExpression;
      const renderOperand = (sub: Expression): string => {
        // A literal operand (e.g. the `1` in `(avg/avg) - 1`) renders directly;
        // it carries no aggregate and must not recurse into the aggregate switch.
        if (sub instanceof LiteralExpression) return sub.getSQL(dialect);
        return renderAggregateSQL(sub, dialect, tableAlias);
      };
      const operandSQL = renderOperand(chain.operand);
      const expressionSQL = renderOperand(chain.expression);
      // Reuse the expression's own SQL renderer (protected — reached via the same
      // `as any` façade the rest of this file already uses for dialect internals).
      return (chain as any)._getSQLChainableUnaryHelper(dialect, operandSQL, expressionSQL);
    }
    case 'count':
      return dialect.aggregateFilterIfNeeded(stubOperand, 'COUNT(*)', '0');
    case 'sum':
      return `SUM(${dialect.aggregateFilterIfNeeded(
        stubOperand,
        renderInnerRef(ex.expression),
        '0',
      )})`;
    case 'min':
      return `MIN(${dialect.aggregateFilterIfNeeded(stubOperand, renderInnerRef(ex.expression))})`;
    case 'max':
      return `MAX(${dialect.aggregateFilterIfNeeded(stubOperand, renderInnerRef(ex.expression))})`;
    case 'average':
      return `AVG(${dialect.aggregateFilterIfNeeded(stubOperand, renderInnerRef(ex.expression))})`;
    case 'countDistinct': {
      const inner = renderInnerRef(ex.expression);
      const refName =
        ex.expression instanceof RefExpression ? (ex.expression as RefExpression).name : undefined;
      return dialect.countDistinctExpression(
        dialect.aggregateFilterIfNeeded(stubOperand, inner),
        refName,
      );
    }
    default:
      // Fail-loud (P2): an unrecognised op reaching here means the caller
      // routed a measure shape this renderer does not understand (e.g. a
      // derived `divide`/`subtract` expression that was not decomposed into
      // single-aggregate leaves first). Returning null previously let the
      // caller silently drop the SELECT item while the ORDER BY / GROUP BY
      // still referenced it, emitting malformed SQL that died at the engine.
      // Throw so the bad query never reaches the requester.
      throw new PlywoodUnsupportedNativeJoinShape(
        `renderAggregateSQL: cannot render measure expression with root op='${op}' ` +
          `(${applyValueExpr.toString()}) as a single native-JOIN aggregate. Native-JOIN ` +
          `expects each projected measure to be one aggregate (count/sum/min/max/average/` +
          `countDistinct). Derived measures must be decomposed into single-aggregate leaves ` +
          `before reaching this renderer.`,
      );
  }
}

/**
 * Fail-loud exception thrown by `External.assertDatasetShape` when a
 * dataset returned by a cross-source query path has more rows than
 * distinct key tuples — the engine-level symptom of a missing
 * post-join re-aggregation. The message names the duplicated tuple
 * and the row-count delta (rows minus distinct tuples).
 *
 * Surfaced synchronously, never logged. See INV-1 / INV-4 in the
 * cross-source aggregation fix spec.
 */
export class PlywoodCardinalityViolation extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PlywoodCardinalityViolation';
    // Restore prototype — ES5 transpilation of Error subclasses loses it,
    // which breaks `instanceof` checks downstream. See
    // https://github.com/Microsoft/TypeScript-wiki/blob/master/Breaking-Changes.md#extending-built-ins-like-error-array-and-map-may-no-longer-work.
    Object.setPrototypeOf(this, PlywoodCardinalityViolation.prototype);
  }
}

/**
 * Thrown by `Expression.isMeasureDecomposable` when an `Aggregate`
 * subclass reachable from a measure expression lacks the static
 * `decomposable: DecomposeTrait` declaration. Single source of truth
 * (INV-3): a missing trait is never silently treated as decomposable.
 */
export class PlywoodTraitMissing extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PlywoodTraitMissing';
    Object.setPrototypeOf(this, PlywoodTraitMissing.prototype);
  }
}

/**
 * Thrown by `External.getNativeJoinDecomposition` when the gate
 * routes a query into the native-JOIN path but the concrete shape
 * is not (yet) supported by the v1 SQL emitter — e.g. multi-alias
 * linked-only split, multi-linkedSource native-JOIN, native Druid
 * engine with no `SQLDialect`, or a split that is not a bare
 * RefExpression. Fail-loud: the caller MUST NOT swallow this and
 * fall back to the single-source path, because that path surfaces
 * downstream as a cryptic "could not get attribute info for X"
 * from the Druid inflater. F4 of the cycle-2 plan.
 */
export class PlywoodUnsupportedNativeJoinShape extends Error {
  constructor(reason: string) {
    super(`Cross-source native-JOIN cannot emit SQL: ${reason}`);
    this.name = 'PlywoodUnsupportedNativeJoinShape';
    Object.setPrototypeOf(this, PlywoodUnsupportedNativeJoinShape.prototype);
  }
}

export class TotalContainer {
  public datum: Datum;

  constructor(d: Datum) {
    this.datum = d;
  }

  toJS(): any {
    return {
      datum: Dataset.datumToJS(this.datum),
    };
  }
}

export type NextFn<Q> = (prevQuery: Q, prevResultLength: number, prevMeta: any) => Q;

export interface QueryAndPostTransform<T> {
  query: T;
  context?: Record<string, any>;
  postTransform: Transform;
  next?: NextFn<T>;
}

export type Inflater = (d: Datum) => void;

export type QuerySelection = 'any' | 'no-top-n' | 'group-by-only';

export type IntrospectionDepth = 'deep' | 'default' | 'shallow';

export interface IntrospectOptions {
  depth?: IntrospectionDepth;
  deep?: boolean; // legacy proxy for depth: "deep"
}

// Check to see if an expression is of the form timeRef.overlap(mainRange).then(timeRef).fallback(timeRef.timeShift(some_duration))
interface HybridTimeBreakdown {
  timeRef: RefExpression;
  mainRangeLiteral: LiteralExpression;
  timeShift: TimeShiftExpression;
}

export type QueryMode = 'raw' | 'value' | 'total' | 'split';

function makeDate(thing: any) {
  let dt = new Date(thing);
  if (isNaN(dt.valueOf())) dt = new Date(Number(thing)); // in case v === "1442018760000"
  return dt;
}

function nullMap<T, Q>(xs: T[], fn: (x: T) => Q): Q[] {
  if (!xs) return null;
  const res: Q[] = [];
  for (const x of xs) {
    const y = fn(x);
    if (y) res.push(y);
  }
  return res.length ? res : null;
}

function filterToAnds(filter: Expression): Expression[] {
  if (filter.equals(Expression.TRUE)) return [];
  if (filter instanceof AndExpression) return filter.getExpressionList();
  return [filter];
}

function filterDiff(strongerFilter: Expression, weakerFilter: Expression): Expression {
  const strongerFilterAnds = filterToAnds(strongerFilter);
  const weakerFilterAnds = filterToAnds(weakerFilter);
  if (weakerFilterAnds.length > strongerFilterAnds.length) return null;
  for (let i = 0; i < weakerFilterAnds.length; i++) {
    if (!weakerFilterAnds[i].equals(strongerFilterAnds[i])) return null;
  }
  return Expression.and(strongerFilterAnds.slice(weakerFilterAnds.length));
}

function getCommonFilter(filter1: Expression, filter2: Expression): Expression {
  const filter1Ands = filterToAnds(filter1);
  const filter2Ands = filterToAnds(filter2);
  const minLength = Math.min(filter1Ands.length, filter2Ands.length);
  const commonExpressions: Expression[] = [];
  for (let i = 0; i < minLength; i++) {
    if (!filter1Ands[i].equals(filter2Ands[i])) break;
    commonExpressions.push(filter1Ands[i]);
  }
  return Expression.and(commonExpressions);
}

function mergeDerivedAttributes(
  derivedAttributes1: Record<string, Expression>,
  derivedAttributes2: Record<string, Expression>,
): Record<string, Expression> {
  const derivedAttributes: Record<string, Expression> = Object.create(null);
  for (const k in derivedAttributes1) {
    derivedAttributes[k] = derivedAttributes1[k];
  }
  for (const k in derivedAttributes2) {
    if (hasOwnProp(derivedAttributes, k) && !derivedAttributes[k].equals(derivedAttributes2[k])) {
      throw new Error(`can not currently redefine conflicting ${k}`);
    }
    derivedAttributes[k] = derivedAttributes2[k];
  }
  return derivedAttributes;
}

function getSampleValue(valueType: string, ex: Expression): PlywoodValue {
  switch (valueType) {
    case 'NULL':
      return null;

    case 'BOOLEAN':
      return true;

    case 'NUMBER':
      return 4;

    case 'NUMBER_RANGE':
      if (ex instanceof NumberBucketExpression) {
        return new NumberRange({
          start: ex.offset,
          end: ex.offset + ex.size,
        });
      } else {
        return new NumberRange({ start: 0, end: 1 });
      }

    case 'TIME':
      return new Date('2015-03-14T00:00:00Z');

    case 'TIME_RANGE':
      if (ex instanceof TimeBucketExpression) {
        const timezone = ex.timezone || Timezone.UTC;
        const start = ex.duration.floor(new Date('2015-03-14T00:00:00Z'), timezone);
        return new TimeRange({
          start,
          end: ex.duration.shift(start, timezone, 1),
        });
      } else {
        return new TimeRange({
          start: new Date('2015-03-14T00:00:00Z'),
          end: new Date('2015-03-15T00:00:00Z'),
        });
      }

    case 'IP':
      return Ip.fromString('127.0.0.1');

    case 'STRING':
      if (ex instanceof RefExpression) {
        return 'some_' + ex.name;
      } else {
        return 'something';
      }

    case 'SET/STRING':
      if (ex instanceof RefExpression) {
        return Set.fromJS([ex.name + '1']);
      } else {
        return Set.fromJS(['something']);
      }

    case 'STRING_RANGE':
      if (ex instanceof RefExpression) {
        return StringRange.fromJS({ start: 'some_' + ex.name, end: null });
      } else {
        return StringRange.fromJS({ start: 'something', end: null });
      }

    default:
      if (ex instanceof SqlRefExpression) {
        return null;
      }

      throw new Error('unsupported simulation on: ' + valueType);
  }
}

function immutableAdd<T>(obj: Record<string, T>, key: string, value: T): Record<string, T> {
  const newObj = Object.create(null);
  for (const k in obj) newObj[k] = obj[k];
  newObj[key] = value;
  return newObj;
}

function findApplyByExpression(
  applies: ApplyExpression[],
  expression: Expression,
): ApplyExpression {
  for (const apply of applies) {
    if (apply.expression.equals(expression)) return apply;
  }
  return null;
}

/**
 * Declarative configuration for a foreign datasource joined to a main
 * External under cross-source decomposition. Every semantic choice that
 * affects how decomposition routes splits, injects synthetic keys, or
 * merges rows MUST live here explicitly — no heuristic inference from
 * schema shape or column types.
 *
 * Shapes:
 *
 *   joinKeys              Declared anchor of the join. Columns that,
 *                         when equal on both sides, identify a
 *                         corresponding row pair. Used for:
 *                           - classifying split aliases as `shared`
 *                             (a split whose refs all land in joinKeys
 *                             is shared automatically),
 *                           - auto-injecting synthetic `__join_<key>`
 *                             splits when the user picks no shared
 *                             split (subset controlled by
 *                             `autoInjectJoinKeys`).
 *
 *   autoInjectJoinKeys    Subset of `joinKeys` that the engine may
 *                         synthesize as `__join_<key>` splits when
 *                         the user's query has no shared split. Any
 *                         joinKey NOT listed here is valid as an
 *                         explicit user-chosen shared split but never
 *                         auto-added. Omit to default to `joinKeys`.
 *                         Typical use: `joinKeys: [partitionId,
 *                         __time]`, `autoInjectJoinKeys: [partitionId]`
 *                         — __time is a valid join anchor only when
 *                         the user explicitly splits on timeBucket,
 *                         otherwise the auto-inject would explode the
 *                         result by snapshot count.
 *
 *   sharedDimensions      Columns present on BOTH sides that carry the
 *                         same semantic meaning (e.g. a productName
 *                         surfaced on linked via derivedAttribute).
 *                         When the user splits on one of these, both
 *                         sides group by it at the SQL level and the
 *                         natural-join aligns equal values. Unlike
 *                         joinKeys, sharedDimensions aren't the join
 *                         anchor — they're additional columns that
 *                         behave like shared splits when referenced.
 *                         MUST be declared: the decomposition refuses
 *                         to silently infer shared-ness from schema
 *                         overlap because the same column name on
 *                         both sides can legitimately mean different
 *                         things.
 *
 *   joinMode              Row-retention semantic of the in-memory join:
 *                           'inner' — drop main rows with no linked
 *                             match (typical for monitoring cubes
 *                             where a split on a linked-only column
 *                             implies the row exists only if linked
 *                             has data),
 *                           'left'  — keep orphan main rows with
 *                             linked columns undefined (use when the
 *                             main side is authoritative and linked
 *                             contributes optional enrichment).
 *                         No default — every cube declares which.
 */
export interface LinkedSourceConfig {
  source: string;
  joinKeys: string[];
  autoInjectJoinKeys?: string[];
  sharedDimensions?: string[];
  joinMode?: 'inner' | 'left';
  attributes?: Attributes;
  derivedAttributes?: Record<string, Expression>;
  /**
   * How main's filter relates to the linked source's time dimension:
   *   - undefined / "bucketed" — main's __time clause propagates to the
   *     lookup query (default; correct for time-partitioned joins).
   *   - "eternal"              — __time is withheld from the linked
   *     schema so `pruneFilterToSchema` rewrites its refs to TRUE.
   *     Used for materialised snapshots whose rows live at a sentinel
   *     timestamp (e.g. MSQ-ingested lookups at 1970).
   */
  timeAlignment?: 'bucketed' | 'eternal';
  /**
   * Cross-engine override. When set, plywood routes THIS linkedSource's
   * sub-query to the named engine instead of inheriting the main
   * external's `engine`. Used when a linkedSource lives in a different
   * store than the main datasource — e.g. a magic-attribute staging view
   * materialised in Postgres (`magic_staging_<attrId>_rev<N>`) joined
   * against a main Druid datasource during the `materializing` phase.
   *
   *   - absent  → inherit the main external's engine/version/requester
   *   - present → override; `requester` MUST also be supplied (a foreign
   *               engine cannot be driven by the main external's
   *               requester — that would dispatch the SQL to the wrong
   *               store). Failure to supply it throws, never falls back.
   *
   * `requester` is intentionally NOT serialised through toJS/fromJS — it
   * is a live host handle injected at runtime, exactly like the main
   * external's own `requester`. `engine` and `version` DO round-trip.
   */
  engine?: string;
  version?: string;
  requester?: PlywoodRequester<any>;
  /**
   * PER-REQUEST ONLY (never authored on a cube config, never serialised).
   * `pruneLinkedFilterRefsInTree` harvests a linked-only filter clause that
   * targets a column living only on this lookup and parks it here on a fresh
   * per-request config copy. Consumers: the JS-join leaf path (`templateFilter`)
   * and the native-JOIN WHERE (both in getCrossExternalDecomposition), and the
   * totals/main-split semijoin-to-root (getSemijoinToRootDecomposition).
   */
  filter?: Expression;
  /**
   * PER-REQUEST ONLY. Set true by `pruneLinkedFilterRefsInTree` when the
   * harvested `filter` is ORPHANED — no value apply or split references this
   * lookup, so the established decomposition path would never materialise a
   * sub-query to honour it. Authorises the totals/main-split semijoin-to-root
   * rescue. False/absent when a sibling sub-query already consumes the filter.
   */
  semijoinToRoot?: boolean;
}

/**
 * JS-form of LinkedSourceConfig — attributes and derivedAttributes come
 * in as JS descriptors, to be rehydrated by `fromJS`.
 */
export interface LinkedSourceConfigJS {
  source: string;
  joinKeys: string[];
  autoInjectJoinKeys?: string[];
  sharedDimensions?: string[];
  joinMode?: 'inner' | 'left';
  attributes?: AttributeJSs;
  derivedAttributes?: Record<string, ExpressionJS>;
  timeAlignment?: 'bucketed' | 'eternal';
  // Cross-engine override — see LinkedSourceConfig.engine for the contract.
  // `requester` is NOT part of the JS form; it is injected at runtime.
  engine?: string;
  version?: string;
}

export interface SpecialApplyTransform {
  mainRangeLiteral: LiteralExpression;
  curTimeRange: TimeRange;
  prevTimeRange: TimeRange;
}

export interface ExternalValue {
  engine?: string;
  version?: string;
  suppress?: boolean;
  source?: string | string[];
  rollup?: boolean;
  attributes?: Attributes;
  attributeOverrides?: Attributes;
  derivedAttributes?: Record<string, Expression>;
  linkedSources?: Record<string, LinkedSourceConfig>;
  delegates?: External[];
  concealBuckets?: boolean;
  mode?: QueryMode;
  dataName?: string;
  rawAttributes?: Attributes;
  filter?: Expression;
  valueExpression?: Expression;
  select?: SelectExpression;
  split?: SplitExpression;
  applies?: ApplyExpression[];
  sort?: SortExpression;
  limit?: LimitExpression;
  havingFilter?: Expression;
  specialApplyTransform?: SpecialApplyTransform;

  // SQL

  withQuery?: string;

  // Druid

  timeAttribute?: string;
  customAggregations?: CustomDruidAggregations;
  customTransforms?: CustomDruidTransforms;
  allowEternity?: boolean;
  allowSelectQueries?: boolean;
  exactResultsOnly?: boolean;
  querySelection?: QuerySelection;
  context?: Record<string, any>;

  requester?: PlywoodRequester<any>;
}

export interface ExternalJS {
  engine: string;
  version?: string;
  source?: string | string[];
  rollup?: boolean;
  attributes?: AttributeJSs;
  attributeOverrides?: AttributeJSs;
  derivedAttributes?: Record<string, ExpressionJS>;
  linkedSources?: Record<string, LinkedSourceConfigJS>;
  filter?: ExpressionJS;
  rawAttributes?: AttributeJSs;
  concealBuckets?: boolean;

  // SQL

  withQuery?: string;

  // Druid

  timeAttribute?: string;
  customAggregations?: CustomDruidAggregations;
  customTransforms?: CustomDruidTransforms;
  allowEternity?: boolean;
  allowSelectQueries?: boolean;
  exactResultsOnly?: boolean;
  querySelection?: QuerySelection;
  context?: Record<string, any>;
}

export interface ApplySegregation {
  aggregateApplies: ApplyExpression[];
  postAggregateApplies: ApplyExpression[];
}

export interface AttributesAndApplies {
  attributes?: Attributes;
  applies?: ApplyExpression[];
}

export abstract class External {
  static type = 'EXTERNAL';

  static SEGMENT_NAME = '__SEGMENT__';
  static VALUE_NAME = '__VALUE__';

  static isExternal(candidate: any): candidate is External {
    return candidate instanceof External;
  }

  static extractVersion(v: string): string {
    if (!v) return null;
    const m = v.match(/^\d+\.\d+\.\d+(?:-[\w\-]+)?/);
    return m ? m[0] : null;
  }

  static versionLessThan(va: string, vb: string): boolean {
    const pa = va.split('-')[0].split('.');
    const pb = vb.split('-')[0].split('.');
    if (pa[0] !== pb[0]) return Number(pa[0]) < Number(pb[0]);
    if (pa[1] !== pb[1]) return Number(pa[1]) < Number(pb[1]);
    return Number(pa[2]) < Number(pb[2]);
  }

  static deduplicateExternals(externals: External[]): External[] {
    if (externals.length < 2) return externals;
    const uniqueExternals = [externals[0]];

    function addToUniqueExternals(external: External) {
      for (const uniqueExternal of uniqueExternals) {
        if (uniqueExternal.equalBaseAndFilter(external)) return;
      }
      uniqueExternals.push(external);
    }

    for (let i = 1; i < externals.length; i++) addToUniqueExternals(externals[i]);
    return uniqueExternals;
  }

  static addExtraFilter(ex: Expression, extraFilter: Expression): Expression {
    if (extraFilter.equals(Expression.TRUE)) return ex;

    return ex.substitute(ex => {
      if (
        ex instanceof RefExpression &&
        ex.type === 'DATASET' &&
        ex.name === External.SEGMENT_NAME
      ) {
        return ex.filter(extraFilter);
      }
      return null;
    });
  }

  static makeZeroDatum(applies: ApplyExpression[]): Datum {
    const newDatum = Object.create(null);
    for (const apply of applies) {
      const applyName = apply.name;
      if (applyName[0] === '_') continue;
      newDatum[applyName] = 0;
    }
    return newDatum;
  }

  static normalizeAndAddApply(
    attributesAndApplies: AttributesAndApplies,
    apply: ApplyExpression,
  ): AttributesAndApplies {
    const { attributes, applies } = attributesAndApplies;

    const expressions: Record<string, Expression> = Object.create(null);
    for (const existingApply of applies) expressions[existingApply.name] = existingApply.expression;
    apply = apply.changeExpression(
      apply.expression.resolveWithExpressions(expressions, 'leave').simplify(),
    );

    return {
      attributes: NamedArray.overrideByName(
        attributes,
        new AttributeInfo({ name: apply.name, type: apply.expression.type }),
      ),
      applies: NamedArray.overrideByName(applies, apply),
    };
  }

  static segregationAggregateApplies(applies: ApplyExpression[]): ApplySegregation {
    const aggregateApplies: ApplyExpression[] = [];
    const postAggregateApplies: ApplyExpression[] = [];
    let nameIndex = 0;

    // First extract all the simple cases
    const appliesToSegregate: ApplyExpression[] = [];
    for (const apply of applies) {
      const applyExpression = apply.expression;
      if (applyExpression.isAggregate()) {
        // This is a vanilla aggregate, just push it in.
        aggregateApplies.push(apply);
      } else {
        appliesToSegregate.push(apply);
      }
    }

    // Now do all the segregation
    for (const apply of appliesToSegregate) {
      const newExpression = apply.expression.substitute(ex => {
        if (ex.isAggregate()) {
          const existingApply = findApplyByExpression(aggregateApplies, ex);
          if (existingApply) {
            return $(existingApply.name, ex.type);
          } else {
            const name = '!T_' + nameIndex++;
            aggregateApplies.push(Expression._.apply(name, ex));
            return $(name, ex.type);
          }
        }
        return null;
      });

      postAggregateApplies.push(apply.changeExpression(newExpression));
    }

    return {
      aggregateApplies,
      postAggregateApplies,
    };
  }

  static getCommonFilterFromExternals(externals: External[]): Expression {
    if (!externals.length) throw new Error('must have externals');
    let commonFilter = externals[0].filter;
    for (let i = 1; i < externals.length; i++) {
      commonFilter = getCommonFilter(commonFilter, externals[i].filter);
    }
    return commonFilter;
  }

  static getMergedDerivedAttributesFromExternals(
    externals: External[],
  ): Record<string, Expression> {
    if (!externals.length) throw new Error('must have externals');
    let derivedAttributes = externals[0].derivedAttributes;
    for (let i = 1; i < externals.length; i++) {
      derivedAttributes = mergeDerivedAttributes(derivedAttributes, externals[i].derivedAttributes);
    }
    return derivedAttributes;
  }

  // ==== Inflaters

  static getIntelligentInflater(expression: Expression, label: string): Inflater {
    if (expression instanceof NumberBucketExpression) {
      return External.numberRangeInflaterFactory(label, expression.size);
    } else if (expression instanceof TimeBucketExpression) {
      return External.timeRangeInflaterFactory(label, expression.duration, expression.timezone);
    } else {
      return External.getSimpleInflater(expression.type, label);
    }
  }

  static getSimpleInflater(type: PlyType, label: string): Inflater {
    switch (type) {
      case 'BOOLEAN':
        return External.booleanInflaterFactory(label);
      case 'NULL':
        return External.nullInflaterFactory(label);
      case 'NUMBER':
        return External.numberInflaterFactory(label);
      case 'STRING':
        return External.stringInflaterFactory(label);
      case 'TIME':
        return External.timeInflaterFactory(label);
      case 'IP':
        return External.ipInflaterFactory(label);
      default:
        return null;
    }
  }

  static booleanInflaterFactory(label: string): Inflater {
    return (d: any) => {
      if (typeof d[label] === 'undefined') {
        d[label] = null;
        return;
      }

      const v = '' + d[label];
      switch (v) {
        case 'null':
          d[label] = null;
          break;

        case '1':
        case 'true':
          d[label] = true;
          break;

        default:
          // '0', 'false', everything else
          d[label] = false;
          break;
      }
    };
  }

  static timeRangeInflaterFactory(label: string, duration: Duration, timezone: Timezone): Inflater {
    return (d: any) => {
      const v = d[label];
      if ('' + v === 'null') {
        d[label] = null;
        return;
      }

      const start = makeDate(v);
      d[label] = new TimeRange({ start, end: duration.shift(start, timezone) });
    };
  }

  static nullInflaterFactory(label: string): Inflater {
    return (d: any) => {
      const v = d[label];
      if ('' + v === 'null' || typeof v === 'undefined') {
        d[label] = null;
      }
    };
  }

  static numberRangeInflaterFactory(label: string, rangeSize: number): Inflater {
    return (d: any) => {
      const v = d[label];
      if ('' + v === 'null') {
        d[label] = null;
        return;
      }

      const start = Number(v);
      d[label] = new NumberRange(safeRange(start, rangeSize));
    };
  }

  static numberInflaterFactory(label: string): Inflater {
    return (d: any) => {
      let v = d[label];
      if ('' + v === 'null') {
        d[label] = null;
        return;
      }

      v = Number(v);
      d[label] = isNaN(v) ? null : v;
    };
  }

  static stringInflaterFactory(label: string): Inflater {
    return (d: any) => {
      const v = d[label];
      if (typeof v === 'undefined') {
        d[label] = null;
      }
    };
  }

  static timeInflaterFactory(label: string): Inflater {
    return (d: any) => {
      const v = d[label];
      if ('' + v === 'null' || typeof v === 'undefined') {
        d[label] = null;
        return;
      }

      d[label] = makeDate(v);
    };
  }

  static ipInflaterFactory(label: string): Inflater {
    return (d: any) => {
      const v = d[label];
      if ('' + v === 'null' || typeof v === 'undefined') {
        d[label] = null;
        return;
      }

      d[label] = Ip.fromString(v);
    };
  }

  static setStringInflaterFactory(label: string): Inflater {
    return (d: any) => {
      let v = d[label];
      if ('' + v === 'null') {
        d[label] = null;
        return;
      }

      if (typeof v === 'string') v = [v];
      d[label] = Set.fromJS({
        setType: 'STRING',
        elements: v,
      });
    };
  }

  static setCardinalityInflaterFactory(label: string): Inflater {
    return (d: any) => {
      const v = d[label];
      d[label] = Array.isArray(v) ? v.length : 1;
    };
  }

  static typeCheckDerivedAttributes(
    derivedAttributes: Record<string, Expression>,
    typeContext: DatasetFullType,
  ): Record<string, Expression> {
    let changed = false;
    const newDerivedAttributes: Record<string, Expression> = {};
    for (const k in derivedAttributes) {
      const ex = derivedAttributes[k];
      const newEx = ex.changeInTypeContext(typeContext);
      if (ex !== newEx) changed = true;
      newDerivedAttributes[k] = newEx;
    }
    return changed ? newDerivedAttributes : derivedAttributes;
  }

  static valuePostTransformFactory() {
    let valueSeen = false;
    return new Transform({
      objectMode: true,
      transform: (d: Datum, encoding, callback) => {
        valueSeen = true;
        callback(null, { type: 'value', value: d[External.VALUE_NAME] });
      },
      flush: callback => {
        callback(null, valueSeen ? null : { type: 'value', value: 0 });
      },
    });
  }

  static inflateArrays(d: Datum, attributes: Attributes): void {
    for (const attribute of attributes) {
      const attributeName = attribute.name;
      if (Array.isArray(d[attributeName])) {
        d[attributeName] = Set.fromJS(d[attributeName] as any);
      }
    }
  }

  static postTransformFactory(
    inflaters: Inflater[],
    attributes: Attributes,
    keys: string[],
    zeroTotalApplies: ApplyExpression[],
  ) {
    let valueSeen = false;
    return new Transform({
      objectMode: true,
      transform: function (d: Datum, encoding, callback) {
        if (!valueSeen) {
          this.push({
            type: 'init',
            attributes,
            keys,
          });
          valueSeen = true;
        }

        for (const inflater of inflaters) {
          inflater(d);
        }

        External.inflateArrays(d, attributes);

        callback(null, {
          type: 'datum',
          datum: d,
        });
      },
      flush: function (callback) {
        if (!valueSeen) {
          this.push({
            type: 'init',
            attributes,
            keys: null,
          });

          if (zeroTotalApplies) {
            this.push({
              type: 'datum',
              datum: External.makeZeroDatum(zeroTotalApplies),
            });
          }
        }
        callback();
      },
    });
  }

  static performQueryAndPostTransform(
    queryAndPostTransform: QueryAndPostTransform<any>,
    requester: PlywoodRequester<any>,
    engine: string,
    rawQueries: any[] | null,
  ): ReadableStream {
    if (!requester) {
      return new ReadableError('must have a requester to make queries');
    }

    let { query, context, postTransform, next } = queryAndPostTransform;
    if (!query || !postTransform) {
      return new ReadableError('no query or postTransform');
    }

    if (next) {
      let streamNumber = 0;
      let meta: any = null;
      let numResults: number;
      const resultStream = new StreamConcat({
        objectMode: true,
        next: () => {
          if (streamNumber) query = next(query, numResults, meta);
          if (!query) return null;
          streamNumber++;
          if (rawQueries) rawQueries.push({ engine, query });
          const stream = requester({ query, context });
          meta = null;
          stream.on('meta', (m: any) => (meta = m));
          numResults = 0;
          stream.on('data', () => numResults++);
          return stream;
        },
      });

      return pipeWithError(resultStream, postTransform);
    } else {
      if (rawQueries) rawQueries.push({ engine, query });
      return pipeWithError(requester({ query, context }), postTransform);
    }
  }

  static buildValueFromStream(stream: ReadableStream): Promise<PlywoodValue> {
    return new Promise((resolve, reject) => {
      const pvb = new PlywoodValueBuilder();
      const target = new Writable({
        objectMode: true,
        write: function (chunk, encoding, callback) {
          pvb.processBit(chunk);
          callback(null);
        },
      }).on('finish', () => {
        resolve(pvb.getValue());
      });

      stream.pipe(target);
      stream.on('error', (e: Error) => {
        stream.unpipe(target);
        reject(e);
      });
    });
  }

  static valuePromiseToStream(valuePromise: Promise<PlywoodValue>): ReadableStream {
    const pt = new PassThrough({ objectMode: true });

    valuePromise
      .then(v => {
        const i = iteratorFactory(v as Dataset);
        let bit: PlyBit;
        while ((bit = i())) {
          pt.write(bit);
        }
        pt.end();
      })
      .catch(e => {
        pt.emit('error', e);
      });

    return pt as any;
  }

  /**
   * Project away a set of column names from a Dataset. Used by cross-source
   * decomposition to strip synthetic join-key columns that served only as
   * the in-memory join anchor. Keys and attributes are recomputed from the
   * remaining columns.
   */
  /**
   * Walk a filter expression and replace every atomic predicate whose free
   * references are not all present in `schemaNames` with TRUE, then simplify.
   * Atomic predicates are recognized structurally: any RefExpression appears
   * inside a containing predicate (overlap/is/greaterThan/etc.), and the
   * chainable's whole predicate is a single AND-leaf.
   *
   * Used when propagating a filter from one External to a peer that shares
   * some but not all columns (e.g. main → linked source). Clauses that
   * don't resolve in the target schema get dropped silently rather than
   * blowing up reference-check with "could not resolve $<column>".
   *
   * The traversal stops at AND/OR/NOT boundaries: those are combinators,
   * not predicates. We recurse into their operands and re-combine the
   * pruned halves. Everything else is treated as an atomic predicate.
   */
  static pruneFilterToSchema(filter: Expression, schemaNames: Record<string, true>): Expression {
    if (!filter) return filter;
    if (filter.equals(Expression.TRUE) || filter.equals(Expression.FALSE)) return filter;

    const op = (filter as any).op;

    // Combinators: recurse into both halves. Only rebuild the combinator
    // when one side actually changed — otherwise return the original
    // filter so callers can detect "no-op prune" via reference equality
    // and skip rewriting. Always-rebuilding triggers downstream refcheck
    // state loss (the substitute in pruneLinkedFilterRefsInTree re-walks
    // the new tree without the original's resolved type metadata).
    if (op === 'and' || op === 'or') {
      const origLeft: Expression = (filter as any).operand;
      const origRight: Expression = (filter as any).expression;
      const left = External.pruneFilterToSchema(origLeft, schemaNames);
      const right = External.pruneFilterToSchema(origRight, schemaNames);
      if (left === origLeft && right === origRight) return filter;
      if (op === 'and') return left.and(right).simplify();
      return left.or(right).simplify();
    }

    if (op === 'not') {
      // A negation whose inside references out-of-schema columns is
      // unevaluable here AS A WHOLE — it must become identity TRUE,
      // never `NOT(TRUE) = FALSE`. Observed live (GrupoIfa dev): the
      // cube-level filter `NOT($url.in([...])) AND 0 < $price` pruned
      // for a lookup linkedSource emitted `WHERE FALSE`; the lookup
      // returned zero rows, the inner join dropped every main row and
      // the magic-dim split rendered as a single naked total.
      // Algebra: NOT(a AND b) = NOT(a) OR NOT(b); with `a` unevaluable
      // its negation is identity TRUE, so the whole clause is TRUE.
      const origInner: Expression = (filter as any).operand;
      const inner = External.pruneFilterToSchema(origInner, schemaNames);
      if (inner === origInner) return filter;
      return Expression.TRUE;
    }

    // Atomic predicate (overlap, is, greaterThan, contains, etc.): the
    // semantic column the predicate targets is a RefExpression in either
    // position — `operand` (the common `$col.is(...)` shape) or
    // `expression` (the literal-first shape `r(0).lessThan($price)` that
    // Turnilo emits for `0 < price`). If a CURRENT-SCOPE ref (nest 0)
    // names a column not in our schema, the predicate can't evaluate
    // here and becomes TRUE (identity) for this side.
    //
    // Critically, we do NOT inspect `getFreeReferences()` and we skip
    // refs with `nest > 0`: a well-formed predicate like `$color.is($^col)`
    // escalates `$^col` to the parent dataset — that ref resolves
    // outside the current external's schema by design. Dropping the
    // whole predicate because it isn't a column here would break a
    // pattern that Plywood users rely on.
    const anyOp: any = filter;
    const operand: Expression | undefined = anyOp.operand;
    if (operand instanceof RefExpression && !schemaNames[operand.name]) {
      return Expression.TRUE;
    }
    // Literal-first shape (`r(0).lessThan($price)`, Turnilo's `0 < price`):
    // the semantic column sits in `expression` position. Only when the
    // operand is a LITERAL do we inspect that side — if the operand is a
    // ref we already handled it above, and `$col.is($var)` (operand in
    // schema, expression resolving in an outer scope) must stay intact.
    if (
      operand instanceof LiteralExpression &&
      anyOp.expression instanceof RefExpression &&
      anyOp.expression.nest === 0 &&
      !schemaNames[anyOp.expression.name]
    ) {
      return Expression.TRUE;
    }
    return filter;
  }

  /**
   * Split a filter by which side of a cross-source decomposition can evaluate
   * each clause. Returns `{ main, post }` where `main` is the subset pushable
   * to the main sub-External's HAVING (Druid-side), and `post` is the subset
   * that must be applied after the in-memory join (because it references
   * apply names or split aliases routed to a linked sub-External).
   *
   * Algebra:
   *   - AND combinator: recurse; each side's main/post halves combine
   *     independently. A mixed-scope AND `(mainRef > 0) AND (linkedRef > 0)`
   *     becomes `{ main: mainRef > 0, post: linkedRef > 0 }`.
   *   - OR / NOT combinator: can't split — if the expression references any
   *     post-join name anywhere, the whole thing must move post-join. Pushing
   *     half of an OR down would over-prune the main side and silently drop
   *     rows the join should have re-admitted via the post-join branch.
   *   - Atomic predicate: the left operand's ref decides. If it names a
   *     main-resolvable apply/split → stays on main. Otherwise → post-join.
   *
   * This is the per-clause decomposition Ogievetsky flagged as the correct
   * move in 0f9cbf3 ("the correct thing to do would be to decompose the
   * havingFilter into havingOnExternal1 AND havingOnExternal2"). His fix at
   * that commit dropped external2's havingFilter pragmatically; here we do
   * the full split.
   */
  static splitFilterByScope(
    filter: Expression,
    mainNames: Record<string, true>,
  ): { main: Expression; post: Expression } {
    if (!filter || filter.equals(Expression.TRUE)) {
      return { main: Expression.TRUE, post: Expression.TRUE };
    }
    if (filter.equals(Expression.FALSE)) {
      // FALSE on either side collapses the whole query. Keep it on main so
      // Druid returns an empty set cheaply; post-join filtering would still
      // produce the same result but after doing the join work.
      return { main: Expression.FALSE, post: Expression.TRUE };
    }

    const op = (filter as any).op;

    if (op === 'and') {
      const l = External.splitFilterByScope((filter as any).operand, mainNames);
      const r = External.splitFilterByScope((filter as any).expression, mainNames);
      return {
        main: l.main.and(r.main).simplify(),
        post: l.post.and(r.post).simplify(),
      };
    }

    if (op === 'or' || op === 'not') {
      const refs = filter.getFreeReferences();
      const allInMain = refs.every(r => mainNames[r]);
      if (allInMain) return { main: filter, post: Expression.TRUE };
      return { main: Expression.TRUE, post: filter };
    }

    const operand: Expression | undefined = (filter as any).operand;
    if (operand instanceof RefExpression && !mainNames[operand.name]) {
      return { main: Expression.TRUE, post: filter };
    }
    return { main: filter, post: Expression.TRUE };
  }

  /**
   * Classify user split aliases against a cross-source boundary. Every
   * alias is placed in exactly one bucket:
   *
   *   shared     → alias's free refs ⊆ joinKeys ∪ sharedDimensions.
   *                Both sides group by it; the natural-join aligns
   *                rows with equal values.
   *   mainOnly   → refs resolve only in main's schema. The alias stays
   *                on main; the join broadcasts its value across
   *                matching linked rows.
   *   linkedOnly → refs resolve only in the linked source's schema.
   *                The alias stays on linked; main rows fan out into
   *                multiple grid rows via the join.
   *
   * Refs that resolve on NEITHER side throw an error with an actionable
   * message — the alias was written against columns the decomposition
   * can't find.
   *
   * Refs that resolve on BOTH sides but were NOT declared as a shared
   * dimension (or joinKey) ALSO throw: silently treating them as shared
   * would change aggregation granularity based on a coincidence of
   * naming. The caller MUST declare the intent via `sharedDimensions`.
   *
   * ONE schema-overlap exception is NOT ambiguous and resolves to MAIN
   * instead of throwing: a split whose refs are exactly the main external's
   * `timeAttribute` (`mainTimeAttribute`) when the linked source is
   * `timeAlignment: 'eternal'`. An eternal linked source's `__time` column
   * is the materialisation-snapshot sentinel (e.g. 1970), NOT an event time;
   * a Time(Day) split is therefore semantically a MAIN-side time bucket and
   * the lookup's same-named column carries no comparable meaning. The
   * exclusion is deliberately narrow — it fires ONLY for the main
   * timeAttribute against an eternal linked source. A genuine business
   * column that overlaps both schemas (or the timeAttribute against a
   * NON-eternal linked source, where `__time` IS event time on both sides)
   * still throws.
   *
   * Constant-valued splits (zero free refs) are treated as shared — a
   * literal expression evaluates identically on both sides.
   */
  static classifySplitAliases(
    split: SplitExpression,
    mainSchema: Record<string, true>,
    linkedSchema: Record<string, true>,
    config: LinkedSourceConfig,
    linkedSourceName: string,
    mainTimeAttribute?: string,
  ): { shared: string[]; mainOnly: string[]; linkedOnly: string[]; foreignLinked: string[] } {
    const shared: string[] = [];
    const mainOnly: string[] = [];
    const linkedOnly: string[] = [];
    // `foreignLinked` — aliases whose refs resolve in neither main nor
    // THIS linkedSource. They belong to a SIBLING linkedSource and will
    // be classified in that sibling's own iteration. Reporting them
    // separately (instead of throwing) is what makes cross-source
    // queries over multiple linkedSources work: e.g. a query splitting
    // on [brand_tier, competitor_size] where brand_tier lives in
    // linkedSource A and competitor_size in B must not explode when
    // A's classifier encounters competitor_size.
    //
    // The OUTER caller (getCrossExternalDecomposition) is responsible
    // for asserting every alias got classified somewhere — a
    // wholly-dangling alias (not in main, not in any linkedSource) is
    // still an error, raised at the loop's tail.
    const foreignLinked: string[] = [];

    const declaredShared: Record<string, true> = {};
    for (const k of config.joinKeys || []) declaredShared[k] = true;
    for (const k of config.sharedDimensions || []) declaredShared[k] = true;

    for (const alias of split.keys) {
      const ex = split.splits[alias];
      const refs = ex.getFreeReferences();

      if (refs.length === 0) {
        shared.push(alias);
        continue;
      }

      const isDeclaredShared = refs.every(r => declaredShared[r]);
      if (isDeclaredShared) {
        shared.push(alias);
        continue;
      }

      const inMain = refs.every(r => mainSchema[r]);
      const inLinked = refs.every(r => linkedSchema[r]);

      // Narrow non-ambiguity exception: the main timeAttribute split against
      // an `eternal` linked source resolves to MAIN, not "ambiguous". The
      // lookup's `__time` is a snapshot sentinel, never an event time, so a
      // Time(Day) bucket cannot mean the same thing on both sides. Scoped to
      // exactly `[mainTimeAttribute]` + `timeAlignment === 'eternal'` so it
      // never weakens the guard for genuine business-column overlaps or for
      // the timeAttribute against a bucketed (event-time) linked source.
      if (
        inMain &&
        inLinked &&
        mainTimeAttribute &&
        refs.length === 1 &&
        refs[0] === mainTimeAttribute &&
        config.timeAlignment === 'eternal'
      ) {
        mainOnly.push(alias);
        continue;
      }

      if (inMain && inLinked) {
        throw new Error(
          `Split alias "${alias}" refs [${refs.join(
            ', ',
          )}] resolve on both main and linked source "${linkedSourceName}" — declare them in \`sharedDimensions\` to use them as a shared split, or qualify the reference to a side-specific column. The engine refuses to infer shared-ness from schema overlap because same-named columns on different sides can legitimately carry different semantics.`,
        );
      }
      if (inMain) {
        mainOnly.push(alias);
      } else if (inLinked) {
        linkedOnly.push(alias);
      } else {
        // Belongs to a sibling linkedSource (or is truly dangling).
        // Defer the decision to the outer loop, which sees every
        // linkedSource's classification and can tell the two apart.
        foreignLinked.push(alias);
      }
    }

    return { shared, mainOnly, linkedOnly, foreignLinked };
  }

  /**
   * Resolve the subset of joinKeys the engine may auto-inject as
   * `__join_<key>` synthetic splits. Reads from `config.autoInjectJoinKeys`
   * when declared; falls back to `config.joinKeys` (default: every join
   * key is auto-injectable).
   *
   * Cubes with a TIME-typed joinKey that don't want per-snapshot row
   * explosion declare a subset that excludes the time key.
   */
  static resolveAutoInjectJoinKeys(config: LinkedSourceConfig): string[] {
    if (config.autoInjectJoinKeys) return config.autoInjectJoinKeys;
    return config.joinKeys || [];
  }

  /**
   * Resolve the join mode for a linked source. The cube declares it
   * explicitly via `config.joinMode`; if omitted, the caller gets
   * undefined and must decide whether to default or error.
   */
  static resolveLinkedJoinMode(config: LinkedSourceConfig): 'inner' | 'left' | undefined {
    return config.joinMode;
  }

  /**
   * Resolve the engine/version/requester binding for a linkedSource's
   * sub-query, honouring the cross-engine override (LinkedSourceConfig.engine).
   *
   *   - When `config.engine` is absent → inherit the main external's
   *     engine/version/requester verbatim (the canonical same-store case;
   *     a lookup_x_rev1 view living in the same Druid datasource).
   *   - When `config.engine` is present and DIFFERS from the main engine →
   *     the sub-query must be dispatched to a different store. The override
   *     requester is MANDATORY: silently inheriting the main requester would
   *     send the foreign SQL (e.g. a Postgres staging view query) to the
   *     Druid broker and either error opaquely or return wrong data. So we
   *     throw with an actionable message instead of falling back.
   *   - When `config.engine` is present but EQUALS the main engine → a
   *     declared-but-same override; inherit the main requester (no foreign
   *     dispatch risk) but honour any explicitly-supplied version/requester.
   *
   * `mainEngine` / `mainVersion` / `mainRequester` come from the main
   * external. `lsName` is only used to make the error message locatable.
   */
  static resolveLinkedEngineBinding(
    config: LinkedSourceConfig,
    lsName: string,
    mainEngine: string,
    mainVersion: string,
    mainRequester: PlywoodRequester<any>,
  ): { engine: string; version: string; requester: PlywoodRequester<any> } {
    const engine = config.engine ?? mainEngine;
    const version = config.version ?? mainVersion;
    if (config.engine && config.engine !== mainEngine) {
      if (!config.requester) {
        throw new Error(
          `External: linkedSource "${lsName}" declares cross-engine override ` +
            `engine="${config.engine}" (main external engine="${mainEngine}") but supplies no ` +
            `requester. A foreign-engine sub-query cannot be driven by the main external's ` +
            `requester — that would dispatch the SQL to the wrong store. Inject ` +
            `\`linkedSources["${lsName}"].requester\` bound to the "${config.engine}" engine.`,
        );
      }
      return { engine, version, requester: config.requester };
    }
    return { engine, version, requester: config.requester ?? mainRequester };
  }

  static dropColumns(dataset: Dataset, drop: string[]): Dataset {
    if (!drop || drop.length === 0) return dataset;
    const dropSet: Record<string, true> = {};
    for (const n of drop) dropSet[n] = true;
    const nextData = dataset.data.map(d => {
      const out: Datum = {};
      for (const k in d) if (!dropSet[k]) out[k] = d[k];
      return out;
    });
    const nextAttributes = (dataset.attributes || []).filter(a => !dropSet[a.name]);
    const nextKeys = (dataset.keys || []).filter(k => !dropSet[k]);
    return new Dataset({
      attributes: nextAttributes,
      keys: nextKeys.length ? nextKeys : undefined,
      data: nextData,
    });
  }

  /**
   * Post-join re-aggregation to the user's split grain (INV-1 producer).
   *
   * The JS-join cross-source path pre-aggregates main at the JOIN-KEY grain
   * (e.g. one row per `brand`) and the linked side maps join-key → user-split
   * value (e.g. `brand` → `brand_country`). The join therefore fans main's
   * aggregate rows out: every (brand, country) pair carries the brand-level
   * count, so a country with N brands appears in N rows. The user asked for
   * the COUNTRY grain — those N rows must collapse to one, with each measure
   * recombined according to its decomposability trait.
   *
   * This is what makes the JS-join lossless for `sum`-class measures and is
   * MANDATORY for the cross-engine case (a Postgres staging view × main Druid
   * cannot use the in-engine native JOIN, so JS-join is the only path).
   *
   * Combination is by DecomposeTrait, declared per aggregator in
   * `mixins/aggregate.ts`:
   *   - 'sum' → numeric sum across the fanned rows (count, sum, and
   *             avg-rewritten sum/count all reduce this way)
   *   - 'min' → minimum
   *   - 'max' → maximum
   *   - 'none' → THROW. The decomposability gate is supposed to divert
   *             non-recombinable measures to the native-JOIN path; a 'none'
   *             trait reaching here is a gate bug, surfaced loud rather than
   *             silently returning a wrong number.
   *
   * `reAggKeys` is the user's split grain (split keys minus the synthetic
   * join aliases). `applyTraits` maps each value-apply column to its trait.
   * Group keys not present in a row are treated as the row's value verbatim;
   * non-numeric apply values for sum/min/max throw (shape contract).
   *
   * No-op (returns the dataset unchanged) when there is no fan-out — the
   * common case where the split grain already equals the join-key grain.
   */
  static reAggregateToSplitGrain(
    dataset: Dataset,
    reAggKeys: string[],
    applyTraits: Record<string, 'sum' | 'min' | 'max' | 'none'>,
  ): Dataset {
    if (!dataset || !reAggKeys || reAggKeys.length === 0) return dataset;
    const data = dataset.data;
    if (!data || data.length === 0) return dataset;

    // Fast path: if every reAggKey tuple is already distinct there is no
    // fan-out to collapse — return as-is to preserve the original instance
    // (and avoid touching non-numeric apply columns needlessly).
    const seenTuples: Record<string, true> = {};
    let hasFanOut = false;
    for (const row of data) {
      const tupleKey = reAggKeys.map(k => JSON.stringify((row as any)[k])).join('|');
      if (seenTuples[tupleKey]) {
        hasFanOut = true;
        break;
      }
      seenTuples[tupleKey] = true;
    }
    if (!hasFanOut) return dataset;

    const applyNames = Object.keys(applyTraits);
    // If ANY measure is non-recombinable ('none' — notably `average`, whose
    // single-column ratio projection cannot be summed across partitions),
    // re-aggregation cannot produce a correct result for this dataset. Do NOT
    // partially collapse (that would silently mix correct sum columns with a
    // wrong avg column). Leave the dataset untouched so the downstream INV-1
    // net (`assertDatasetShape`) fails loud on the fan-out — the documented,
    // pre-existing limitation for avg + linked-only split (the correct fix is
    // to route those measures to native-JOIN, tracked separately). Sum/min/max
    // -only queries fall through and collapse normally.
    const hasNonRecombinable = applyNames.some(n => applyTraits[n] === 'none');
    if (hasNonRecombinable) return dataset;

    const groups = new Map<string, Datum>();
    const order: string[] = [];
    for (const row of data) {
      const tupleKey = reAggKeys.map(k => JSON.stringify((row as any)[k])).join('|');
      let acc = groups.get(tupleKey);
      if (!acc) {
        // Seed the accumulator with the group-key columns plus any
        // non-apply columns from the first row (carried verbatim).
        acc = {};
        for (const k in row) {
          if (!hasOwnProp(row, k)) continue;
          acc[k] = (row as any)[k];
        }
        groups.set(tupleKey, acc);
        order.push(tupleKey);
        continue;
      }
      for (const name of applyNames) {
        const trait = applyTraits[name];
        const incoming = (row as any)[name];
        const current = (acc as any)[name];
        if (incoming == null) continue; // left-join orphan contributes nothing
        if (current == null) {
          (acc as any)[name] = incoming;
          continue;
        }
        if (trait === 'sum') {
          if (typeof current !== 'number' || typeof incoming !== 'number') {
            throw new Error(
              `External.reAggregateToSplitGrain: apply "${name}" has trait 'sum' but a ` +
                `non-numeric value (${JSON.stringify(current)} / ${JSON.stringify(incoming)}); ` +
                `sum re-aggregation requires numeric measure columns.`,
            );
          }
          (acc as any)[name] = current + incoming;
        } else if (trait === 'min') {
          (acc as any)[name] = incoming < current ? incoming : current;
        } else if (trait === 'max') {
          (acc as any)[name] = incoming > current ? incoming : current;
        } else {
          // trait === 'none' — the gate should have diverted this measure
          // to native-JOIN. Reaching here means the gate let a
          // non-recombinable measure into the JS-join path: fail loud.
          throw new Error(
            `External.reAggregateToSplitGrain: apply "${name}" has non-recombinable trait ` +
              `'none' but reached post-join re-aggregation. The decomposability gate must ` +
              `divert 'none'-trait measures to the native-JOIN path; this is a gate bug. ` +
              `Re-aggregating it would silently produce a wrong value.`,
          );
        }
      }
    }

    const collapsed: Datum[] = order.map(k => groups.get(k)!);
    return new Dataset({
      attributes: dataset.attributes,
      keys: reAggKeys,
      data: collapsed,
    });
  }

  /**
   * Replay the scalar recombination half of the segregate-then-recombine
   * decomposition. After `reAggregateToSplitGrain` has collapsed the
   * homomorphic leaf aggregates (`!T_0 = sum($x)`, `!T_1 = count()`, …) to the
   * user's split grain, this reconstructs each derived measure by evaluating
   * its post-aggregate expression (`avg_price = $!T_0 / $!T_1`) per row, then
   * drops the synthetic leaf columns (`!T_*`) so the caller sees only the
   * measures they requested.
   *
   * Orthogonal to re-aggregation: the leaves are the only thing the join and
   * re-agg touch; the recombination is pure per-row scalar arithmetic with no
   * cross-row dependency, so it is correct to apply AFTER the grain has
   * collapsed. No-op when `postAggregateApplies` is empty (every measure was a
   * single aggregate kept under its own name).
   *
   * Synthetic-leaf naming: `segregationAggregateApplies` mints leaf names
   * prefixed `!T_`. Those — and only those — are dropped; a single-aggregate
   * measure that segregation kept under its own user-facing name (e.g.
   * `price_min`) is never a `!T_` column and survives.
   */
  static applyPostAggregateRecombination(
    dataset: Dataset,
    postAggregateApplies: ApplyExpression[] | undefined,
  ): Dataset {
    if (!dataset) return dataset;
    if (!postAggregateApplies || postAggregateApplies.length === 0) return dataset;
    let out = dataset;
    for (const apply of postAggregateApplies) {
      out = out.apply(apply.name, apply.expression);
    }
    // Drop synthetic leaf columns (`!T_*`). Read the actual column set from the
    // first datum rather than trusting `attributes`, since `Dataset.apply` adds
    // derived columns to the data without necessarily refreshing attributes.
    const sample = out.data && out.data[0] ? out.data[0] : {};
    const syntheticLeaves: string[] = [];
    for (const col in sample) {
      if (!hasOwnProp(sample, col)) continue;
      if (col.indexOf('!T_') === 0) syntheticLeaves.push(col);
    }
    if (syntheticLeaves.length === 0) return out;
    return External.dropColumns(out, syntheticLeaves);
  }

  /**
   * Resolve the single re-aggregation trait for a value-apply: the reducer
   * the post-join step uses to collapse fan-out rows to the user's split
   * grain.
   *
   * A column is post-join re-aggregatable by a SINGLE reducer only when the
   * apply's value expression is ONE aggregate (after the avg→sum/count
   * rewrite), and that aggregate's static `decomposable` trait
   * (mixins/aggregate.ts, INV-3) is 'sum' | 'min' | 'max':
   *
   *   - count / sum  → 'sum'  (associative + commutative across partitions)
   *   - min / max    → 'min' / 'max'
   *
   * Everything else resolves to 'none' — NOT post-join-reducible:
   *
   *   - a DERIVED root (e.g. `average` rewrites to `divide(sum, count)`):
   *     the projected column is a RATIO already evaluated per main-row.
   *     Summing ratios across partitions is mathematically wrong (the sum
   *     of per-brand averages is not the tier-level average). The correct
   *     path carries the underlying sum/count separately and divides AFTER
   *     re-agg — which this single-column projection does not do — so it
   *     must route to native-JOIN (one SQL computes the avg at the user's
   *     grain) instead.
   *   - 'none'-trait aggregates (countDistinct, quantile, mode): not
   *     losslessly recombinable from pre-aggregated partitions.
   *
   * A 'none' result tells the gate to divert the measure to native-JOIN;
   * `reAggregateToSplitGrain` throws loud if a 'none' still reaches it. A
   * missing trait throws `PlywoodTraitMissing` via the same path as
   * `isMeasureDecomposable` — never silently defaulted.
   */
  static resolveApplyDecomposeTrait(apply: ApplyExpression): 'sum' | 'min' | 'max' | 'none' {
    if (!apply || !apply.expression) return 'sum'; // no value contribution
    // Rewrite avg→sum/count first so the root-op check sees the canonical
    // form (avg's root becomes `divide`, correctly classified 'none').
    const root = apply.expression.decomposeAverage();
    if (!root.isAggregate()) {
      // Derived combination of aggregates (divide/add/subtract/…) — not a
      // single post-join reducer. Forces native-JOIN via the gate.
      return 'none';
    }
    const ctor = Expression.classMap[root.op] as any;
    const trait = ctor && ctor.decomposable;
    if (trait === undefined) {
      // Reuse isMeasureDecomposable's fail-loud path so the error message +
      // exception type stay identical (INV-3, PlywoodTraitMissing).
      Expression.isMeasureDecomposable(apply);
    }
    if (trait === 'sum' || trait === 'min' || trait === 'max') return trait;
    return 'none';
  }

  /**
   * Transparent net (INV-1). Walks `dataset.data` building a multiset
   * keyed by the tuple of `dataset.keys`. If any tuple appears more
   * than once, throws `PlywoodCardinalityViolation` synchronously
   * with the duplicate tuple plus the row-count delta in the message.
   *
   * Datasets with no keys (totals shapes — `mode === 'total'` on the
   * upstream External) carry a single row of aggregates by construction;
   * the assertion is a no-op in that case (INV-1 only constrains
   * datasets that declare keys).
   *
   * Never logs. Never wraps. Failure here means the engine produced a
   * shape the caller can't reason about: the cross-source decomposition
   * fanned main rows past the user's split grain without re-aggregating.
   */
  static assertDatasetShape(dataset: Dataset): void {
    if (!dataset) return;
    const keys = dataset.keys || [];
    if (keys.length === 0) return; // totals or shapeless — INV-1 doesn't apply
    const data = dataset.data || [];
    const seen = new Map<string, Datum>();
    for (const row of data) {
      // JSON.stringify on the key projection is enough: every plywood
      // primitive (string, number, boolean, Date, null, undefined)
      // round-trips through it deterministically. Two rows with the
      // same key tuple stringify identically.
      const tupleParts: any[] = [];
      for (const k of keys) tupleParts.push(row[k]);
      const tupleKey = JSON.stringify(tupleParts);
      if (seen.has(tupleKey)) {
        const distinctTuples: Record<string, true> = {};
        for (const r of data) {
          const tp: any[] = [];
          for (const k of keys) tp.push(r[k]);
          distinctTuples[JSON.stringify(tp)] = true;
        }
        const distinctCount = Object.keys(distinctTuples).length;
        const delta = data.length - distinctCount;
        const tupleDisplay = keys.map((k, i) => `${k}=${JSON.stringify(tupleParts[i])}`).join(', ');
        throw new PlywoodCardinalityViolation(
          `External.assertDatasetShape: duplicate key tuple [${tupleDisplay}] in dataset ` +
            `with keys [${keys.join(', ')}]; row count ${data.length} exceeds distinct ` +
            `tuple count ${distinctCount} (delta ${delta}). The upstream cross-source ` +
            `decomposition fanned main rows past the user's split grain without re-aggregating.`,
        );
      }
      seen.set(tupleKey, row);
    }
  }

  static jsToValue(parameters: ExternalJS, requester: PlywoodRequester<any>): ExternalValue {
    const value: ExternalValue = {
      engine: parameters.engine,
      version: parameters.version,
      source: parameters.source,
      suppress: true,
      rollup: parameters.rollup,
      concealBuckets: Boolean(parameters.concealBuckets),
      requester,
    };
    if (parameters.attributes) {
      value.attributes = AttributeInfo.fromJSs(parameters.attributes);
    }
    if (parameters.attributeOverrides) {
      value.attributeOverrides = AttributeInfo.fromJSs(parameters.attributeOverrides);
    }
    if (parameters.derivedAttributes) {
      value.derivedAttributes = Expression.expressionLookupFromJS(parameters.derivedAttributes);
    }
    if (parameters.linkedSources) {
      value.linkedSources = {};
      for (const name in parameters.linkedSources) {
        const ls = parameters.linkedSources[name] as any;
        value.linkedSources[name] = {
          source: ls.source,
          joinKeys: ls.joinKeys,
          autoInjectJoinKeys: ls.autoInjectJoinKeys,
          sharedDimensions: ls.sharedDimensions,
          joinMode: ls.joinMode,
          attributes: ls.attributes ? AttributeInfo.fromJSs(ls.attributes) : undefined,
          derivedAttributes: ls.derivedAttributes
            ? Expression.expressionLookupFromJS(ls.derivedAttributes)
            : undefined,
          timeAlignment: ls.timeAlignment,
          // Cross-engine override fields round-trip through JS; `requester`
          // does NOT (it is a live host handle injected at runtime — see
          // LinkedSourceConfig.engine). Carrying engine/version here lets a
          // deserialised External remember it must route this linkedSource
          // elsewhere, and `resolveLinkedEngineBinding` fails loud at query
          // time if the matching requester was never injected.
          engine: ls.engine,
          version: ls.version,
        };
      }
    }

    value.filter = parameters.filter ? Expression.fromJS(parameters.filter) : Expression.TRUE;
    // The distinguished time column travels through this seam too.
    // Without this line, a Druid SQL / Postgres / MySQL external whose
    // cube declares `timeAttribute: 'time'` would silently lose the
    // value at fromJS time — only `DruidExternal.fromJS` (the legacy
    // native Druid path) was previously copying it across, which is
    // why time-bound cubes worked there but not on the SQL transports.
    if (typeof parameters.timeAttribute === 'string' && parameters.timeAttribute.length > 0) {
      value.timeAttribute = parameters.timeAttribute;
    }

    return value;
  }

  static classMap: Record<string, typeof External> = {};
  static register(ex: typeof External): void {
    const engine = (<any>ex).engine.replace(/^\w/, (s: string) => s.toLowerCase());
    External.classMap[engine] = ex;
  }

  static getConstructorFor(engine: string): typeof External {
    const ClassFn = External.classMap[engine];
    if (!ClassFn) throw new Error(`unsupported engine '${engine}'`);
    return ClassFn;
  }

  static uniteValueExternalsIntoTotal(
    keyExternals: { key: string; external?: External }[],
  ): External {
    if (keyExternals.length === 0) return null;
    const applies: ApplyExpression[] = [];

    let baseExternal: External = null;
    for (const keyExternal of keyExternals) {
      const key = keyExternal.key;
      const external = keyExternal.external;
      if (!baseExternal) baseExternal = external;
      applies.push(Expression._.apply(key, new ExternalExpression({ external })));
    }

    return keyExternals[0].external.getBase().makeTotal(applies);
  }

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any> = null): External {
    if (!hasOwnProp(parameters, 'engine')) {
      throw new Error('external `engine` must be defined');
    }
    const engine: string = parameters.engine;
    if (typeof engine !== 'string') throw new Error('engine must be a string');
    const ClassFn = External.getConstructorFor(engine);

    // Back compat
    if (!requester && hasOwnProp(parameters, 'requester')) {
      console.warn("'requester' parameter should be passed as context (2nd argument)");
      requester = (parameters as any).requester;
    }
    if (parameters.source == null) {
      parameters.source =
        (parameters as any).dataSource != null
          ? (parameters as any).dataSource
          : (parameters as any).table;
    }

    return ClassFn.fromJS(parameters, requester);
  }

  static fromValue(parameters: ExternalValue): External {
    const { engine } = parameters;
    const ClassFn = External.getConstructorFor(engine) as any;
    return new ClassFn(parameters);
  }

  /**
   * When a datum contains an External that declares linkedSources, synthesize
   * a sibling External for each linked source and inject it at the same scope.
   *
   * This mirrors the manual `ply().apply('main', main).apply('reviews', reviews)`
   * pattern callers used to write by hand: the auto-decomposition pipeline
   * downstream assumes every external referenced in the expression is
   * directly addressable in the enclosing context.
   *
   * Idempotent: if a linked-source name is already bound in the datum (caller
   * supplied it explicitly), we leave it untouched.
   */
  /**
   * Pre-refcheck rewrite: for every `.filter(F)` applied to a ref whose
   * name matches a declared linkedSource in the context, prune F to
   * that source's schema. Without this pass, refcheck walks F against
   * the linked source's type context and throws on the first ref that
   * doesn't resolve (e.g. a main-only column like `$reference`).
   *
   * Algebraic justification: `$linked.filter(F)` where F references a
   * column not in linked's schema is a type error — F can't be
   * evaluated on a row from the linked side. The clause must be
   * dropped (replaced with TRUE and simplified). The downstream join
   * on shared or synthetic joinKeys propagates the main-side narrowing
   * to the linked rows, so no information is lost.
   *
   * This runs BEFORE referenceCheck so malformed shapes the UI emits
   * (where the client lacks schema awareness for the linked sources)
   * get rewritten to their correct algebraic form before any type
   * checking.
   */
  static pruneLinkedFilterRefsInTree(expression: Expression, context: Datum): Expression {
    // Build a schema lookup for every External-named binding in the context —
    // both the primary (usually `main`) and each declared linkedSource. A
    // `.filter(F)` applied to any of those refs gets F pruned to that
    // side's own schema, because a clause over a column the side doesn't
    // have is identity there (the join on shared/synthetic joinKeys
    // propagates main's narrowing to the linked rows, and vice-versa).
    const schemas: Record<string, Record<string, true>> = {};
    for (const k in context) {
      const v = context[k];
      if (!(v instanceof External)) continue;

      // Primary binding (e.g. `main`) — schema is its own raw + derived attrs,
      // PLUS timeAttribute. Druid SQL introspect emits the time column only
      // as `timeAttribute` (never as a member of `attributes`); without
      // adding it here the `pruneFilterToSchema` call below rewrites
      // `$time.overlap(...)` to TRUE, the main sub-query goes to Druid with
      // no time bound and `allowEternity: false`, and Druid throws
      // "must filter on time unless the allowEternity flag is set".
      const mainSchema: Record<string, true> = {};
      for (const a of v.rawAttributes || []) mainSchema[a.name] = true;
      for (const dk in v.derivedAttributes || {}) mainSchema[dk] = true;
      const ta = (v as any).timeAttribute;
      if (typeof ta === 'string' && ta.length > 0) mainSchema[ta] = true;
      schemas[k] = mainSchema;

      // Linked sources — their declared attributes + derivedAttributes
      if (!v.linkedSources) continue;
      for (const lsName in v.linkedSources) {
        const ls = v.linkedSources[lsName];
        const schema: Record<string, true> = {};
        if (ls.attributes) for (const a of ls.attributes as any[]) schema[a.name] = true;
        if (ls.derivedAttributes) for (const kk in ls.derivedAttributes) schema[kk] = true;
        schemas[lsName] = schema;
      }
    }
    if (Object.keys(schemas).length === 0) return expression;

    // Per-linkedSource harvest of the linked-ONLY filter clauses.
    //
    // The Turnilo front stamps the full cube filter (including clauses over a
    // column that lives ONLY in a magic-dim lookup, e.g.
    // `$brand_country.overlap(['Francia'])`) onto BOTH the main `.filter()` and
    // every linked-source apply. `pruneFilterToSchema` below correctly keeps
    // such a clause only in the apply of the lookup that owns the column. But
    // that apply is a sibling DATASET apply no output measure references — so
    // the very next `.simplify()` in `_initialPrepare` deletes it as dead code,
    // taking the linked-only clause with it. Decomposition then seeds the
    // lookup sub-query's filter from `this.filter` (main's filter), which never
    // carried the linked-only column, and the lookup query emits no WHERE →
    // the inner join restricts nothing → the user sees EVERY value of the
    // linked dimension instead of the one they filtered to.
    //
    // To survive simplify, the harvested clause must travel WITH THIS QUERY.
    // It is an IMMUTABLE per-request derivation: we never write into the
    // linkedSource config we received. That config object is shared by
    // reference across every External `valueOf()`/`fromValue` derives downstream
    // (value.linkedSources = this.linkedSources) AND — in the server — it is the
    // long-lived settings-manager cube config reused by EVERY request. Mutating
    // its `.filter` (the v1 approach) leaked the clause cross-request: after one
    // Francia query, every later no-filter query on the same cube emitted the
    // lookup with `WHERE brand_country = 'Francia'`. Instead we derive a FRESH
    // main External with a FRESH linkedSources map whose owning-lookup config is
    // a shallow copy carrying the harvested `filter`, and bind THAT copy into
    // this request's datum slot (`context[k]`). The datum map is per-request, so
    // replacing its slot touches nothing shared; the derived External (and its
    // fresh config) rides the resolve→simplify→decomposition pipeline as `this`,
    // where getCrossExternalDecomposition reads the harvested clause off the
    // per-request config. See getCrossExternalDecomposition (templateFilter),
    // the simulateDruidLinkedDimFilter Francia wire fixture, and the
    // simulateDruidLinkedDimFilterContamination cross-request suite.
    for (const k in context) {
      const v = context[k];
      if (!(v instanceof External) || !v.linkedSources) continue;
      const mainSchema = schemas[k] || {};
      const timeAttrName = (v as any).timeAttribute;
      // Harvested linked-only clauses for this main, keyed by linkedSource name.
      // Accumulated on the stack — nothing shared is touched until we mint the
      // per-request copy below.
      const harvested: Record<string, Expression> = {};
      // Per-lookup flag: does the expression have a LIVE consumer of this
      // linkedSource — a value-returning apply that references the lookup (its
      // ref name or one of its linked-only columns) OR a split over a
      // linked-only column? When yes, the established decomposition path
      // (JS-join / cross-external) materialises a sibling sub-query that
      // already honours the harvested filter, so the totals/main-split
      // semijoin-to-root rescue must NOT fire (it would double-handle and, for
      // a multi-key lookup, throw on the single-joinKey assumption). When NO
      // live consumer exists, the clause is genuinely ORPHANED — `.simplify()`
      // deletes the dead sibling apply and the filter would be lost — so we
      // mark the lookup `semijoinToRoot: true` to authorise the rescue.
      const hasLiveConsumer: Record<string, boolean> = {};
      // Per-lookup flag: did the harvested residue reference a NON-linked-only
      // (i.e. main) column? Such a residue cannot be pushed onto a lookup-side
      // query, so it must not authorise the semijoin-to-root rescue.
      const harvestResidueDirty: Record<string, boolean> = {};
      for (const lsName in v.linkedSources) {
        const linkedSchema = schemas[lsName];
        if (!linkedSchema) continue;
        // Linked-ONLY names: columns the lookup has that main does NOT, minus
        // the time attribute. Pruning a filter to this schema isolates exactly
        // the clauses main cannot absorb (so they would otherwise be lost) and
        // that are not the shared time bound (which the eternal/bucketed
        // alignment logic at decomposition handles on its own — re-stashing it
        // here would, under `timeAlignment:eternal`, filter a sentinel-time
        // snapshot lookup down to zero rows).
        const linkedOnly: Record<string, true> = {};
        for (const n in linkedSchema) {
          if (mainSchema[n]) continue;
          if (typeof timeAttrName === 'string' && n === timeAttrName) continue;
          linkedOnly[n] = true;
        }
        if (Object.keys(linkedOnly).length === 0) continue;
        // Walk the tree for any `.filter(F)` whose operand refs this main or
        // this lookup; harvest the linked-only clauses of F into the local map.
        // In the SAME walk, detect a live consumer: a non-DATASET (value) apply
        // OR a split whose expression references the lookup name or a
        // linked-only column.
        expression.forEach(e => {
          if (e instanceof FilterExpression) {
            const op = e.operand;
            if (!(op instanceof RefExpression)) return;
            if (op.name !== k && op.name !== lsName) return;
            const residue = External.pruneFilterToSchema(e.expression, linkedOnly);
            if (residue.equals(Expression.TRUE)) return;
            // The semijoin-to-root puts this residue on a LOOKUP-side query
            // (SELECT DISTINCT joinKey WHERE <residue>), so it is only valid
            // when EVERY free reference resolves on the lookup schema. A bare
            // boolean main ref like `$promo` survives pruneFilterToSchema intact
            // (the prune classifies only atomic comparison predicates) but
            // references a MAIN column — pushing it onto the lookup query throws
            // "could not resolve $promo". Such a residue must NOT authorise the
            // rescue. (The pre-existing native-JOIN / JS-join consumers do their
            // own schema handling, so the harvested `config.filter` is unchanged
            // for them; only the semijoin authorisation is gated here.)
            const dirty = residue.getFreeReferences().some(rn => !linkedOnly[rn]);
            if (dirty) harvestResidueDirty[lsName] = true;
            const prev = harvested[lsName];
            harvested[lsName] =
              prev && !prev.equals(Expression.TRUE) ? prev.and(residue).simplify() : residue;
            return;
          }
          if (e instanceof ApplyExpression) {
            if (e.expression.type === 'DATASET') return; // scope registration, not a value
            const refs = e.expression.getFreeReferences();
            if (refs.some(rn => rn === lsName || linkedOnly[rn])) hasLiveConsumer[lsName] = true;
            return;
          }
          if (e instanceof SplitExpression) {
            e.mapSplits((_n, sx) => {
              const refs = sx.getFreeReferences();
              if (refs.some(rn => rn === lsName || linkedOnly[rn])) hasLiveConsumer[lsName] = true;
            });
            return;
          }
        });
      }
      // Nothing harvested → leave the shared External untouched (the common
      // case: no linked-only filter, the fix is fully inert).
      if (Object.keys(harvested).length === 0) continue;
      // Mint a per-request copy: fresh linkedSources MAP, fresh config OBJECT
      // for each owning lookup, with the harvested clause as `config.filter`.
      // Lookups with no harvested clause keep their original config by
      // reference (read-only — never mutated). Then rebind this request's datum
      // slot to the copy so resolve/decomposition see the harvested filter
      // without ever writing to the shared config.
      const freshLinked: Record<string, LinkedSourceConfig> = {};
      for (const lsName in v.linkedSources) {
        const orig = v.linkedSources[lsName];
        const clause = harvested[lsName];
        freshLinked[lsName] = clause
          ? ({
              ...(orig as any),
              filter: clause,
              // Authorise the totals/main-split semijoin-to-root ONLY when the
              // clause is orphaned (no live linked consumer) AND the harvested
              // residue is purely linked-only-resolvable (a dirty residue —
              // e.g. a bare main boolean ref — cannot ride a lookup-side
              // query). When a sibling sub-query consumes it, or the residue is
              // dirty, this stays false and the rescue stands down.
              semijoinToRoot: !hasLiveConsumer[lsName] && !harvestResidueDirty[lsName],
            } as LinkedSourceConfig)
          : orig;
      }
      const copyValue = v.valueOf();
      copyValue.linkedSources = freshLinked;
      context[k] = External.fromValue(copyValue);
    }

    return expression.substitute(e => {
      // Target: .filter(F) whose operand is a RefExpression to a known source
      if (!(e instanceof FilterExpression)) return null;
      const op = e.operand;
      if (!(op instanceof RefExpression)) return null;
      const schema = schemas[op.name];
      if (!schema) return null;
      const originalFilter = e.expression;
      const pruned = External.pruneFilterToSchema(originalFilter, schema);
      if (pruned === originalFilter) return null;
      return op.filter(pruned);
    });
  }

  static expandLinkedSourcesInDatum(datum: Datum): Datum {
    let expanded: Datum | null = null;
    for (const k in datum) {
      if (!hasOwnProp(datum, k)) continue;
      const value = datum[k];
      if (!(value instanceof External)) continue;
      if (!value.linkedSources || Object.keys(value.linkedSources).length === 0) continue;

      for (const lsName in value.linkedSources) {
        if (lsName === k) continue;
        if (hasOwnProp(datum, lsName)) continue;
        if (expanded && hasOwnProp(expanded, lsName)) continue;

        const ls = value.linkedSources[lsName];
        const normalizedAttrs: Attributes | undefined = ls.attributes
          ? (ls.attributes as any[]).map(a =>
              a instanceof AttributeInfo ? a : AttributeInfo.fromJS(a),
            )
          : undefined;
        let normalizedDerived: Record<string, Expression> | undefined;
        if (ls.derivedAttributes) {
          normalizedDerived = {};
          for (const k in ls.derivedAttributes) {
            const v = (ls.derivedAttributes as any)[k];
            normalizedDerived[k] = v instanceof Expression ? v : Expression.fromJSLoose(v);
          }
        }
        // Prune main.filter to only the clauses whose free references exist
        // in the linked schema. A filter like `$__time.overlap(...) AND
        // $reference.is(...)` on main is meaningful for main only — the
        // linked side has no `reference` column. Applying it unfiltered to
        // the synthesized peer would fail reference-check and blow up the
        // whole query even though the user's intent was only to narrow the
        // main-side rows.
        const linkedNames: Record<string, true> = {};
        if (normalizedAttrs) {
          for (const a of normalizedAttrs) linkedNames[a.name] = true;
        }
        if (normalizedDerived) {
          for (const k in normalizedDerived) linkedNames[k] = true;
        }
        const prunedFilter = External.pruneFilterToSchema(value.filter, linkedNames);

        // Honour a per-linkedSource cross-engine override. Absent → inherit
        // the main external's engine/version/requester (canonical case).
        const binding = External.resolveLinkedEngineBinding(
          ls,
          lsName,
          value.engine,
          value.version,
          value.requester,
        );

        const linkedValue: ExternalValue = {
          engine: binding.engine,
          version: binding.version,
          source: ls.source,
          suppress: true,
          rollup: value.rollup,
          concealBuckets: value.concealBuckets,
          requester: binding.requester,
          attributes: normalizedAttrs,
          derivedAttributes: normalizedDerived,
          filter: prunedFilter,
          timeAttribute: (value as any).timeAttribute,
          customAggregations: (value as any).customAggregations,
          customTransforms: (value as any).customTransforms,
          allowEternity: (value as any).allowEternity,
          allowSelectQueries: (value as any).allowSelectQueries,
          exactResultsOnly: (value as any).exactResultsOnly,
          querySelection: (value as any).querySelection,
          context: (value as any).context,
        };
        const linkedExt = External.fromValue(linkedValue);

        if (!expanded) expanded = { ...datum };
        expanded[lsName] = linkedExt;
      }
    }
    return expanded || datum;
  }

  public engine: string;
  public version: string;
  public source: string | string[];
  public suppress: boolean;
  public rollup: boolean;
  public attributes: Attributes = null;
  public attributeOverrides: Attributes = null;
  public derivedAttributes: Record<string, Expression>;
  public linkedSources: Record<string, LinkedSourceConfig>;
  // The distinguished time column for this external. Druid native
  // (`DruidExternal`) re-declared this for legacy reasons; lifting it
  // here makes every transport (Druid SQL, Postgres, MySQL, …) honour
  // the timeAttribute uniformly. Without it, a cube that declares
  // `timeAttribute: 'time'` to a Druid-SQL external simply forgets the
  // value, and any `$time`-referencing expression fails
  // `expressionDefined` because `getRawFullType` has no entry for the
  // time column. The downstream symptom is "must filter on time unless
  // the allowEternity flag is set" raised by Druid when the absorbed
  // filter never made it into the SQL.
  public timeAttribute: string | undefined;

  public delegates: External[];
  public concealBuckets: boolean;

  public rawAttributes: Attributes;
  public requester: PlywoodRequester<any>;
  public mode: QueryMode;
  public filter: Expression;
  public valueExpression: Expression;
  public select: SelectExpression;
  public split: SplitExpression;
  public dataName: string;
  public applies: ApplyExpression[];
  public sort: SortExpression;
  public limit: LimitExpression;
  public havingFilter: Expression;
  public specialApplyTransform: SpecialApplyTransform;

  constructor(parameters: ExternalValue, dummy: any = null) {
    if (dummy !== dummyObject) {
      throw new TypeError('can not call `new External` directly use External.fromJS instead');
    }
    this.engine = parameters.engine;

    let version: string = null;
    if (parameters.version) {
      version = External.extractVersion(parameters.version);
      if (!version) throw new Error(`invalid version ${parameters.version}`);
    }
    this.version = version;
    this.source = parameters.source;

    this.suppress = Boolean(parameters.suppress);
    this.rollup = Boolean(parameters.rollup);
    if (parameters.attributes) {
      this.attributes = parameters.attributes;
    }
    if (parameters.attributeOverrides) {
      this.attributeOverrides = parameters.attributeOverrides;
    }
    this.derivedAttributes = parameters.derivedAttributes || {};
    this.linkedSources = parameters.linkedSources || {};
    if (parameters.delegates) {
      this.delegates = parameters.delegates;
    }
    this.concealBuckets = parameters.concealBuckets;
    // Lifted here so SQL externals (DruidSQLExternal in particular)
    // honour the timeAttribute the cube declares. DruidExternal still
    // re-assigns this in its own constructor with a fallback to
    // DruidExternal.TIME_ATTRIBUTE; that overwrite is benign — the
    // DruidExternal subclass runs after super() and its assignment wins.
    if (typeof parameters.timeAttribute === 'string' && parameters.timeAttribute.length > 0) {
      this.timeAttribute = parameters.timeAttribute;
    }

    this.rawAttributes = parameters.rawAttributes || parameters.attributes || [];
    this.requester = parameters.requester;

    this.mode = parameters.mode || 'raw';
    this.filter = parameters.filter || Expression.TRUE;
    this.specialApplyTransform = parameters.specialApplyTransform;

    if (this.rawAttributes.length) {
      this.derivedAttributes = External.typeCheckDerivedAttributes(
        this.derivedAttributes,
        this.getRawFullType(true),
      );
      this.filter = this.filter.changeInTypeContext(this.getRawFullType());
    }

    switch (this.mode) {
      case 'raw':
        this.select = parameters.select;
        this.sort = parameters.sort;
        this.limit = parameters.limit;
        break;

      case 'value':
        this.valueExpression = parameters.valueExpression;
        break;

      case 'total':
        this.applies = parameters.applies || [];
        break;

      case 'split':
        this.select = parameters.select;
        this.dataName = parameters.dataName;
        this.split = parameters.split;
        if (!this.split) throw new Error('must have split action in split mode');
        this.applies = parameters.applies || [];
        this.sort = parameters.sort;
        this.limit = parameters.limit;
        this.havingFilter = parameters.havingFilter || Expression.TRUE;
        break;
    }
  }

  protected _ensureEngine(engine: string) {
    if (!this.engine) {
      this.engine = engine;
      return;
    }
    if (this.engine !== engine) {
      throw new TypeError(`incorrect engine '${this.engine}' (needs to be: '${engine}')`);
    }
  }

  protected _ensureMinVersion(minVersion: string) {
    if (this.version && External.versionLessThan(this.version, minVersion)) {
      throw new Error(`only ${this.engine} versions >= ${minVersion} are supported`);
    }
  }

  public valueOf(): ExternalValue {
    const value: ExternalValue = {
      engine: this.engine,
      version: this.version,
      source: this.source,
      rollup: this.rollup,
      mode: this.mode,
    };
    if (this.suppress) value.suppress = this.suppress;
    if (this.attributes) value.attributes = this.attributes;
    if (this.attributeOverrides) value.attributeOverrides = this.attributeOverrides;
    if (nonEmptyLookup(this.derivedAttributes)) value.derivedAttributes = this.derivedAttributes;
    if (nonEmptyLookup(this.linkedSources)) value.linkedSources = this.linkedSources;
    if (this.delegates) value.delegates = this.delegates;
    value.concealBuckets = this.concealBuckets;
    if (this.timeAttribute) value.timeAttribute = this.timeAttribute;

    if (this.mode !== 'raw' && this.rawAttributes) {
      value.rawAttributes = this.rawAttributes;
    }
    if (this.requester) {
      value.requester = this.requester;
    }

    if (this.dataName) {
      value.dataName = this.dataName;
    }
    value.filter = this.filter;
    if (this.valueExpression) {
      value.valueExpression = this.valueExpression;
    }
    if (this.select) {
      value.select = this.select;
    }
    if (this.split) {
      value.split = this.split;
    }
    if (this.applies) {
      value.applies = this.applies;
    }
    if (this.sort) {
      value.sort = this.sort;
    }
    if (this.limit) {
      value.limit = this.limit;
    }
    if (this.havingFilter) {
      value.havingFilter = this.havingFilter;
    }
    if (this.specialApplyTransform) {
      value.specialApplyTransform = this.specialApplyTransform;
    }
    return value;
  }

  public toJS(): ExternalJS {
    const js: ExternalJS = {
      engine: this.engine,
      source: this.source,
    };
    if (this.version) js.version = this.version;
    if (this.rollup) js.rollup = true;
    if (this.attributes) js.attributes = AttributeInfo.toJSs(this.attributes);
    if (this.attributeOverrides)
      js.attributeOverrides = AttributeInfo.toJSs(this.attributeOverrides);
    if (nonEmptyLookup(this.derivedAttributes))
      js.derivedAttributes = Expression.expressionLookupToJS(this.derivedAttributes);
    if (nonEmptyLookup(this.linkedSources)) js.linkedSources = this.linkedSources;
    if (this.concealBuckets) js.concealBuckets = true;
    // timeAttribute round-trips on toJS for SQL externals (Druid SQL,
    // Postgres, MySQL). DruidExternal overrides this method and re-emits
    // it under its legacy "skip if equal to '__time'" rule; that override
    // wins because it runs after super().toJS().
    if (this.timeAttribute && this.engine !== 'druid') js.timeAttribute = this.timeAttribute;

    if (this.mode !== 'raw' && this.rawAttributes)
      js.rawAttributes = AttributeInfo.toJSs(this.rawAttributes);
    if (!this.filter.equals(Expression.TRUE)) {
      js.filter = this.filter.toJS();
    }
    return js;
  }

  public toJSON(): ExternalJS {
    return this.toJS();
  }

  public toString(): string {
    const { mode } = this;
    switch (mode) {
      case 'raw':
        return `ExternalRaw(${this.filter})`;

      case 'value':
        return `ExternalValue(${this.valueExpression})`;

      case 'total':
        return `ExternalTotal(${this.applies.length})`;

      case 'split':
        return `ExternalSplit(${this.split}, ${this.applies.length})`;

      default:
        throw new Error(`unknown mode: ${mode}`);
    }
  }

  public equals(other: External | undefined): boolean {
    return (
      this.equalBaseAndFilter(other) &&
      immutableLookupsEqual(this.derivedAttributes, other.derivedAttributes) &&
      immutableArraysEqual(this.attributes, other.attributes) &&
      immutableArraysEqual(this.delegates, other.delegates) &&
      this.concealBuckets === other.concealBuckets &&
      Boolean(this.requester) === Boolean(other.requester)
    );
  }

  public equalBaseAndFilter(other: External): boolean {
    return this.equalBase(other) && this.filter.equals(other.filter);
  }

  public equalBase(other: External): boolean {
    return (
      other instanceof External &&
      this.engine === other.engine &&
      String(this.source) === String(other.source) &&
      this.version === other.version &&
      this.rollup === other.rollup &&
      this.mode === other.mode
    );
  }

  public changeVersion(version: string) {
    const value = this.valueOf();
    value.version = version;
    return External.fromValue(value);
  }

  public attachRequester(requester: PlywoodRequester<any>): External {
    const value = this.valueOf();
    value.requester = requester;
    return External.fromValue(value);
  }

  public versionBefore(neededVersion: string): boolean {
    const { version } = this;
    return version && External.versionLessThan(version, neededVersion);
  }

  protected capability(_cap: string): boolean {
    return false;
  }

  public getAttributesInfo(attributeName: string) {
    const attributeInfo = NamedArray.get(this.rawAttributes, attributeName);
    if (!attributeInfo) throw new Error(`could not get attribute info for '${attributeName}'`);
    return attributeInfo;
  }

  public updateAttribute(newAttribute: AttributeInfo): External {
    if (!this.attributes) return this;
    const value = this.valueOf();
    value.attributes = AttributeInfo.override(value.attributes, [newAttribute]);
    return External.fromValue(value);
  }

  public show(): External {
    const value = this.valueOf();
    value.suppress = false;
    return External.fromValue(value);
  }

  public hasAttribute(name: string): boolean {
    const { attributes, rawAttributes, derivedAttributes } = this;
    if (SimpleArray.find(rawAttributes || attributes, a => a.name === name)) return true;
    return hasOwnProp(derivedAttributes, name);
  }

  public expressionDefined(ex: Expression): boolean {
    return ex.definedInTypeContext(this.getFullType());
  }

  public bucketsConcealed(ex: Expression) {
    return ex.every((ex, index, depth, nestDiff) => {
      if (nestDiff) return true;
      if (ex instanceof RefExpression) {
        const refAttributeInfo = this.getAttributesInfo(ex.name);
        if (refAttributeInfo && refAttributeInfo.maker instanceof TimeFloorExpression) {
          return refAttributeInfo.maker.alignsWith(ex);
        }
      } else if (ex instanceof ChainableExpression) {
        const refExpression = ex.operand;
        if (refExpression instanceof RefExpression) {
          const refAttributeInfo = this.getAttributesInfo(refExpression.name);
          if (refAttributeInfo && refAttributeInfo.maker instanceof TimeFloorExpression) {
            return refAttributeInfo.maker.alignsWith(ex);
          }
        }
      }
      return null;
    });
  }

  public changeSpecialApplyTransform(specialApplyTransform: SpecialApplyTransform): External {
    const value = this.valueOf();
    value.specialApplyTransform = specialApplyTransform;
    return External.fromValue(value);
  }

  // -----------------

  public abstract canHandleFilter(filter: FilterExpression): boolean;

  public abstract canHandleSort(sort: SortExpression): boolean;

  // -----------------

  public addDelegate(delegate: External): External {
    const value = this.valueOf();
    if (!value.delegates) value.delegates = [];
    value.delegates = value.delegates.concat(delegate);
    return External.fromValue(value);
  }

  public getBase(): External {
    const value = this.valueOf();
    value.suppress = true;
    value.mode = 'raw';
    value.dataName = null;
    if (this.mode !== 'raw') value.attributes = value.rawAttributes;
    value.rawAttributes = null;
    value.filter = null;
    value.applies = [];
    value.split = null;
    value.sort = null;
    value.limit = null;

    value.delegates = nullMap(value.delegates, e => e.getBase());
    return External.fromValue(value);
  }

  public getRaw(): External {
    if (this.mode === 'raw') return this;

    const value = this.valueOf();
    value.suppress = true;
    value.mode = 'raw';
    value.dataName = null;
    value.attributes = value.rawAttributes;
    value.rawAttributes = null;
    value.applies = [];
    value.split = null;
    value.sort = null;
    value.limit = null;
    value.specialApplyTransform = null;

    value.delegates = nullMap(value.delegates, e => e.getRaw());
    return External.fromValue(value);
  }

  public makeTotal(applies: ApplyExpression[]): External {
    if (this.mode !== 'raw') return null;

    if (!applies.length) throw new Error('must have applies');

    const externals: External[] = [];
    for (const apply of applies) {
      const applyExpression = apply.expression;
      if (applyExpression instanceof ExternalExpression) {
        externals.push(applyExpression.external);
      }
    }

    const commonFilter = External.getCommonFilterFromExternals(externals);

    const value = this.valueOf();
    value.mode = 'total';
    value.suppress = false;
    value.rawAttributes = value.attributes;
    // Only called with homogeneous externals (same base). The caller in
    // Dataset.ts groups applies by equalBase() before invoking this path, so
    // a schema-safe merge of derivedAttributes is guaranteed.
    value.derivedAttributes = External.getMergedDerivedAttributesFromExternals(externals);
    value.filter = commonFilter;
    value.attributes = [];
    value.applies = [];
    value.delegates = nullMap(value.delegates, e => e.makeTotal(applies));
    let totalExternal = External.fromValue(value);

    for (const apply of applies) {
      totalExternal = totalExternal._addApplyExpression(apply);
      if (!totalExternal) return null;
    }

    return totalExternal;
  }

  // Check to see if an expression is of the form timeRef.overlap(mainRange).then(timeRef).fallback(timeRef.timeShift(some_duration)).timeBucket(some_duration)
  private getHybridTimeExpressionDecomposition(
    possibleHybrid: Expression,
  ): HybridTimeBreakdown | undefined {
    if (possibleHybrid instanceof FallbackExpression) {
      const thenExpression = possibleHybrid.operand;
      const timeShiftExpression = possibleHybrid.expression;
      if (
        thenExpression instanceof ThenExpression &&
        timeShiftExpression instanceof TimeShiftExpression
      ) {
        const mainOverlap = thenExpression.operand;
        const timeRef = timeShiftExpression.operand;
        if (mainOverlap instanceof OverlapExpression && this.isTimeRef(timeRef)) {
          const mainOverlapLiteral = mainOverlap.expression;
          if (mainOverlapLiteral instanceof LiteralExpression) {
            return {
              timeRef,
              mainRangeLiteral: mainOverlapLiteral,
              timeShift: timeShiftExpression,
            };
          }
        }
      }
    }
    return undefined;
  }

  private _addFilterForNext(ex: Expression): External {
    // If we have a filter on hybrid time expression like:
    // timeRef.overlap(mainRange).then(timeRef).fallback(timeRef.timeShift(some_duration)) .overlap(time_range)
    // do special logic to add the filter correctly
    let hybridTimeBreakdown: HybridTimeBreakdown | undefined;
    let curTimeRange: TimeRange | undefined;
    const extractAndRest = ex.extractFromAnd(possibleHybrid => {
      if (possibleHybrid instanceof OverlapExpression) {
        const { operand, expression } = possibleHybrid;

        const possibleHybridTimeBreakdown = this.getHybridTimeExpressionDecomposition(operand);

        if (possibleHybridTimeBreakdown && expression instanceof LiteralExpression) {
          const literalValue = expression.getLiteralValue();
          if (literalValue instanceof TimeRange) {
            hybridTimeBreakdown = possibleHybridTimeBreakdown;
            curTimeRange = literalValue;
            return true;
          }
        }
      }
      return false;
    });

    if (hybridTimeBreakdown) {
      const { timeRef, timeShift, mainRangeLiteral } = hybridTimeBreakdown;

      // Transform filter
      const prevTimeRange = curTimeRange.shift(
        timeShift.duration,
        timeShift.getTimezone() || Timezone.UTC,
        -timeShift.step, // reverse the shift
      );

      const newTimeFilter = timeRef.overlap(
        new Set({
          setType: 'TIME_RANGE',
          elements: [curTimeRange, prevTimeRange],
        }),
      );

      return this._addFilterExpression(
        Expression._.filter(Expression.and([newTimeFilter, extractAndRest.rest])),
      ).changeSpecialApplyTransform({
        mainRangeLiteral, // Transform apply filters
        curTimeRange,
        prevTimeRange,
      });
    }

    return this._addFilterExpression(Expression._.filter(ex));
  }

  public addExpression(ex: Expression): External {
    if (ex instanceof FilterExpression) {
      return this._addFilterExpression(ex);
    }
    if (ex instanceof SelectExpression) {
      return this._addSelectExpression(ex);
    }
    if (ex instanceof SplitExpression) {
      return this._addSplitExpression(ex);
    }
    if (ex instanceof ApplyExpression) {
      return this._addApplyExpression(ex);
    }
    if (ex instanceof SortExpression) {
      return this._addSortExpression(ex);
    }
    if (ex instanceof LimitExpression) {
      return this._addLimitExpression(ex);
    }
    if (ex.isAggregate()) {
      return this._addAggregateExpression(ex);
    }
    return this._addPostAggregateExpression(ex);
  }

  private _addFilterExpression(filter: FilterExpression): External {
    const { expression } = filter;
    if (!expression.resolvedWithoutExternals()) return null;
    if (!this.expressionDefined(expression)) return null;

    const value = this.valueOf();
    switch (this.mode) {
      case 'raw':
        if (this.concealBuckets && !this.bucketsConcealed(expression)) return null;
        if (!this.canHandleFilter(filter)) return null;
        if (value.filter.equals(Expression.TRUE)) {
          value.filter = expression;
        } else {
          value.filter = value.filter.and(expression);
        }
        break;

      case 'split':
        if (this.limit) return null;
        value.havingFilter = value.havingFilter.and(expression).simplify();
        break;

      default:
        return null; // can not add filter in total mode
    }

    value.delegates = nullMap(value.delegates, e => e._addFilterExpression(filter));
    return External.fromValue(value);
  }

  private _addSelectExpression(selectExpression: SelectExpression): External {
    const { mode } = this;
    if (mode !== 'raw' && mode !== 'split') return null; // Can only select on 'raw' or 'split' datasets

    const { datasetType } = this.getFullType();
    const { attributes } = selectExpression;
    for (const attribute of attributes) {
      if (!datasetType[attribute]) return null;
    }

    const value = this.valueOf();
    value.suppress = false;
    value.select = selectExpression;
    value.delegates = nullMap(value.delegates, e => e._addSelectExpression(selectExpression));

    if (mode === 'split') {
      value.applies = value.applies.filter(apply => attributes.indexOf(apply.name) !== -1);
      value.attributes = value.attributes.filter(
        attribute => attributes.indexOf(attribute.name) !== -1,
      );
    }

    return External.fromValue(value);
  }

  private _addSplitExpression(split: SplitExpression): External {
    if (this.mode !== 'raw') return null; // Can only split on 'raw' datasets
    const splitKeys = split.keys;
    for (const splitKey of splitKeys) {
      const splitExpression = split.splits[splitKey];
      if (!this.expressionDefined(splitExpression)) return null;
      if (this.concealBuckets && !this.bucketsConcealed(splitExpression)) return null;
    }

    const value = this.valueOf();
    value.suppress = false;
    value.mode = 'split';
    value.dataName = split.dataName;
    value.split = split;
    value.rawAttributes = value.attributes;
    value.attributes = split.mapSplits(
      (name, expression) => new AttributeInfo({ name, type: Set.unwrapSetType(expression.type) }),
    );
    value.delegates = nullMap(value.delegates, e => e._addSplitExpression(split));
    return External.fromValue(value);
  }

  private _addApplyExpression(apply: ApplyExpression): External {
    const expression = apply.expression;
    if (expression.type === 'DATASET') return null;
    if (!expression.resolved()) return null;
    if (!this.expressionDefined(expression)) return null;

    let value: ExternalValue;
    if (this.mode === 'raw') {
      value = this.valueOf();
      value.derivedAttributes = immutableAdd(value.derivedAttributes, apply.name, apply.expression);
    } else {
      if (this.specialApplyTransform) {
        const { mainRangeLiteral, curTimeRange, prevTimeRange } = this.specialApplyTransform;
        apply = apply.changeExpression(
          apply.expression
            .substitute(ex => {
              if (
                ex instanceof OverlapExpression &&
                this.isTimeRef(ex.operand) &&
                ex.expression instanceof LiteralExpression
              ) {
                return ex.changeExpression(
                  r(mainRangeLiteral.equals(ex.expression) ? curTimeRange : prevTimeRange),
                );
              }
              return null;
            })
            .simplify(),
        );
      }

      // Can not redefine index for now.
      if (this.split && this.split.hasKey(apply.name)) return null;

      const applyExpression = apply.expression;
      if (applyExpression instanceof ExternalExpression) {
        // When the apply IS a whole external, there are two cases:
        //   - equalBase(other): same datasource seen through another lens
        //     (filter etc.) — collapse into our own SQL via valueExpression.
        //   - !equalBase(other) AND other is one of our linkedSources: leave
        //     the ExternalExpression intact so getCrossExternalDecomposition
        //     can route this apply to the foreign sub-query downstream.
        let isDeclaredLinked = false;
        if (this.linkedSources) {
          const foreignSource = String(applyExpression.external.source);
          for (const lsName in this.linkedSources) {
            if (String(this.linkedSources[lsName].source) === foreignSource) {
              isDeclaredLinked = true;
              break;
            }
          }
        }
        if (!isDeclaredLinked) {
          apply = apply.changeExpression(
            applyExpression.external.valueExpressionWithinFilter(this.filter),
          );
        }
      }

      value = this.valueOf();
      const added = External.normalizeAndAddApply(value, apply);
      value.applies = added.applies;
      value.attributes = added.attributes;
    }
    value.delegates = nullMap(value.delegates, e => e._addApplyExpression(apply));
    return External.fromValue(value);
  }

  private _addSortExpression(sort: SortExpression): External {
    if (this.limit) return null; // Can not sort after limit
    if (!this.canHandleSort(sort)) return null;

    const value = this.valueOf();
    value.sort = sort;
    value.delegates = nullMap(value.delegates, e => e._addSortExpression(sort));
    return External.fromValue(value);
  }

  private _addLimitExpression(limit: LimitExpression): External {
    const value = this.valueOf();
    value.suppress = false;
    if (!value.limit || limit.value < value.limit.value) {
      value.limit = limit;
    }
    value.delegates = nullMap(value.delegates, e => e._addLimitExpression(limit));
    return External.fromValue(value);
  }

  private _addAggregateExpression(aggregate: Expression): External {
    if (this.mode === 'split') {
      if (aggregate.type !== 'NUMBER') return null; // Only works for numbers, avoids 'collect'
      // This is in case of a resplit that needs to be folded

      let valueExpression = $(External.SEGMENT_NAME, 'DATASET').performAction(
        this.split.getAction(),
      );
      this.applies.forEach(apply => {
        valueExpression = valueExpression.performAction(apply.getAction());
      });
      valueExpression = valueExpression.performAction(aggregate);

      const value = this.valueOf();
      value.mode = 'value';
      value.suppress = false;
      value.valueExpression = valueExpression;
      value.attributes = null;
      value.delegates = nullMap(value.delegates, e => e._addAggregateExpression(aggregate));
      return External.fromValue(value);
    }

    if (this.mode !== 'raw' || this.limit) return null; // Can not value aggregate something with a limit
    if (aggregate instanceof ChainableExpression) {
      if (aggregate instanceof ChainableUnaryExpression) {
        if (!this.expressionDefined(aggregate.expression)) return null;
      }

      const value = this.valueOf();
      value.mode = 'value';
      value.suppress = false;
      value.valueExpression = aggregate.changeOperand($(External.SEGMENT_NAME, 'DATASET'));
      value.rawAttributes = value.attributes;
      value.attributes = null;
      value.delegates = nullMap(value.delegates, e => e._addAggregateExpression(aggregate));
      return External.fromValue(value);
    } else {
      return null;
    }
  }

  private _addPostAggregateExpression(action: Expression): External {
    if (this.mode !== 'value')
      throw new Error('must be in value mode to call addPostAggregateExpression');
    if (action instanceof ChainableExpression) {
      if (!action.operand.equals(Expression._)) return null;

      let commonFilter = this.filter;
      let newValueExpression: Expression;

      if (action instanceof ChainableUnaryExpression) {
        const actionExpression = action.expression;
        if (actionExpression instanceof ExternalExpression) {
          const otherExternal = actionExpression.external;
          if (!this.equalBase(otherExternal)) return null;

          commonFilter = getCommonFilter(commonFilter, otherExternal.filter);
          const newExpression = action.changeExpression(
            otherExternal.valueExpressionWithinFilter(commonFilter),
          );
          newValueExpression =
            this.valueExpressionWithinFilter(commonFilter).performAction(newExpression);
        } else if (!actionExpression.hasExternal()) {
          newValueExpression = this.valueExpression.performAction(action);
        } else {
          return null;
        }
      } else {
        newValueExpression = this.valueExpression.performAction(action);
      }

      const value = this.valueOf();
      value.valueExpression = newValueExpression;
      value.filter = commonFilter;
      value.delegates = nullMap(value.delegates, e => e._addPostAggregateExpression(action));
      return External.fromValue(value);
    } else {
      return null;
    }
  }

  public prePush(ex: ChainableUnaryExpression): External {
    if (this.mode !== 'value') return null;
    if (ex.type === 'DATASET') return null;
    if (!ex.operand.noRefs() || !ex.expression.equals(Expression._)) return null;

    const value = this.valueOf();
    value.valueExpression = ex.changeExpression(value.valueExpression);
    value.delegates = nullMap(value.delegates, e => e.prePush(ex));
    return External.fromValue(value);
  }

  // ----------------------

  public valueExpressionWithinFilter(withinFilter: Expression): Expression {
    if (this.mode !== 'value') return null;
    const extraFilter = filterDiff(this.filter, withinFilter);
    if (!extraFilter) throw new Error('not within the segment');
    return External.addExtraFilter(this.valueExpression, extraFilter);
  }

  public toValueApply(): ApplyExpression {
    if (this.mode !== 'value') return null;
    return Expression._.apply(External.VALUE_NAME, this.valueExpression);
  }

  public sortOnLabel(): boolean {
    const sort = this.sort;
    if (!sort) return false;

    const sortOn = (<RefExpression>sort.expression).name;
    if (!this.split || !this.split.hasKey(sortOn)) return false;

    const applies = this.applies;
    for (const apply of applies) {
      if (apply.name === sortOn) return false;
    }

    return true;
  }

  public getQuerySplit(): SplitExpression {
    return this.split.transformExpressions(ex => {
      return this.inlineDerivedAttributes(ex);
    });
  }

  public getQueryFilter(): Expression {
    let filter = this.inlineDerivedAttributes(this.filter).simplify();

    if (filter instanceof RefExpression && !this.capability('filter-on-attribute')) {
      filter = filter.is(true);
    }

    return filter;
  }

  public inlineDerivedAttributes(expression: Expression): Expression {
    const { derivedAttributes } = this;
    return expression.substitute(refEx => {
      if (refEx instanceof RefExpression) {
        const refName = refEx.name;
        return derivedAttributes[refName] || null;
      } else {
        return null;
      }
    });
  }

  public getSelectedAttributes(): Attributes {
    let { mode, select, attributes, derivedAttributes } = this;
    if (mode === 'raw') {
      for (const k in derivedAttributes) {
        attributes = attributes.concat(
          new AttributeInfo({ name: k, type: derivedAttributes[k].type }),
        );
      }
    }
    if (!select) return attributes;
    const selectAttributes = select.attributes;
    return selectAttributes.map(s => NamedArray.findByName(attributes, s));
  }

  public getValueType(): PlyTypeSimple {
    const { valueExpression } = this;
    if (!valueExpression) return null;
    return valueExpression.type as PlyTypeSimple;
  }

  // -----------------

  public addNextExternalToDatum(datum: Datum): void {
    const { mode, dataName, split } = this;
    if (mode !== 'split') throw new Error('must be in split mode to addNextExternalToDatum');
    datum[dataName] = this.getRaw()._addFilterForNext(split.filterFromDatum(datum));
  }

  public getDelegate(): External {
    const { mode, delegates } = this;
    if (!delegates || !delegates.length || mode === 'raw') return null;
    return delegates[0];
  }

  /**
   * SEMIJOIN-TO-ROOT for a TOTALS query carrying a harvested linked-only
   * filter clause.
   *
   * The bug (Ismael 2026-06-02): a datum-root TOTALS query
   * (`this.mode !== 'split'`) whose filter restricts a column that lives ONLY
   * on a linked-source lookup (e.g. `$brand_country.overlap(['Francia'])`)
   * silently drops that restriction. `pruneLinkedFilterRefsInTree` DOES harvest
   * the clause onto the per-request `config.filter` slot, but its ONLY consumer
   * — `getCrossExternalDecomposition` — returns null on its first line for any
   * non-split mode (`if (this.mode !== 'split') return null`). With no
   * cross-external decomposition, both `simulateValue` and
   * `queryBasicValueStream` fall through to the plain single-external path whose
   * WHERE is `getQueryFilter()` (main's filter only). The linked-only column
   * was pruned off main → the totals SQL carries NO predicate → the aggregate
   * is computed over the whole period (all countries), not the filtered subset.
   *
   * This is PATH-INDEPENDENT: it bites EVERY measure. A non-decomposable measure
   * (countDistinct) would route through native-JOIN on a split; a decomposable
   * one (avg/sum) takes the flat route — but on a TOTALS query NEITHER reaches
   * the cross-external consumer, so both leak the all-country value. Verified
   * empirically: the `AVG("price")` totals SQL is byte-identical with and
   * without the Francia clause.
   *
   * The fix: a brand-set semijoin lifted to the datum-root grain. Run the lookup
   * side first (`SELECT DISTINCT <joinKey> FROM lookup WHERE <linked-clause>`),
   * collect the joinKey set, then restrict the TOTALS main query with
   * `main.<joinKey> IN (<set>)`. With `joinMode: inner` an IN-list is the exact
   * algebraic equivalent of the inner JOIN — every main row whose joinKey maps
   * to (e.g.) Francia survives, the rest are dropped, the aggregate is over the
   * filtered subset. An empty set (country that maps to no brand) yields
   * `IN ()` → no rows → total 0 (NOT the all-country leak).
   *
   * Returns null when this shape does not apply, in which case the caller falls
   * through to today's exact path (orthogonality: a totals query with NO
   * harvested linked-only clause emits byte-identical SQL to before — no lookup,
   * no IN-list). Only fires when:
   *   - `this.mode !== 'split'` (a split is handled by the cross-external path);
   *   - there is exactly one linkedSource carrying a non-TRIVIAL harvested
   *     `config.filter` (the per-request clause minted by
   *     pruneLinkedFilterRefsInTree);
   *   - `getCrossExternalDecomposition()` returned null (we never double-handle
   *     a shape the split path already owns).
   *
   * `joinMode: 'left'` FAILS LOUD: an IN-list is inner semantics; a left join
   * keeps orphan main rows, so an IN-list would silently change the result.
   * Parity with the left-join pin in the native-JOIN path.
   */
  public getSemijoinToRootDecomposition(): {
    joinKey: string;
    joinKeyType: PlyType;
    lookupExternal: External;
    buildFilteredMain: (joinKeyValues: any[]) => External;
  } | null {
    // Only a NON-split (totals / value) query: a split is owned by the
    // cross-external path. `raw` mode never carries applies/aggregates to
    // restrict, so it is out of scope.
    if (this.mode === 'raw') return null;
    if (!this.linkedSources || Object.keys(this.linkedSources).length === 0) return null;

    // The cross-external (split) path already owns any shape
    // getCrossExternalDecomposition accepts — the linked-only-split native-JOIN
    // and the JS-join. Never double-handle: when that path returns non-null this
    // branch stands down. The shapes that reach HERE are exactly the ones it
    // rejects:
    //   - a TOTALS row (`this.mode === 'total'`) — the 4314 `mode !== 'split'`
    //     gate makes getCrossExternalDecomposition return null;
    //   - a split on a MAIN-only dimension (e.g. `brand`) with a linked-only
    //     filter and NO linked split/measure — no foreign apply for the cross-
    //     external partitioner to seed a sub-plan from, so it returns null too.
    // BOTH have the same defect: the harvested linked-only clause has no
    // consumer and is silently dropped. The semijoin-to-root IN-list fixes
    // both: the lookup DISTINCT-joinKey set narrows `main.<joinKey> IN (…)`,
    // and the rest of the query (the main-dim split or the bare totals) runs
    // unchanged through the flat path.
    if (this.getCrossExternalDecomposition()) return null;

    // The semijoin-to-root ONLY rescues an ORPHANED linked-only filter — one
    // that has NO live consumer. `pruneLinkedFilterRefsInTree` decides this at
    // harvest time (where it can see the whole expression, before resolve peels
    // sibling sub-externals apart) and marks the per-request config
    // `semijoinToRoot: true` exactly when no value apply or split references the
    // lookup. When a linked measure apply or linked-only split DOES reference it
    // (e.g. `avg_rating = $reviews.average($rating)`), the established
    // decomposition path materialises a sibling sub-query that already honours
    // the harvested filter — firing here would double-handle (and, for a
    // multi-key lookup, throw on the single-joinKey assumption). So we gate on
    // the flag, NOT on `this`'s post-decomposition apply set (the linked apply
    // has already been peeled onto the sibling by the time simulateValue/
    // queryBasicValueStream reach this external).
    //
    // Find the single linkedSource carrying a harvested linked-only clause
    // (config.filter present, not TRUE, AND flagged orphaned). More than one is
    // out of scope for v1 — fail loud rather than guess which IN-list to
    // compose.
    const owners: string[] = [];
    for (const lsName in this.linkedSources) {
      const cfg = this.linkedSources[lsName] as any;
      const f = cfg.filter as Expression | undefined;
      if (f && !f.equals(Expression.TRUE) && cfg.semijoinToRoot === true) owners.push(lsName);
    }
    if (owners.length === 0) return null;
    if (owners.length > 1) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `semijoin-to-root: ${owners.length} linkedSources [${owners.join(
          ', ',
        )}] carry a linked-only filter clause at once — composing multiple ` +
          `independent IN-list semijoins on a single query is not supported`,
      );
    }

    const lsName = owners[0];
    const config = this.linkedSources[lsName] as any;

    const joinMode = External.resolveLinkedJoinMode(config);
    if (joinMode !== 'inner') {
      // 'left' (or a missing/other mode) cannot be expressed as a main-side
      // IN-list: a left join keeps orphan main rows, so an IN-list would
      // silently DISCARD rows the user asked to keep. Refuse loudly — parity
      // with the native-JOIN left-join pin. (linked/filter/join in the text
      // for the pin matcher.)
      throw new PlywoodUnsupportedNativeJoinShape(
        `semijoin-to-root for linkedSource "${lsName}": joinMode="${
          joinMode || 'undefined'
        }" cannot honour a linked-only filter via a main-side IN-list — an IN-list is ` +
          `INNER-join semantics; a left join keeps orphan main rows and would not ` +
          `restrict the result. Declare joinMode:'inner' or split on the linked dim.`,
      );
    }

    const joinKeys: string[] = config.joinKeys || [];
    if (joinKeys.length !== 1) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `semijoin-to-root for linkedSource "${lsName}": expected exactly one ` +
          `joinKey, got [${joinKeys.join(', ')}]. A multi-key semijoin (IN-list over a ` +
          `tuple) is not supported.`,
      );
    }
    const joinKey = joinKeys[0];

    // ── Build the lookup-side raw external (SELECT DISTINCT <joinKey> WHERE
    // <clause>). Same recipe as the JS-join template (getCrossExternalDecom-
    // position ~4727): normalize attributes/derived, prune main's filter to the
    // lookup schema (honouring timeAlignment:'eternal'), AND in the harvested
    // linked-only clause, resolve the engine binding.
    const normalizedAttributes: Attributes | undefined = config.attributes
      ? (config.attributes as any[]).map(a =>
          a instanceof AttributeInfo ? a : AttributeInfo.fromJS(a),
        )
      : undefined;
    let normalizedDerived: Record<string, Expression> | undefined;
    if (config.derivedAttributes) {
      normalizedDerived = {};
      for (const k in config.derivedAttributes) {
        const v = config.derivedAttributes[k];
        normalizedDerived[k] = v instanceof Expression ? v : Expression.fromJSLoose(v);
      }
    }

    const timeAttrName =
      (typeof (this as any).getTimeAttribute === 'function'
        ? (this as any).getTimeAttribute()
        : undefined) || (this as any).timeAttribute;
    const timeAlignment = config.timeAlignment;
    const linkedSchemaNames: Record<string, true> = {};
    if (normalizedAttributes) {
      for (const a of normalizedAttributes) {
        if (timeAlignment === 'eternal' && a.name === timeAttrName) continue;
        linkedSchemaNames[a.name] = true;
      }
    }
    if (normalizedDerived) {
      for (const k in normalizedDerived) {
        if (timeAlignment === 'eternal' && k === timeAttrName) continue;
        linkedSchemaNames[k] = true;
      }
    }
    const prunedMainFilter = External.pruneFilterToSchema(this.filter, linkedSchemaNames);
    const stashedLinkedFilter: Expression = config.filter;
    const templateFilter =
      prunedMainFilter && !prunedMainFilter.equals(Expression.TRUE)
        ? prunedMainFilter.and(stashedLinkedFilter).simplify()
        : stashedLinkedFilter;
    const templateAllowEternity = timeAlignment === 'eternal' ? true : (this as any).allowEternity;

    const binding = External.resolveLinkedEngineBinding(
      config,
      lsName,
      this.engine,
      this.version,
      this.requester,
    );

    // Resolve the joinKey's native type on the lookup side so the DISTINCT
    // split rebuilds with the right type before it's collected.
    let joinKeyType: PlyType = 'STRING';
    if (config.attributes) {
      for (const a of config.attributes as any[]) {
        if (a.name === joinKey && a.type) joinKeyType = a.type;
      }
    }

    const lookupBase = External.fromValue({
      engine: binding.engine,
      version: binding.version,
      source: config.source,
      suppress: true,
      rollup: this.rollup,
      concealBuckets: this.concealBuckets,
      requester: binding.requester,
      attributes: normalizedAttributes,
      derivedAttributes: normalizedDerived,
      filter: templateFilter,
      timeAttribute: (this as any).timeAttribute,
      customAggregations: (this as any).customAggregations,
      customTransforms: (this as any).customTransforms,
      allowEternity: templateAllowEternity,
      allowSelectQueries: (this as any).allowSelectQueries,
      exactResultsOnly: (this as any).exactResultsOnly,
      querySelection: (this as any).querySelection,
      context: (this as any).context,
    });
    // SELECT DISTINCT <joinKey> = a split on the joinKey with no measures.
    const lookupExternal = lookupBase.addExpression(
      Expression._.split($(joinKey, joinKeyType), joinKey, 'main'),
    );
    if (!lookupExternal) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `semijoin-to-root for "${lsName}": lookup rejected the DISTINCT split on ` +
          `joinKey "${joinKey}" — the column must resolve in the lookup schema`,
      );
    }

    // ── Build the filtered TOTALS main: today's `this`, restricted by
    // `main.<joinKey> IN (<set>)`, with linkedSources STRIPPED so the derived
    // external dispatches through the plain single-external path (it must NOT
    // re-enter this branch — there is no harvested clause left to honour once
    // it's been folded into the IN-list). Per-request derivation only; the
    // shared External is never mutated.
    const buildFilteredMain = (joinKeyValues: any[]): External => {
      const inSet = Set.fromJS({
        setType: joinKeyType === 'NUMBER' ? 'NUMBER' : 'STRING',
        elements: joinKeyValues,
      });
      const inList = $(joinKey, joinKeyType).overlap(r(inSet));
      const value = this.valueOf();
      value.filter = this.filter.and(inList).simplify();
      value.linkedSources = {};
      return External.fromValue(value);
    };

    return { joinKey, joinKeyType, lookupExternal, buildFilteredMain };
  }

  public simulateValue(
    lastNode: boolean,
    simulatedQueries: any[],
    externalForNext: External = null,
  ): PlywoodValue | TotalContainer {
    const { mode } = this;

    if (!externalForNext) externalForNext = this;

    const delegate = this.getDelegate();
    if (delegate) {
      return delegate.simulateValue(lastNode, simulatedQueries, externalForNext);
    }

    // Cross-external decomposition: when foreign applies are present, simulate
    // a query per sub-external. The synthetic dataset keeps the combined shape
    // so downstream consumers see the post-join schema. queryBasicValueStream
    // has the equivalent wiring for the async path.
    const crossExt = this.getCrossExternalDecomposition();
    if (crossExt) {
      // Phase 4 native-JOIN path: single combined SQL replaces the
      // main + linked pair. Push exactly one query into the simulate
      // log; the synthesised Dataset still carries the user's split
      // shape so downstream consumers see the post-join schema.
      if (crossExt.kind === 'nativeJoin' && crossExt.nativeJoin) {
        // Match the wrapping shape druidSqlExternal.sqlToQuery uses
        // so the simulate plan looks structurally identical to a
        // normal single-source SQL query.
        simulatedQueries.push({
          query: crossExt.nativeJoin.sql,
          context: { ...((this as any).context || {}), sqlTimeZone: 'Etc/UTC' },
        });
        const datum: Datum = {};
        if (this.split) {
          this.split.mapSplits((name, expression) => {
            datum[name] = getSampleValue(Set.unwrapSetType(expression.type), expression);
          });
        }
        for (const apply of this.applies) {
          datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
        }
        return new Dataset({
          keys: this.split ? this.split.mapSplits(name => name) : null,
          data: [datum],
        });
      }
      crossExt.mainExternal.simulateValue(lastNode, simulatedQueries, externalForNext);
      for (const le of crossExt.linkedExternals) {
        le.external.simulateValue(lastNode, simulatedQueries, externalForNext);
      }
      // Synthesize a representative Dataset: split keys + all applies (main + linked)
      const datum: Datum = {};
      this.split.mapSplits((name, expression) => {
        datum[name] = getSampleValue(Set.unwrapSetType(expression.type), expression);
      });
      for (const apply of this.applies) {
        datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
      }
      return new Dataset({ keys: this.split.mapSplits(name => name), data: [datum] });
    }

    // Time-shift decomposition (Ogievetsky's groupAppliesByTimeFilterValue):
    // when two apply groups differ only by an overlap filter on the time
    // ref, split into two sub-Externals — one per period. queryBasicValue
    // Stream does this via leftJoin/fullJoin; simulation just needs each
    // sub-query pushed so callers can assert on query count and shape.
    const timeDecomposed = this.getJoinDecompositionShortcut();
    if (timeDecomposed) {
      timeDecomposed.external1.simulateValue(lastNode, simulatedQueries, externalForNext);
      timeDecomposed.external2.simulateValue(lastNode, simulatedQueries, externalForNext);
      const datum: Datum = {};
      if (this.split) {
        this.split.mapSplits((name, expression) => {
          datum[name] = getSampleValue(Set.unwrapSetType(expression.type), expression);
        });
      }
      for (const apply of this.applies || []) {
        datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
      }
      return new Dataset({
        keys: this.split ? this.split.mapSplits(name => name) : null,
        data: [datum],
      });
    }

    // Semijoin-to-root: a TOTALS query carrying a harvested linked-only filter
    // clause. Run the lookup DISTINCT-joinKey sub-query first, then dispatch a
    // filtered main with `main.<joinKey> IN (<set>)`. IDENTICAL logic to the
    // async path in queryBasicValueStream so simulate and execution never
    // diverge. In simulate there is no engine to read the joinKey set from, so
    // we restrict main with a SAMPLE one-element set — enough to prove the
    // IN-list semijoin shape reaches the totals SQL.
    const semijoin = this.getSemijoinToRootDecomposition();
    if (semijoin) {
      semijoin.lookupExternal.simulateValue(lastNode, simulatedQueries, externalForNext);
      const sample = [getSampleValue(Set.unwrapSetType(semijoin.joinKeyType), null)];
      return semijoin
        .buildFilteredMain(sample)
        .simulateValue(lastNode, simulatedQueries, externalForNext);
    }

    simulatedQueries.push(this.getQueryAndPostTransform().query);

    if (mode === 'value') {
      const valueExpression = this.valueExpression;
      return getSampleValue(valueExpression.type, valueExpression);
    }

    let keys: string[] = null;
    const datum: Datum = {};
    if (mode === 'raw') {
      const attributes = this.attributes;
      for (const attribute of attributes) {
        datum[attribute.name] = getSampleValue(attribute.type, null);
      }
    } else {
      if (mode === 'split') {
        this.split.mapSplits((name, expression) => {
          datum[name] = getSampleValue(Set.unwrapSetType(expression.type), expression);
        });
        keys = this.split.mapSplits(name => name);
      }

      const applies = this.applies;
      for (const apply of applies) {
        datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
      }
    }

    if (mode === 'total') {
      return new TotalContainer(datum);
    }

    if (!lastNode && mode === 'split') {
      externalForNext.addNextExternalToDatum(datum);
    }
    return new Dataset({
      keys,
      data: [datum],
    });
  }

  public getQueryAndPostTransform(): QueryAndPostTransform<any> {
    throw new Error('can not call getQueryAndPostTransform directly');
  }

  public queryValue(
    lastNode: boolean,
    rawQueries: any[],
    externalForNext: External = null,
  ): Promise<PlywoodValue | TotalContainer> {
    const stream = this.queryValueStream(lastNode, rawQueries, externalForNext);
    const valuePromise = External.buildValueFromStream(stream);

    if (this.mode === 'total') {
      return valuePromise.then(v => {
        return v instanceof Dataset && v.data.length === 1 ? new TotalContainer(v.data[0]) : v;
      });
    }

    return valuePromise;
  }

  protected queryBasicValueStream(rawQueries: any[] | null): ReadableStream {
    const crossExt = this.getCrossExternalDecomposition();
    if (crossExt) {
      // Phase 4 native-JOIN path: dispatch the single combined SQL
      // via the main external's requester. The result is the
      // already-grouped, already-joined dataset; we just need to
      // attach keys + inflaters and hand it back.
      if (crossExt.kind === 'nativeJoin' && crossExt.nativeJoin) {
        const nj = crossExt.nativeJoin;
        return External.valuePromiseToStream(
          new Promise<PlywoodValue>((resolve, reject) => {
            const requester = this.requester;
            if (!requester) {
              reject(
                new Error(
                  'Cross-source native-JOIN: external has no requester to dispatch the combined SQL',
                ),
              );
              return;
            }
            if (rawQueries) rawQueries.push({ engine: this.engine, query: nj.sql });
            // Druid SQL: requester expects `query: { query: <sql> }`.
            // Other engines accept the bare string. Match Druid's
            // nesting unconditionally — non-Druid engines that take
            // this path can override via subclass-aware wiring (out
            // of scope for v1 — native-JOIN currently only fires on
            // the Druid SQL transport).
            const reqStream = requester({
              query: { query: nj.sql } as any,
              context: (this as any).context,
            });
            const rows: Datum[] = [];
            reqStream.on('data', (r: any) => rows.push(r));
            reqStream.on('error', (e: any) => reject(e));
            reqStream.on('end', () => {
              // Build attributes + inflaters from splitKeyAttributes when the
              // combined SQL carries more than the linked key (e.g. a Time(Day)
              // main-side bucket): the time column must inflate to a TimeRange
              // (not a bare STRING) and the dataset must be keyed on EVERY split.
              // Fall back to the single-key shape for plain linked-only splits.
              const skAttrs =
                nj.splitKeyAttributes && nj.splitKeyAttributes.length > 0
                  ? nj.splitKeyAttributes
                  : [{ name: nj.splitAlias, type: 'STRING' as PlyType, splitExpr: null as any }];
              const splitAttributeInfos = skAttrs.map(
                sk => new AttributeInfo({ name: sk.name, type: sk.type }),
              );
              const inflaters: Inflater[] = [];
              for (const sk of skAttrs) {
                if (sk.splitExpr) {
                  const inf = External.getIntelligentInflater(sk.splitExpr, sk.name);
                  if (inf) inflaters.push(inf);
                }
              }
              if (inflaters.length > 0) {
                for (const r of rows) for (const inf of inflaters) inf(r);
              }
              let ds = new Dataset({
                keys: nj.keys,
                attributes: [
                  ...splitAttributeInfos,
                  ...nj.applyNames.map(n => new AttributeInfo({ name: n, type: 'NUMBER' })),
                ],
                data: rows,
              });
              // Post-join HAVING / sort / limit (mirror of the jsJoin branch at
              // ~3341-3349). The engine returned the JOIN+GROUP BY rows; the
              // HAVING is applied here by alias (the Dataset is keyed by
              // applyNames). When a HAVING is present the combined SQL stripped
              // its inline ORDER BY / LIMIT, so sort/limit must run AFTER the
              // filter — order filter → sort → limit — else a pre-filter cut
              // would starve the surviving rows.
              if (crossExt.postJoinHavingFilter) {
                ds = ds.filter(crossExt.postJoinHavingFilter);
              }
              if (crossExt.postJoinSort) {
                ds = ds.sort(crossExt.postJoinSort.expression, crossExt.postJoinSort.direction);
              }
              if (crossExt.postJoinLimit) {
                ds = ds.limit(crossExt.postJoinLimit.value);
              }
              External.assertDatasetShape(ds); // INV-1: also pin the native-JOIN result.
              resolve(ds);
            });
          }),
        );
      }
      const mainPromise = External.buildValueFromStream(
        crossExt.mainExternal.queryBasicValueStream(rawQueries),
      );
      const linkedPromises = crossExt.linkedExternals.map(le =>
        External.buildValueFromStream(le.external.queryBasicValueStream(rawQueries)),
      );
      return External.valuePromiseToStream(
        Promise.all([mainPromise, ...linkedPromises]).then(([main, ...linked]) => {
          // If main is a totals-only dataset (no split keys), its rows
          // broadcast onto every linked row. Dataset.join's auto-dispatch
          // only handles "other's keys ⊂ this.keys" as broadcast — for the
          // inverse case (this.keys empty, other has keys) we must call
          // join from the linked side so the matcher sees main as a
          // broadcastable subset.
          const mainIsTotals = !(main as Dataset).keys || (main as Dataset).keys.length === 0;
          let joined: Dataset;
          // Per-source join mode comes from the decomposition — each
          // linked sub declared its own 'inner' | 'left' via config.
          // Mixed modes are fine: the first linked's mode governs the
          // main↔first join, subsequent sides apply their own mode
          // against the accumulating joined Dataset.
          if (mainIsTotals && linked.length > 0) {
            joined = linked[0] as Dataset;
            joined = joined.join(main as Dataset, crossExt.linkedExternals[0].joinMode);
            for (let i = 1; i < linked.length; i++) {
              joined = joined.join(linked[i] as Dataset, crossExt.linkedExternals[i].joinMode);
            }
          } else {
            joined = main as Dataset;
            for (let i = 0; i < linked.length; i++) {
              joined = joined.join(linked[i] as Dataset, crossExt.linkedExternals[i].joinMode);
            }
          }
          // Re-aggregate to the user's split grain BEFORE HAVING/sort/limit.
          // The join fans main's join-key-grain rows (one per `brand`) out
          // across the linked split values, so a country with N brands shows
          // N rows each carrying the brand-level measure. Collapsing here is
          // mandatory:
          //   - Ordering vs HAVING: HAVING predicates over a measure must see
          //     the country-grain value (sum of brands), not a per-brand value.
          //   - Ordering vs sort/limit: a limit BEFORE collapse would cut
          //     pre-collapse brand rows, silently dropping whole countries.
          //   So re-agg must precede all three.
          // For sum-class measures (count/sum/avg-rewritten) this is lossless;
          // it is also the ONLY correct path for cross-engine linked sources,
          // where the native in-engine JOIN is impossible.
          if (crossExt.reAggKeys && crossExt.reAggKeys.length > 0) {
            joined = External.reAggregateToSplitGrain(
              joined,
              crossExt.reAggKeys,
              crossExt.reAggApplyTraits || {},
            );
          }
          // Replay the scalar recombination (segregate-then-recombine). The
          // leaf aggregates were just re-aggregated to the split grain; now
          // reconstruct each derived measure (e.g. `avg_price = $!T_0/$!T_1`)
          // over the collapsed rows, then drop the synthetic leaf columns so
          // the caller sees only the measures they asked for. This MUST run
          // before HAVING/sort/limit, which reference the derived measure
          // names. For all-single-aggregate queries postAggregateApplies is
          // empty and this is a no-op.
          joined = External.applyPostAggregateRecombination(joined, crossExt.postAggregateApplies);
          // Apply the post-join HAVING before sort/limit so sort ordering
          // reflects only the surviving rows, and limit caps against the
          // filtered result — not the pre-filter row count (which would
          // silently under-return rows). Plywood's predicate evaluation
          // now treats null/undefined operands as NULL (SQL semantics —
          // any comparison with NULL is NULL, falsy in HAVING), so no
          // defensive wrapping is needed: a left-join orphan row with
          // undefined linked-apply columns fails the predicate naturally
          // and gets dropped.
          if (crossExt.postJoinHavingFilter) {
            joined = joined.filter(crossExt.postJoinHavingFilter);
          }
          if (crossExt.postJoinSort) {
            joined = joined.sort(crossExt.postJoinSort.expression, crossExt.postJoinSort.direction);
          }
          if (crossExt.postJoinLimit) {
            joined = joined.limit(crossExt.postJoinLimit.value);
          }
          // Drop synthetic join-key columns the decomposition inserted so the
          // caller sees only the columns the user asked for. These columns
          // served purely as the algebraic anchor for the in-memory join and
          // carry no user-facing meaning.
          if (crossExt.syntheticJoinAliases && crossExt.syntheticJoinAliases.length > 0) {
            joined = External.dropColumns(joined, crossExt.syntheticJoinAliases);
          }
          // INV-1 transparent net. If the cross-source decomposition lost
          // its grain (linked-only split + non-decomposable measure fans
          // main rows past the user's grid), the resulting dataset has
          // more rows than distinct key tuples. Fail loud here — before
          // the bad shape leaks into the caller's response.
          External.assertDatasetShape(joined);
          return joined;
        }),
      );
    }

    // Semijoin-to-root: a TOTALS query carrying a harvested linked-only filter
    // clause (IDENTICAL logic to the simulate path above so the two never
    // diverge). Dispatch the lookup DISTINCT-joinKey sub-query first, collect
    // the joinKey set, then dispatch the TOTALS main restricted by
    // `main.<joinKey> IN (<set>)`. An empty set (a country that maps to no
    // brand) yields `IN ()` → no rows → total 0, NOT the all-country leak.
    const semijoin = this.getSemijoinToRootDecomposition();
    if (semijoin) {
      return External.valuePromiseToStream(
        External.buildValueFromStream(
          semijoin.lookupExternal.queryBasicValueStream(rawQueries),
        ).then(lookupPv => {
          const lookupDs = lookupPv as Dataset;
          const joinKeyValues = (lookupDs.data || [])
            .map(d => d[semijoin.joinKey])
            .filter(v => v !== undefined && v !== null);
          return External.buildValueFromStream(
            semijoin.buildFilteredMain(joinKeyValues).queryBasicValueStream(rawQueries),
          );
        }),
      );
    }

    const decomposed = this.getJoinDecompositionShortcut();
    if (decomposed) {
      const { waterfallFilterExpression } = decomposed;
      if (waterfallFilterExpression) {
        return External.valuePromiseToStream(
          External.buildValueFromStream(
            decomposed.external1.queryBasicValueStream(rawQueries),
          ).then(pv1 => {
            const ds1 = pv1 as Dataset;
            const ds1Filter = Expression.or(
              ds1.data.map(datum => waterfallFilterExpression.filterFromDatum(datum)),
            );

            // Add filter to second external
            const ex2Value = decomposed.external2.valueOf();
            ex2Value.filter = ex2Value.filter.and(ds1Filter);
            const filteredExternal = External.fromValue(ex2Value);

            return External.buildValueFromStream(
              filteredExternal.queryBasicValueStream(rawQueries),
            ).then(pv2 => {
              return ds1.leftJoin(pv2 as Dataset);
            });
          }),
        );
      } else {
        const plywoodValue1Promise = External.buildValueFromStream(
          decomposed.external1.queryBasicValueStream(rawQueries),
        );
        const plywoodValue2Promise = External.buildValueFromStream(
          decomposed.external2.queryBasicValueStream(rawQueries),
        );

        return External.valuePromiseToStream(
          Promise.all([plywoodValue1Promise, plywoodValue2Promise]).then(([pv1, pv2]) => {
            const ds1 = pv1 as Dataset;
            let ds2 = pv2 as Dataset;

            const { timeShift } = decomposed;
            if (timeShift && ds2.data.length) {
              const timeLabel = ds2.keys[0];
              const timeShiftDuration = timeShift.duration;
              const timeShiftTimezone = timeShift.timezone;
              ds2 = ds2.applyFn(
                timeLabel,
                (d: Datum) => {
                  const tr = d[timeLabel] as TimeRange;
                  const shiftedStart = timeShiftDuration.shift(tr.start, timeShiftTimezone, 1);
                  return new TimeRange({
                    start: shiftedStart,
                    end: shiftedStart, // We do not actually care about the end since later we compare by start only
                    bounds: '[]', // Make this range represent a single data point
                  });
                },
                'TIME_RANGE',
              );
            }

            let joined = timeShift ? ds1.leftJoin(ds2) : ds1.fullJoin(ds2);

            // Apply sort and limit
            const mySort = this.sort;
            if (mySort && !(this.sortOnLabel() && mySort.direction === 'ascending')) {
              joined = joined.sort(mySort.expression, mySort.direction);
            }

            const myLimit = this.limit;
            if (myLimit) {
              joined = joined.limit(myLimit.value);
            }

            return joined;
          }),
        );
      }
    }

    const { engine, requester } = this;

    let queryAndPostTransform: QueryAndPostTransform<any>;
    try {
      queryAndPostTransform = this.getQueryAndPostTransform();
    } catch (e) {
      return new ReadableError(e);
    }

    return External.performQueryAndPostTransform(
      queryAndPostTransform,
      requester,
      engine,
      rawQueries,
    );
  }

  public queryValueStream(
    lastNode: boolean,
    rawQueries: any[] | null,
    externalForNext: External = null,
  ): ReadableStream {
    if (!externalForNext) externalForNext = this;

    const delegate = this.getDelegate();
    if (delegate) {
      return delegate.queryValueStream(lastNode, rawQueries, externalForNext);
    }

    let finalStream = this.queryBasicValueStream(rawQueries);

    if (!lastNode && this.mode === 'split') {
      finalStream = pipeWithError(
        finalStream,
        new Transform({
          objectMode: true,
          transform: (chunk, enc, callback) => {
            if (chunk.type === 'datum') externalForNext.addNextExternalToDatum(chunk.datum);
            callback(null, chunk);
          },
        }),
      );
    }

    return finalStream;
  }

  // -------------------------

  public needsIntrospect(): boolean {
    return !this.rawAttributes.length;
  }

  protected abstract getIntrospectAttributes(depth: IntrospectionDepth): Promise<Attributes>;

  public introspect(options: IntrospectOptions = {}): Promise<External> {
    if (!this.requester) {
      return Promise.reject(new Error('must have a requester to introspect'));
    }

    if (!this.version) {
      return (this.constructor as any).getVersion(this.requester).then((version: string) => {
        version = External.extractVersion(version);
        if (!version) throw new Error('external version not found, please specify explicitly');
        return this.changeVersion(version).introspect(options);
      });
    }

    const depth = options.depth || (options.deep ? 'deep' : 'default');
    return this.getIntrospectAttributes(depth).then(attributes => {
      const value = this.valueOf();

      // Apply user provided (if any) overrides to the received attributes
      if (value.attributeOverrides) {
        attributes = AttributeInfo.override(attributes, value.attributeOverrides);
      }

      // Override any existing attributes (we do not just replace them)
      if (value.attributes) {
        attributes = AttributeInfo.override(value.attributes, attributes);
      }

      value.attributes = attributes;
      // Once attributes are set attributeOverrides will be ignored
      return External.fromValue(value);
    });
  }

  public getRawFullType(skipDerived = false): DatasetFullType {
    const { rawAttributes, derivedAttributes } = this;
    if (!rawAttributes.length) throw new Error('dataset has not been introspected');

    const myDatasetType: Record<string, FullType> = {};
    for (const rawAttribute of rawAttributes) {
      const attrName = rawAttribute.name;
      myDatasetType[attrName] = {
        type: <PlyTypeSimple>rawAttribute.type,
      };
    }

    if (!skipDerived) {
      for (const name in derivedAttributes) {
        myDatasetType[name] = {
          type: <PlyTypeSimple>derivedAttributes[name].type,
        };
      }
    }

    // Druid SQL externals declare their time column ONLY via the
    // `timeAttribute` field — it is not duplicated into `attributes`.
    // Surface it in the type context so `$time`-referencing expressions
    // (the cube's own time filter, in particular) pass `definedInTypeContext`
    // and absorb correctly into `External.filter`. Without this, a
    // `$main.filter($time.overlap(...))` on a cube with linkedSources fails
    // `_addFilterExpression`'s `expressionDefined` check, the filter is
    // never absorbed, and the main sub-query reaches Druid with no time
    // bound — which Druid rejects as "must filter on time unless the
    // allowEternity flag is set". The first declaration wins on name
    // collision (rawAttributes already populated above), so explicitly
    // listing `time` in attributes — as fixtures sometimes do — keeps the
    // explicit type and we don't override it.
    const ta = (this as any).timeAttribute;
    if (typeof ta === 'string' && ta.length > 0 && !(ta in myDatasetType)) {
      myDatasetType[ta] = { type: 'TIME' };
    }

    // Flat-expose linked source attributes at main's top level so expressions
    // like $title (a column that lives only in `reviews`) type-check against
    // this external. Main attributes win on name collisions (the column is
    // physically main's); shared columns (joinKeys) are already present from
    // rawAttributes and we don't overwrite them. getCrossExternalDecomposition
    // partitions splits/applies by source at execute time and strips linked-
    // only keys from main's SQL path.
    for (const name in this.linkedSources) {
      const ls = this.linkedSources[name];
      if (ls.attributes) {
        for (const attr of ls.attributes) {
          if (!(attr.name in myDatasetType)) {
            myDatasetType[attr.name] = { type: <PlyTypeSimple>attr.type };
          }
        }
      }
      if (ls.derivedAttributes) {
        for (const key in ls.derivedAttributes) {
          if (!(key in myDatasetType)) {
            myDatasetType[key] = { type: <PlyTypeSimple>ls.derivedAttributes[key].type };
          }
        }
      }
    }

    return {
      type: 'DATASET',
      datasetType: myDatasetType,
    };
  }

  public getFullType(): DatasetFullType {
    const { mode, attributes } = this;

    if (mode === 'value') throw new Error('not supported for value mode yet');
    let myFullType = this.getRawFullType();

    if (mode !== 'raw') {
      const splitDatasetType: Record<string, FullType> = {};
      splitDatasetType[this.dataName || External.SEGMENT_NAME] = myFullType;

      for (const attribute of attributes) {
        const attrName = attribute.name;
        splitDatasetType[attrName] = {
          type: <PlyTypeSimple>attribute.type,
        };
      }

      myFullType = {
        type: 'DATASET',
        datasetType: splitDatasetType,
      };
    }

    // See the note in getRawFullType: linkedSources are NOT registered here
    // either. Resolution is via peer externals at the enclosing datum scope.

    return myFullType;
  }

  public getTimeAttribute(): string | undefined {
    return undefined;
  }

  public isTimeRef(ex: Expression): ex is RefExpression {
    return ex instanceof RefExpression && ex.name === this.getTimeAttribute();
  }

  private groupAppliesByTimeFilterValue():
    | {
        filterValue: Set | TimeRange;
        timeRef: Expression;
        unfilteredApplies: ApplyExpression[];
        hasSort: boolean;
      }[]
    | null {
    const { applies, sort } = this;
    const groups: {
      filterValue: Set | TimeRange;
      timeRef: Expression;
      unfilteredApplies: ApplyExpression[];
      hasSort: boolean;
    }[] = [];
    const constantApplies: ApplyExpression[] = [];

    for (const apply of applies) {
      if (apply.expression instanceof LiteralExpression) {
        constantApplies.push(apply);
        continue;
      }

      let applyFilterValue: Set | TimeRange = null;
      let timeRef: Expression = null;
      let badCondition = false;
      const newApply = apply.changeExpression(
        apply.expression
          .substitute(ex => {
            if (
              ex instanceof OverlapExpression &&
              this.isTimeRef(ex.operand) &&
              ex.expression.getLiteralValue()
            ) {
              const myValue = ex.expression.getLiteralValue();
              if (applyFilterValue && !(applyFilterValue as any).equals(myValue)) {
                badCondition = true;
              }
              applyFilterValue = myValue;
              timeRef = ex.operand;
              return Expression.TRUE;
            }
            return null;
          })
          .simplify(),
      );

      if (badCondition || !applyFilterValue) return null;

      const myGroup = groups.find(r => (applyFilterValue as any).equals(r.filterValue));
      const mySort = Boolean(
        sort && sort.expression instanceof RefExpression && newApply.name === sort.expression.name,
      );
      if (myGroup) {
        myGroup.unfilteredApplies.push(newApply);
        if (mySort) myGroup.hasSort = true;
      } else {
        groups.push({
          filterValue: applyFilterValue,
          timeRef,
          unfilteredApplies: [newApply],
          hasSort: mySort,
        });
      }
    }

    if (groups.length && constantApplies.length) {
      groups[0].unfilteredApplies.push(...constantApplies);
    }

    return groups;
  }

  public getJoinDecompositionShortcut(): {
    external1: External;
    external2: External;
    timeShift?: TimeShiftExpression;
    waterfallFilterExpression?: SplitExpression;
  } | null {
    if (this.mode !== 'split') return null;

    // Applies must decompose into 2 things
    const appliesByTimeFilterValue = this.groupAppliesByTimeFilterValue();
    if (!appliesByTimeFilterValue || appliesByTimeFilterValue.length !== 2) return null;

    // Those two things need to be TimeRanges
    const filterV0 = appliesByTimeFilterValue[0].filterValue;
    const filterV1 = appliesByTimeFilterValue[1].filterValue;
    if (!(filterV0 instanceof TimeRange && filterV1 instanceof TimeRange)) return null;

    // Make sure that the first value of appliesByTimeFilterValue is now
    if (filterV0.start < filterV1.start) appliesByTimeFilterValue.reverse();

    // Find the time split (must be only one)
    const timeSplitNames = this.split
      .mapSplits((name, ex) => (ex instanceof TimeBucketExpression ? name : undefined))
      .filter(Boolean);

    // Check for timeseries/groupBy decomposition
    if (timeSplitNames.length === 1) {
      const timeSplitName = timeSplitNames[0];
      const timeSplitExpression = this.split.splits[timeSplitName] as TimeBucketExpression;

      if (timeSplitExpression instanceof TimeBucketExpression) {
        const hybridTimeDecomposition = this.getHybridTimeExpressionDecomposition(
          timeSplitExpression.operand,
        );

        if (hybridTimeDecomposition) {
          const { timeRef, timeShift } = hybridTimeDecomposition;

          const simpleSplit = this.split.addSplits({
            [timeSplitName]: timeSplitExpression.changeOperand(timeRef),
          });

          const external1Value = this.valueOf();
          external1Value.filter = timeRef
            .overlap(appliesByTimeFilterValue[0].filterValue)
            .and(external1Value.filter)
            .simplify();
          external1Value.split = simpleSplit;
          external1Value.applies = appliesByTimeFilterValue[0].unfilteredApplies;
          external1Value.limit = null; // Remove limit and sort
          external1Value.sort = null; // So we get a timeseries

          const external2Value = this.valueOf();
          external2Value.filter = timeRef
            .overlap(appliesByTimeFilterValue[1].filterValue)
            .and(external2Value.filter)
            .simplify();
          external2Value.split = simpleSplit;
          external2Value.applies = appliesByTimeFilterValue[1].unfilteredApplies;
          external2Value.limit = null;
          external2Value.sort = null;

          return {
            external1: External.fromValue(external1Value),
            external2: External.fromValue(external2Value),
            timeShift: timeShift.changeOperand(Expression._),
          };
        }
      }
    }

    // Check for topN decomposition
    if (
      this.split.numSplits() === 1 &&
      appliesByTimeFilterValue[0].hasSort &&
      this.limit &&
      this.limit.value <= 1000
    ) {
      const external1Value = this.valueOf();
      external1Value.filter = appliesByTimeFilterValue[0].timeRef
        .overlap(appliesByTimeFilterValue[0].filterValue)
        .and(external1Value.filter)
        .simplify();
      external1Value.applies = appliesByTimeFilterValue[0].unfilteredApplies;

      const external2Value = this.valueOf();
      external2Value.filter = appliesByTimeFilterValue[0].timeRef
        .overlap(appliesByTimeFilterValue[1].filterValue)
        .and(external2Value.filter)
        .simplify();
      external2Value.applies = appliesByTimeFilterValue[1].unfilteredApplies;
      external2Value.sort = external2Value.sort.changeExpression($(external2Value.applies[0].name));

      // ToDo: strictly speaking this is incorrect. This only works under the assumption that the havingFilter can be fully resolved using external1
      // the correct thing to do would be to decompose the havingFilter into `havingOnExternal1 AND havingOnExternal2` and then to assign them accordingly.
      delete external2Value.havingFilter;

      return {
        external1: External.fromValue(external1Value),
        external2: External.fromValue(external2Value),
        waterfallFilterExpression: external1Value.split,
      };
    }

    return null;
  }

  /**
   * Cross-external decomposition: when a main-rooted split carries applies
   * that reference foreign Externals (e.g. $reviews.average($rating) on a
   * $main split), partition the applies into per-external sub-plans that
   * can each be answered by a single datasource query. The caller joins
   * the results on the shared split keys.
   *
   * Returns `null` whenever the query is either single-external or is a
   * shape we can't decompose safely — the outer pipeline then falls through
   * to the normal single-query path (which may itself fail at SQL time;
   * that's expected, the pre-existing behavior, and easier to debug).
   *
   * Mirrors the layout of getJoinDecompositionShortcut above.
   */

  /**
   * Phase 4 native-JOIN sibling — emits a single Druid SQL with an
   * INNER/LEFT JOIN against the lookup. Routed to when the
   * decomposability gate refuses the JS-join path (countDistinct,
   * quantile, average, mode, min/max over a linked-only split).
   *
   * Phase 3 contract: returns null when the multi-alias case or
   * other unsupported shape is detected; Phase 4 fills in the
   * single-alias case.
   *
   * @param linkedOnlySplitAliases  the user-side split aliases whose
   *                                 free refs resolve only in some
   *                                 linkedSource's attributes
   * @param mainApplies              the value-applies to project
   * @param involvedLinkedNames      lookup-source names participating
   */
  public getNativeJoinDecomposition(
    linkedOnlySplitAliases: string[],
    mainApplies: ApplyExpression[],
    involvedLinkedNames: Record<string, true>,
    mainSideSplitAliases: string[] = [],
  ): {
    kind: 'nativeJoin';
    nativeJoin: {
      sql: string;
      joinMode: 'inner' | 'left';
      splitAlias: string;
      mainSource: string;
      linkedSource: string;
      keys: string[];
      applyNames: string[];
      // The split-key attributes carried by the combined SQL, in projection
      // order: the main-side splits (e.g. a `$__time.timeBucket(P1D)` day
      // bucket) followed by the single linked-only split key. The execution
      // layer builds the result Dataset's attributes from these (so a TIME
      // bucket inflates to a TimeRange, not a bare STRING) and keys the
      // dataset on all of them — without this a double split [Time × country]
      // would lose the time dimension. `splitExpr` is the original split
      // expression, used to pick the intelligent inflater.
      splitKeyAttributes: { name: string; type: PlyType; splitExpr: Expression }[];
    };
    mainExternal: External;
    linkedExternals: {
      name: string;
      external: External;
      joinKeys: string[];
      joinMode: 'inner' | 'left';
    }[];
    syntheticJoinAliases?: string[];
    // HAVING applied POST-JOIN against the returned Dataset (mirror of the
    // jsJoin branch). The native-JOIN applies never pass through
    // `External.addExpression`, so `$uniq` is not absorbed as a column and an
    // in-SQL `HAVING COUNT(DISTINCT …)` would reference an unresolvable alias.
    // The executor keys the Dataset by `applyNames`, so `Dataset.filter($uniq >
    // N)` runs by alias — identical to the jsJoin path. Undefined when the
    // outer External carries no havingFilter.
    postJoinHavingFilter?: Expression;
    // When a postJoinHavingFilter is present the combined SQL's inline ORDER BY
    // / LIMIT are STRIPPED (else the engine cuts to LIMIT before the post-join
    // filter runs and surviving rows starve) and surfaced here; the executor
    // applies them AFTER the having filter, in order filter → sort → limit.
    postJoinSort?: SortExpression;
    postJoinLimit?: LimitExpression;
  } {
    // Phase 4: support single linked-only split + single linkedSource.
    // Multi-alias and multi-source native-JOIN are post-MVP.
    // F4 (cycle-2): every unsupported shape throws
    // `PlywoodUnsupportedNativeJoinShape` with a site-specific reason —
    // returning null silently let the caller fall back to a single-
    // source path that died late with "could not get attribute info
    // for X" from the Druid inflater.
    if (linkedOnlySplitAliases.length !== 1) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `multi-alias linked-only split unsupported (aliases=${linkedOnlySplitAliases.join(
          ',',
        )}, count=${linkedOnlySplitAliases.length})`,
      );
    }
    const lsNames = Object.keys(involvedLinkedNames);
    if (lsNames.length !== 1) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `multiple linkedSources involved [${lsNames.join(
          ',',
        )}] — native-JOIN v1 supports exactly 1`,
      );
    }
    const splitAlias = linkedOnlySplitAliases[0];
    const lsName = lsNames[0];
    const config = this.linkedSources[lsName];
    if (!config) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `linkedSource "${lsName}" missing in linkedSources map`,
      );
    }
    const joinMode = External.resolveLinkedJoinMode(config);
    if (!joinMode) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `linkedSource "${lsName}" missing joinMode (resolveLinkedJoinMode undefined)`,
      );
    }
    const joinKeys = config.joinKeys || [];
    if (joinKeys.length === 0) {
      throw new PlywoodUnsupportedNativeJoinShape(`linkedSource "${lsName}" has empty joinKeys`);
    }

    // A native JOIN emits ONE SQL statement that references both the main
    // source and the linked source — it can only do so when both live in
    // the SAME engine. If the linkedSource declares a cross-engine override
    // (e.g. a Postgres staging view joined against a main Druid datasource),
    // a single SQL JOIN is impossible: the two sides must each run their own
    // query and be joined in JS. Refuse loudly here rather than emit SQL that
    // references a table the engine cannot see. The JS-join path
    // (getCrossExternalDecomposition) handles the cross-engine case.
    if (config.engine && config.engine !== this.engine) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `linkedSource "${lsName}" declares cross-engine override engine="${config.engine}" ` +
          `(main engine="${this.engine}"); a single native SQL JOIN cannot span two engines. ` +
          `This shape must decompose to per-side queries joined in JS — do not route it through ` +
          `getNativeJoinDecomposition.`,
      );
    }

    // Build the SQL. Pattern (single linkedSource, single linked-only
    // split alias, single joinKey for v1):
    //
    //   SELECT lookup.<linked_attr> AS "<splitAlias>",
    //          <main_measure_sql> AS "<measureName>"
    //   FROM <main_source> main
    //   <INNER|LEFT> JOIN <linked_source> lookup
    //     ON main.<joinKey> = lookup.<joinKey>
    //   WHERE <main filter SQL>
    //   GROUP BY 1
    //
    // The split alias on the lookup side comes from the user's split
    // expression — typically a RefExpression to a linked column.
    const dialect = (this as any).dialect as SQLDialect;
    if (!dialect) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `native Druid engine has no SQLDialect; native-JOIN requires SQL transport (engine="${this.engine}")`,
      );
    }
    const splitExpr = this.split.splits[splitAlias];
    if (!splitExpr) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `split alias "${splitAlias}" not found in this.split.splits`,
      );
    }
    // For v1 we only support the simplest shape: split is a bare
    // RefExpression to a column that exists on the linked side. More
    // complex transforms (TIME_FLOOR, etc.) are out of scope.
    const splitRef = splitExpr instanceof RefExpression ? splitExpr : null;
    if (!splitRef) {
      throw new PlywoodUnsupportedNativeJoinShape(
        `split expr ${splitExpr.toString()} is not a bare RefExpression (TIME_FLOOR/SUBSTR/etc unsupported)`,
      );
    }
    const linkedColName = splitRef.name;

    const mainSource = String(this.source);
    const linkedSource = String(config.source);
    const mainAlias = 'main';
    const lookupAlias = 'lookup';
    const escName = (n: string) => dialect.escapeName(n);

    // ON clause: AND of every declared joinKey
    const onConds = joinKeys
      .map(k => `${mainAlias}.${escName(k)} = ${lookupAlias}.${escName(k)}`)
      .join(' AND ');

    // SELECT clause: the main-side split keys (e.g. a `$__time.timeBucket(P1D)`
    // day bucket — fix B classifies the main timeAttribute main-side against an
    // eternal lookup) rendered from `main`, then the linked-only split column
    // from `lookup`, then each main apply as its aggregate SQL. Projecting the
    // main-side splits FIRST gives them the leading GROUP BY positions; the
    // linked key follows. Both must reach the GROUP BY — emitting `GROUP BY 1`
    // (linked key only) silently DROPS every main-side split (the [Time ×
    // country] double-split bug: the time dimension vanished and countDistinct
    // collapsed to an all-period count).
    const selectParts: string[] = [];
    const groupByPositions: number[] = [];
    // splitKeyAttributes travels back to the execution layer so it can build
    // the result Dataset's attributes/keys/inflaters covering ALL split keys.
    const splitKeyAttributes: { name: string; type: PlyType; splitExpr: Expression }[] = [];
    // Order matters: main-side splits occupy positions 1..M, the linked key
    // M+1. Collect them in this order so positions line up.
    const orderedSplitKeyAliases: string[] = [];
    for (const msAlias of mainSideSplitAliases) {
      const msExpr = this.split.splits[msAlias];
      if (!msExpr) {
        throw new PlywoodUnsupportedNativeJoinShape(
          `main-side split alias "${msAlias}" not found in this.split.splits`,
        );
      }
      let msSQL: string;
      const prevTable = (dialect as any).table;
      (dialect as any).setTable(mainAlias);
      try {
        msSQL = msExpr.getSQL(dialect);
      } finally {
        (dialect as any).setTable(prevTable);
      }
      selectParts.push(`${msSQL} AS ${escName(msAlias)}`);
      groupByPositions.push(selectParts.length);
      splitKeyAttributes.push({
        name: msAlias,
        type: Set.unwrapSetType(msExpr.type),
        splitExpr: msExpr,
      });
      orderedSplitKeyAliases.push(msAlias);
    }
    // The linked-only split column from the lookup side.
    selectParts.push(`${lookupAlias}.${escName(linkedColName)} AS ${escName(splitAlias)}`);
    groupByPositions.push(selectParts.length);
    splitKeyAttributes.push({
      name: splitAlias,
      type: Set.unwrapSetType(splitExpr.type),
      splitExpr,
    });
    orderedSplitKeyAliases.push(splitAlias);
    const applyNames: string[] = [];
    for (const apply of mainApplies) {
      if (apply.expression.type === 'DATASET') continue; // skip scope-registrations
      // The apply's tree still carries unresolved `$main` dataset refs
      // (they would be absorbed via External.addExpression on the
      // single-source path). For native-JOIN we don't go through
      // addExpression, so calling `.getSQL` on the apply directly
      // tries to render the literal dataset and throws "unsupported
      // type: DATASET". Render the aggregate manually: walk the
      // apply's expression, find the Aggregate node, call its
      // helper with a stub operandSQL (the only thing operandSQL
      // contributes to is the aggregate-filter detection — we have
      // no per-apply filter here, so any non-WHERE string works).
      const aggExpr = apply.expression;
      const prevTable = (dialect as any).table;
      (dialect as any).setTable(mainAlias);
      try {
        // renderAggregateSQL throws (PlywoodUnsupportedNativeJoinShape) on any
        // unrenderable op — it never returns a silent null that would drop the
        // SELECT item while the ORDER BY still references it. So every value
        // apply is guaranteed to contribute a SELECT column here.
        const aggSQL = renderAggregateSQL(aggExpr, dialect, mainAlias);
        selectParts.push(`${aggSQL} AS ${escName(apply.name)}`);
        applyNames.push(apply.name);
      } finally {
        (dialect as any).setTable(prevTable);
      }
    }

    // WHERE: main's getQueryFilter SQL, qualified to main alias, AND any
    // harvested LINKED-ONLY filter clause qualified to the LOOKUP alias.
    //
    // The main-side half (`this.getQueryFilter()`) carries only the clauses over
    // columns main owns (e.g. the __time bound). A filter over a column that
    // lives ONLY on the lookup (`brand_country = 'Francia'`) never survives onto
    // `this.filter`: the front stamps it on the magic linked-source apply, which
    // `.simplify()` deletes as dead code (see pruneLinkedFilterRefsInTree). The
    // v2 fix harvests that clause onto a PER-REQUEST copy of this External and
    // parks it at `this.linkedSources[lsName].filter` (== `config.filter`), the
    // SAME slot the JS-join leaf path reads as `templateFilter`
    // (getCrossExternalDecomposition). The native-JOIN path must read it too:
    // rendered against the `lookup` alias it becomes
    // `lookup."brand_country" = 'Francia'`, so the INNER JOIN keeps only the main
    // rows whose brand maps to Francia — the split shows only Francia and the
    // non-decomposable aggregate (countDistinct) is computed over Francia rows
    // only. Without this the clause was silently dropped: every country returned
    // identical to the no-filter query (Ismael's reported bug). Inert when no
    // clause was harvested (`config.filter` absent/TRUE) → a no-linked-filter
    // panel emits exactly the same SQL as before (orthogonality preserved).
    const filter = this.getQueryFilter();
    const whereConds: string[] = [];
    if (!filter.equals(Expression.TRUE)) {
      const prevTable = (dialect as any).table;
      (dialect as any).setTable(mainAlias);
      try {
        whereConds.push(filter.getSQL(dialect));
      } finally {
        (dialect as any).setTable(prevTable);
      }
    }
    const stashedLinkedFilter: Expression | undefined = (config as any).filter;
    if (stashedLinkedFilter && !stashedLinkedFilter.equals(Expression.TRUE)) {
      const prevTable = (dialect as any).table;
      (dialect as any).setTable(lookupAlias);
      try {
        whereConds.push(stashedLinkedFilter.getSQL(dialect));
      } finally {
        (dialect as any).setTable(prevTable);
      }
    }
    const whereSQL = whereConds.length ? 'WHERE ' + whereConds.join(' AND ') : '';

    const joinSql = joinMode === 'inner' ? 'INNER JOIN' : 'LEFT JOIN';
    const sqlParts = [
      `SELECT ${selectParts.join(', ')}`,
      `FROM ${escName(mainSource)} AS ${mainAlias}`,
      `${joinSql} ${escName(linkedSource)} AS ${lookupAlias} ON ${onConds}`,
    ];
    if (whereSQL) sqlParts.push(whereSQL);
    // GROUP BY covers EVERY split-key position (main-side buckets + linked
    // key), not just `1`. A single linked-only split still emits `GROUP BY 1`;
    // a [Time(Day) × country] double split emits `GROUP BY 1, 2`.
    sqlParts.push(`GROUP BY ${groupByPositions.join(', ')}`);

    // HAVING routing (mirror of the jsJoin branch, ~5006-5028). A `.filter()`
    // on the aggregate value folded into `this.havingFilter`. The native-JOIN
    // applies do NOT pass through `External.addExpression`, so `$uniq` is never
    // absorbed as a column and an in-SQL `HAVING COUNT(DISTINCT …) > N` would
    // reference an unresolvable alias. We therefore apply the HAVING POST-JOIN:
    // the executor keys the result Dataset by `applyNames`, so
    // `Dataset.filter($uniq > N)` runs by alias — exactly like the jsJoin path.
    //
    // Scope split: the projectable names are the rendered measure aliases plus
    // every split key. A clause over a projectable name → postJoinHavingFilter.
    // A clause over anything NOT projected can't be evaluated post-join either,
    // so it fails loud rather than being silently dropped (the original bug).
    let postJoinHavingFilter: Expression | undefined;
    let postJoinSort: SortExpression | undefined;
    let postJoinLimit: LimitExpression | undefined;
    if (this.havingFilter && !this.havingFilter.equals(Expression.TRUE)) {
      const projectableNames: Record<string, true> = {};
      for (const n of applyNames) projectableNames[n] = true;
      for (const k of orderedSplitKeyAliases) projectableNames[k] = true;
      // `splitFilterByScope(filter, names)` returns clauses over `names` as its
      // `main` half and clauses over names OUTSIDE the set as its `post` half.
      // Here `names` = the projectable post-join names, so its `main` half is
      // exactly what we can evaluate post-join and its `post` half is the
      // UNPROJECTED remainder we must reject loudly.
      const { main: havingProjectable, post: havingUnprojected } = External.splitFilterByScope(
        this.havingFilter,
        projectableNames,
      );
      if (!havingUnprojected.equals(Expression.TRUE)) {
        const offending = havingUnprojected
          .getFreeReferences()
          .filter(r => !projectableNames[r])
          .join(', ');
        throw new PlywoodUnsupportedNativeJoinShape(
          `native-JOIN HAVING references non-projected name(s) [${offending}] — ` +
            `only the rendered measure aliases [${applyNames.join(', ')}] and split keys ` +
            `[${orderedSplitKeyAliases.join(', ')}] are resolvable post-join`,
        );
      }
      if (!havingProjectable.equals(Expression.TRUE)) {
        postJoinHavingFilter = havingProjectable;
      }
    }

    // Sort/limit routing. When a HAVING moved post-join the inline ORDER BY /
    // LIMIT MUST be STRIPPED from the SQL: if the engine cut to LIMIT before the
    // post-join filter ran, surviving rows would starve (mirror of jsJoin
    // ~5019-5028). They are surfaced as postJoinSort / postJoinLimit and the
    // executor applies them AFTER the filter, in order filter → sort → limit.
    // Without a HAVING the inline sort/limit stay put (Druid topN) — orthogonal.
    if (postJoinHavingFilter) {
      if (this.sort) postJoinSort = this.sort;
      if (this.limit) postJoinLimit = this.limit;
    } else {
      if (this.sort) {
        sqlParts.push(this.sort.getSQL(dialect));
      }
      if (this.limit) {
        sqlParts.push(this.limit.getSQL(dialect));
      }
    }

    const sql = sqlParts.join('\n');

    return {
      kind: 'nativeJoin',
      nativeJoin: {
        sql,
        joinMode,
        splitAlias,
        mainSource,
        linkedSource,
        // ALL split keys (main-side buckets + the linked key), in projection
        // order — so the result Dataset is keyed on every requested dimension,
        // not just the linked one.
        keys: orderedSplitKeyAliases.slice(),
        applyNames,
        splitKeyAttributes,
      },
      // The caller's main-side and linked-side externals are unused
      // by the nativeJoin execution path — the combined SQL replaces
      // them — but the return shape needs the fields for type parity.
      // We attach a stub mainExternal so .equalBase() etc. don't trip.
      mainExternal: this,
      linkedExternals: [],
      syntheticJoinAliases: [],
      postJoinHavingFilter,
      postJoinSort,
      postJoinLimit,
    };
  }

  public getCrossExternalDecomposition(): {
    // Routing discriminator (INV-2). Two paths emit from this function:
    //   - 'jsJoin'     — historic path: pre-aggregate main + linked,
    //                    in-memory join. Safe only when every main-side
    //                    measure declares `decomposable: 'sum'`. Default
    //                    when omitted (back-compat with callers reading
    //                    the legacy shape).
    //   - 'nativeJoin' — single SQL with a Druid INNER/LEFT JOIN against
    //                    the lookup datasource. Mandatory when a main-
    //                    side measure is non-decomposable (countDistinct,
    //                    quantile, average) AND the user split includes
    //                    a linked-only alias (would fan main rows past
    //                    the grid grain). See `nativeJoin` for the
    //                    emit() shape filled by Phase 4.
    kind?: 'jsJoin' | 'nativeJoin';
    // Populated when kind === 'nativeJoin'. The combined SQL the
    // execution layer dispatches as a single query; the joinMode,
    // splitAlias, source identifiers, key list, and apply names
    // travel with it for downstream shape attribution.
    nativeJoin?: {
      sql: string;
      joinMode: 'inner' | 'left';
      splitAlias: string;
      mainSource: string;
      linkedSource: string;
      keys: string[];
      applyNames: string[];
      splitKeyAttributes?: { name: string; type: PlyType; splitExpr: Expression }[];
    };
    mainExternal: External;
    linkedExternals: {
      name: string;
      external: External;
      joinKeys: string[];
      joinMode: 'inner' | 'left';
    }[];
    postJoinSort?: SortExpression;
    postJoinLimit?: LimitExpression;
    // Aliases the decomposition itself inserted into both sides' splits so
    // the join has something to align on (declared joinKeys from the linked
    // source's config). These are implementation detail — the execution
    // layer drops them from the final dataset so the caller sees only the
    // columns they asked for.
    syntheticJoinAliases?: string[];
    // HAVING clauses that reference apply names or split aliases routed to
    // a linked sub-External. They can't be pushed into the main side's
    // Druid SQL (the column doesn't exist there) so the execution layer
    // applies them against the joined Dataset after the in-memory join,
    // before postJoinSort / postJoinLimit run.
    postJoinHavingFilter?: Expression;
    // Post-join re-aggregation grain (INV-1). The user's split keys minus
    // the synthetic join aliases — the grain the JS-join result must collapse
    // to after the in-memory join fans main's join-key-grain rows out across
    // the linked split values. Empty/undefined when there is no linked-only
    // split (no fan-out possible).
    reAggKeys?: string[];
    // Per-LEAF decomposability trait, keyed by leaf-aggregate column name.
    // Drives the post-join re-aggregation reducer (sum/min/max). 'none' here is
    // a gate bug (those leaves must route to native-JOIN) and fails loud at
    // re-agg.
    reAggApplyTraits?: Record<string, 'sum' | 'min' | 'max' | 'none'>;
    // Scalar recombination applies (Ogievetsky segregate-then-recombine). The
    // main sub-query projects the homomorphic leaf aggregates; after the
    // post-join re-aggregation collapses the leaves to the user's split grain,
    // the execution layer replays these applies (e.g. `avg_price = $!T_0 /
    // $!T_1`) to reconstruct the derived measures, then drops the leaf columns.
    // Empty for queries whose measures are all single aggregates.
    postAggregateApplies?: ApplyExpression[];
  } | null {
    if (this.mode !== 'split') return null;
    if (!this.applies || this.applies.length === 0) return null;
    if (!this.linkedSources || Object.keys(this.linkedSources).length === 0) return null;

    // Pre-gate avg-rewrite (F2). `average($x)` is mathematically
    // `sum($x) / count()` — the identity rewrite `decomposeAverage`
    // (baseExpression.ts:1805) already exists and is invariant-
    // preserving. Apply it locally to every contributing apply BEFORE
    // the decomposability gate evaluates traits: post-rewrite, every
    // avg becomes a sum/count pair (both trait='sum'), the JS-join
    // path stays safe, and the gate doesn't force the heavier
    // native-JOIN route for an aggregate that decomposes cleanly.
    //
    // Local rewrite only: we mutate a const `rewrittenApplies` and
    // route the rest of this method through it. `this.applies` stays
    // untouched, so the rewrite does NOT leak to non-cross-source
    // paths (a viz with avg + no linked-only split still emits AVG()
    // unchanged — see avgRewriteIsolation pin).
    const rewrittenApplies: ApplyExpression[] = this.applies.map(a =>
      a.changeExpression(a.expression.decomposeAverage()),
    );

    // Index linkedSources by source-string so we can match foreign ExternalExpressions back
    // to their declared linked-source name. Source is the stable identity.
    const linkedByMaterializedSource = new Map<string, { name: string; config: any }>();
    for (const name in this.linkedSources) {
      linkedByMaterializedSource.set(String(this.linkedSources[name].source), {
        name,
        config: this.linkedSources[name],
      });
    }

    // For each apply, collect the set of foreign linked-source names it touches.
    // A foreign reference is any ExternalExpression whose external's source does
    // not match ours. Zero foreign refs → main-side apply. One foreign ref →
    // that linked side. Two+ → this apply mixes sources, we can't decompose.
    const mainApplies: ApplyExpression[] = [];
    const linkedAppliesByName: Record<string, ApplyExpression[]> = {};
    // Capture the first foreign ExternalExpression instance we see per linked
    // name — we'll reuse its (already time-filtered, schema-complete) external
    // as the base for the sub-query. This avoids having to synthesize a fresh
    // linked External from scratch here.
    const foreignTemplateByName: Record<string, External> = {};

    for (const apply of rewrittenApplies) {
      // Dataset applies (e.g. .apply('reviews', $reviews.filter(F))) are
      // scope registrations — they declare that a source is reachable at
      // this ply level, not that it contributes a value to the output. We
      // treat them as main-side so they're kept intact and don't trigger
      // decomposition on their own. Only value-returning aggregates pull
      // the query into cross-source territory.
      if (apply.expression.type === 'DATASET') {
        mainApplies.push(apply);
        continue;
      }

      const foreignNames: Record<string, true> = {};
      apply.expression.forEach(sub => {
        if (!(sub instanceof ExternalExpression)) return;
        if (this.equalBase(sub.external)) return;
        const hit = linkedByMaterializedSource.get(String(sub.external.source));
        if (!hit) return; // a foreign external that isn't a declared linkedSource — leave it
        foreignNames[hit.name] = true;
        if (!foreignTemplateByName[hit.name]) foreignTemplateByName[hit.name] = sub.external;
      });

      const foreignKeys = Object.keys(foreignNames);
      if (foreignKeys.length === 0) {
        mainApplies.push(apply);
      } else if (foreignKeys.length === 1) {
        (linkedAppliesByName[foreignKeys[0]] ||= []).push(apply);
      } else {
        return null;
      }
    }

    // Split the main-side applies into scope-registration (DATASET) applies —
    // carried verbatim — and value applies, which may be segregated into
    // homomorphic leaf aggregates below (only when a linked-only split makes
    // the join fan main rows out; see `mainLeafApplies` after the gate).
    const mainDatasetApplies = mainApplies.filter(a => a.expression.type === 'DATASET');
    const mainValueApplies = mainApplies.filter(a => a.expression.type !== 'DATASET');

    // A cross-source query is triggered by either of two signals:
    //
    //   1. A VALUE apply whose expression aggregates from a declared linked
    //      source (a semantic output contribution).
    //   2. A split alias whose free refs resolve only in a linked source's
    //      attributes (the user grouped by a column that main can't see).
    //
    // Signal (2) was removed at one point because, without auto-inject, it
    // produced a misleading "no shared joinKey" error from our own layer.
    // Now that sharedAliases.length === 0 is handled by synthesizing the
    // joinKeys, the schema-driven trigger is safe again — and necessary:
    // without it, a query like `.split($content)` where `content` only
    // lives in reviews would be handed to the main side and rejected by
    // Druid with "column not found", even though the query has a
    // well-defined cross-source meaning.
    const involvedLinkedNames: Record<string, true> = Object.keys(linkedAppliesByName).reduce<
      Record<string, true>
    >((acc, n) => ((acc[n] = true), acc), {});
    for (const alias of this.split.keys) {
      const ex = this.split.splits[alias];
      const refs = ex.getFreeReferences();
      if (refs.length === 0) continue;
      // Main-resolvable names: rawAttributes ∪ derivedAttributes ∪
      // timeAttribute. Without timeAttribute here, a split on the time
      // column would be classified as "not in main" and routed to a
      // linkedSource — falsely triggering cross-source decomposition or
      // throwing "dangling alias" depending on what the linked side
      // declares.
      const mainAttrs: Record<string, true> = {};
      for (const a of this.rawAttributes || []) mainAttrs[a.name] = true;
      for (const k in this.derivedAttributes) mainAttrs[k] = true;
      const ta = (this as any).timeAttribute;
      if (typeof ta === 'string' && ta.length > 0) mainAttrs[ta] = true;
      const anyInMain = refs.some(r => mainAttrs[r]);
      for (const lsName in this.linkedSources) {
        const ls = this.linkedSources[lsName];
        const linkedAttrs: Record<string, true> = {};
        if (ls.attributes) for (const a of ls.attributes as any[]) linkedAttrs[a.name] = true;
        if (ls.derivedAttributes) for (const k in ls.derivedAttributes) linkedAttrs[k] = true;
        const allInLinked = refs.every(r => linkedAttrs[r]);
        if (allInLinked && !anyInMain) involvedLinkedNames[lsName] = true;
      }
    }
    if (Object.keys(involvedLinkedNames).length === 0) return null;

    // INV-2 decomposability gate.
    //
    // The JS-join path pre-aggregates main per join-key tuple, joins
    // against the linked rows in memory, then projects the result.
    // That's only safe when every main-side measure is a 'sum'-trait
    // aggregate: a sum of partition partials equals the global sum.
    // For countDistinct, average, quantile, mode (trait 'none'), and
    // for min/max (trait 'min'/'max' — pending a type-aware post-join
    // reducer, R-4), the pre-aggregate-then-join produces wrong
    // numbers even when the join cardinality is right.
    //
    // Pre-check: only fire the gate when there's at least one
    // linked-only split alias. With shared-only or main-only splits
    // the fan-out doesn't happen (main's pre-aggregate already
    // matches the grid grain), so JS-join stays valid even for
    // non-decomposable measures.
    //
    // Linked-only signal: any alias whose free refs all resolve in
    // some linkedSource's attributes AND none resolve in main. This
    // is the same test the per-source loop applies; we precompute
    // it here so the gate decision precedes the loop.
    let gateHasLinkedOnlySplit = false;
    {
      const mainAttrsForGate: Record<string, true> = {};
      for (const a of this.rawAttributes || []) mainAttrsForGate[a.name] = true;
      for (const k in this.derivedAttributes) mainAttrsForGate[k] = true;
      const taForGate = (this as any).timeAttribute;
      if (typeof taForGate === 'string' && taForGate.length > 0) {
        mainAttrsForGate[taForGate] = true;
      }
      for (const alias of this.split.keys) {
        const exAlias = this.split.splits[alias];
        const refs = exAlias.getFreeReferences();
        if (refs.length === 0) continue;
        const anyInMain = refs.some(r => mainAttrsForGate[r]);
        if (anyInMain) continue;
        for (const lsName in this.linkedSources) {
          const ls = this.linkedSources[lsName];
          const linkedAttrs: Record<string, true> = {};
          if (ls.attributes) for (const a of ls.attributes as any[]) linkedAttrs[a.name] = true;
          if (ls.derivedAttributes) for (const k in ls.derivedAttributes) linkedAttrs[k] = true;
          if (refs.every(r => linkedAttrs[r])) {
            gateHasLinkedOnlySplit = true;
            break;
          }
        }
        if (gateHasLinkedOnlySplit) break;
      }
    }

    // Algebraic decomposition of main-side measures into homomorphic leaf
    // aggregates + a scalar recombination (Ogievetsky's segregate-then-
    // recombine, the same machinery `segregationAggregateApplies` already uses
    // for nested-aggregate applies).
    //
    // ONLY when there is a linked-only split. That is the sole shape where the
    // join fans main's join-key-grain rows out across the linked split, so the
    // grid grain (e.g. brand_country) is coarser than main's pre-aggregate
    // grain (brand). A measure like `average($x)` = `sum($x)/count()` is then a
    // ratio that cannot be recombined across the fan-out (media-de-medias):
    // summing per-brand averages is not the per-country average. We must carry
    // the homomorphic LEAVES (`sum($x)`, `count()`) as separate columns —
    // each re-aggregatable by its own trait — and replay the deriving scalar
    // function (`divide`) AFTER re-aggregation at the split grain.
    //
    // When the split is shared/main-only there is NO fan-out: main already
    // pre-aggregates at the grid grain, so avg/min/max are correct as native
    // single columns and HAVING/sort run on Druid. In that case we keep the
    // value applies WHOLE (no leaves, no post-agg) — the fix is inert outside
    // the broken shape, which keeps it orthogonal and minimal.
    //
    // `segregationAggregateApplies`: `aggregateApplies` are the leaves (deduped
    // — a shared `count()` is emitted once), `postAggregateApplies` are the
    // originals with each leaf aggregate replaced by a ref to its leaf column.
    // min/max leaves recombine trivially (min of mins). countDistinct/quantile
    // leaves resolve to 'none' and divert to native-JOIN below.
    const mainSeg = gateHasLinkedOnlySplit
      ? External.segregationAggregateApplies(mainValueApplies)
      : { aggregateApplies: mainValueApplies, postAggregateApplies: [] as ApplyExpression[] };
    const mainLeafApplies: ApplyExpression[] = [...mainDatasetApplies, ...mainSeg.aggregateApplies];
    const mainPostAggApplies: ApplyExpression[] = mainSeg.postAggregateApplies;

    if (gateHasLinkedOnlySplit) {
      // INV-2 gate, evaluated on the segregated LEAVES (post avg-rewrite +
      // segregation). A measure is jsJoin-safe iff every one of its leaf
      // aggregates re-aggregates by a single homomorphic reducer (trait
      // 'sum' | 'min' | 'max'). The scalar recombination on top (divide,
      // subtract, …) is replayed in JS after re-aggregation, so it does NOT
      // affect decomposability — only the leaves do. A 'none' leaf
      // (countDistinct, quantile, mode, customAggregate) cannot be recombined
      // from partition partials and forces the native-JOIN path, where the
      // measure is computed in a single SQL at the bucket grain.
      const undecomposableLeaf = mainLeafApplies.find(a => {
        if (a.expression.type === 'DATASET') return false;
        return External.resolveApplyDecomposeTrait(a) === 'none';
      });
      if (undecomposableLeaf) {
        // The JS-join path is unsafe. Surface a nativeJoin
        // discriminator so the execution layer (Phase 4) emits a
        // single Druid SQL with an in-engine JOIN.
        //
        // Find the single linked-only split alias driving the GROUP
        // BY. Multi-alias native-JOIN (e.g. linked-only × shared) is
        // post-MVP; if the user splits on more than one linked-only
        // alias, fall through to null so the caller gets a clear
        // single-query SQL error rather than a half-built native
        // join.
        const linkedOnlySplitAliases: string[] = [];
        // Main-side split aliases that must ALSO be carried into the combined
        // SQL's SELECT + GROUP BY (e.g. a `$__time.timeBucket(P1D)` day bucket).
        // Fix B classifies the main timeAttribute main-side against an eternal
        // lookup; before this collection the native-JOIN renderer projected ONLY
        // the linked-only key and emitted `GROUP BY 1`, silently dropping every
        // main-side split — so a [Time(Day) × country] double split lost the
        // time dimension and countDistinct collapsed to an all-period count.
        const mainSideSplitAliases: string[] = [];
        const mainAttrsForGate2: Record<string, true> = {};
        for (const a of this.rawAttributes || []) mainAttrsForGate2[a.name] = true;
        for (const k in this.derivedAttributes) mainAttrsForGate2[k] = true;
        const taForGate2 = (this as any).timeAttribute;
        if (typeof taForGate2 === 'string' && taForGate2.length > 0) {
          mainAttrsForGate2[taForGate2] = true;
        }
        for (const alias of this.split.keys) {
          const exAlias = this.split.splits[alias];
          const refs = exAlias.getFreeReferences();
          if (refs.length === 0) continue;
          const anyInMain = refs.some(r => mainAttrsForGate2[r]);
          if (anyInMain) {
            // A main-side group key — carried into the combined SQL alongside
            // the linked key. (A genuinely ambiguous alias whose refs ALSO live
            // only on the linked side never reaches here without a 'none' leaf;
            // with one, classifying the main-resolvable side is correct — fix B.)
            mainSideSplitAliases.push(alias);
            continue;
          }
          for (const lsName in this.linkedSources) {
            const ls = this.linkedSources[lsName];
            const linkedAttrs: Record<string, true> = {};
            if (ls.attributes) for (const a of ls.attributes as any[]) linkedAttrs[a.name] = true;
            if (ls.derivedAttributes) for (const k in ls.derivedAttributes) linkedAttrs[k] = true;
            if (refs.every(r => linkedAttrs[r])) {
              linkedOnlySplitAliases.push(alias);
              break;
            }
          }
        }
        // Native-JOIN computes every measure in a single SQL grouped at the
        // bucket grain, so there is no fan-out and avg/min/max need no
        // decomposition there. Pass the ORIGINAL (un-rewritten) value applies:
        // `average($x)` renders as `AVG($x)` and `min($x)` as `MIN($x)`
        // directly. Passing the avg-REWRITTEN `divide(sum,count)` form here was
        // the bug — `renderAggregateSQL` has no `divide` case and silently
        // dropped the SELECT item while the ORDER BY still referenced it,
        // emitting malformed SQL. The original-aggregate form is what
        // `renderAggregateSQL` understands.
        const nativeJoinApplies = this.applies.filter(
          a => a.expression.type === 'DATASET' || mainValueApplies.some(m => m.name === a.name),
        );
        const nativeJoin = this.getNativeJoinDecomposition(
          linkedOnlySplitAliases,
          nativeJoinApplies,
          involvedLinkedNames,
          mainSideSplitAliases,
        );
        return nativeJoin;
      }
    }

    const linkedExternals: {
      name: string;
      external: External;
      joinKeys: string[];
      joinMode: 'inner' | 'left';
    }[] = [];

    // Track which aliases of `this.split` got routed to SOME
    // classification (main/shared/linkedOnly) across any iteration.
    // After the loop, any alias never classified anywhere is truly
    // dangling — surfaced as a clear error below instead of silently
    // producing an incomplete join.
    const everClassifiedAliases: Record<string, true> = {};

    // Once we commit to decomposing, any further inability to materialize
    // sub-queries is a hard error — falling back to the single-query path
    // would silently corrupt the result by pretending the foreign ref is
    // a native column. Clear errors > silent wrong data.
    for (const lsName in involvedLinkedNames) {
      const config = this.linkedSources[lsName];
      // A main split alias is valid on the linked side iff every free
      // Always synthesize a fresh RAW linked external — the one captured during
      // apply-absorption is in 'value' mode (it already absorbed an aggregate)
      // and can't re-accept a split. Same recipe as expandLinkedSourcesInDatum.
      // Normalize attributes/derivedAttributes: callers may have passed plain
      // JS objects (turnilo does), so coerce to AttributeInfo[] / Expression[]
      // unless they are already.
      const normalizedAttributes: Attributes | undefined = config.attributes
        ? (config.attributes as any[]).map(a =>
            a instanceof AttributeInfo ? a : AttributeInfo.fromJS(a),
          )
        : undefined;
      let normalizedDerived: Record<string, Expression> | undefined;
      if (config.derivedAttributes) {
        normalizedDerived = {};
        for (const k in config.derivedAttributes) {
          const v = (config.derivedAttributes as any)[k];
          normalizedDerived[k] = v instanceof Expression ? v : Expression.fromJSLoose(v);
        }
      }
      // Prune the inherited filter to the linked side's schema — main's
      // filter may reference columns only in main's own rawAttributes
      // (e.g. `reference`), and those clauses are identity on the linked
      // side. The join on shared/synthetic joinKeys propagates main's
      // extra narrowing into the linked rows.
      //
      // `timeAlignment` on the linkedSource config opts out of
      // propagating main's __time clause:
      //   - undefined / "bucketed" — __time stays in linkedSchemaNames;
      //     the filter clause survives the prune and reaches the lookup.
      //   - "eternal"              — the timeAttribute is deliberately
      //     withheld so `pruneFilterToSchema` treats its refs as
      //     out-of-scope and rewrites them to TRUE. Used when the
      //     lookup is a materialised snapshot whose rows carry a
      //     sentinel __time (e.g. 1970) and main's interval would
      //     return zero rows.
      const timeAttrName =
        (typeof (this as any).getTimeAttribute === 'function'
          ? (this as any).getTimeAttribute()
          : undefined) || (this as any).timeAttribute;
      const timeAlignment = (config as any).timeAlignment;
      const linkedSchemaNames: Record<string, true> = {};
      if (normalizedAttributes) {
        for (const a of normalizedAttributes) {
          if (timeAlignment === 'eternal' && a.name === timeAttrName) continue;
          linkedSchemaNames[a.name] = true;
        }
      }
      if (normalizedDerived) {
        for (const k in normalizedDerived) {
          if (timeAlignment === 'eternal' && k === timeAttrName) continue;
          linkedSchemaNames[k] = true;
        }
      }
      const prunedMainFilter = External.pruneFilterToSchema(this.filter, linkedSchemaNames);
      // AND in any linked-only filter clause that `pruneLinkedFilterRefsInTree`
      // harvested onto this PER-REQUEST config copy (clauses over columns that
      // live only on the lookup — e.g. `$brand_country.overlap(['Francia'])` —
      // which `.simplify()` would otherwise discard along with the dead sibling
      // apply that carried them). `config` here is `this.linkedSources[lsName]`,
      // and when a linked-only clause was harvested `this` is the per-request
      // External copy whose fresh config carries `.filter`; the shared
      // settings-manager config is never written. Without this the lookup
      // sub-query would emit no WHERE for the linked-only filter and the inner
      // join would restrict nothing. The clause is already pruned to the lookup
      // schema, so it is safe to AND directly.
      const stashedLinkedFilter: Expression | undefined = (config as any).filter;
      const templateFilter =
        stashedLinkedFilter && !stashedLinkedFilter.equals(Expression.TRUE)
          ? prunedMainFilter && !prunedMainFilter.equals(Expression.TRUE)
            ? prunedMainFilter.and(stashedLinkedFilter).simplify()
            : stashedLinkedFilter
          : prunedMainFilter;
      // When `timeAlignment: "eternal"` strips the time clause, the
      // resulting Druid query has no __time bound and would throw
      // "must filter on time unless the allowEternity flag is set".
      // Force-enable allowEternity on the template: declaring the
      // linked source eternal is declaring this permission too.
      const templateAllowEternity =
        timeAlignment === 'eternal' ? true : (this as any).allowEternity;

      // Cross-engine override. Absent → inherit main's engine/version/
      // requester (canonical). Present + different engine → route this
      // linkedSource's sub-query to its own store; requester is mandatory
      // (fails loud — never silently inherits the main requester). This is
      // the JS-join path: each side runs its own query, so cross-engine is
      // fully supported here. The nativeJoin path (getNativeJoinDecomposition)
      // refuses cross-engine because a single SQL JOIN cannot span engines.
      const binding = External.resolveLinkedEngineBinding(
        config,
        lsName,
        this.engine,
        this.version,
        this.requester,
      );

      const template = External.fromValue({
        engine: binding.engine,
        version: binding.version,
        source: config.source,
        suppress: true,
        rollup: this.rollup,
        concealBuckets: this.concealBuckets,
        requester: binding.requester,
        attributes: normalizedAttributes,
        derivedAttributes: normalizedDerived,
        filter: templateFilter,
        timeAttribute: (this as any).timeAttribute,
        customAggregations: (this as any).customAggregations,
        customTransforms: (this as any).customTransforms,
        allowEternity: templateAllowEternity,
        allowSelectQueries: (this as any).allowSelectQueries,
        exactResultsOnly: (this as any).exactResultsOnly,
        querySelection: (this as any).querySelection,
        context: (this as any).context,
      });
      // A linked attribute set for this source — names that exist in the
      // linked external's schema (attributes + derivedAttributes) but not
      // necessarily as joinKeys. Used to classify split aliases whose refs
      // are linked-only (meaningful on that side but not on main).
      const linkedAttrs: Record<string, true> = {};
      if (config.attributes) {
        for (const a of config.attributes as any[]) linkedAttrs[a.name] = true;
      }
      if (config.derivedAttributes) {
        for (const k in config.derivedAttributes) linkedAttrs[k] = true;
      }
      // Main's own schema (rawAttributes + derivedAttributes + timeAttribute)
      // — used to classify splits that are main-compatible. rawAttributes is
      // populated from attributes via the constructor; timeAttribute is
      // declared separately on Druid SQL externals and must be added in
      // explicitly so a split on the time column is recognised as main-side.
      const mainAttrs: Record<string, true> = {};
      for (const a of this.rawAttributes || []) mainAttrs[a.name] = true;
      for (const k in this.derivedAttributes) mainAttrs[k] = true;
      const ta = (this as any).timeAttribute;
      if (typeof ta === 'string' && ta.length > 0) mainAttrs[ta] = true;

      // Delegate to the declarative classifier. The classifier reads
      // `config.sharedDimensions` for dimensions that live on both sides
      // with the same meaning, and refuses to silently infer shared-ness
      // from schema overlap — an actionable error fires when the user's
      // intent is ambiguous. See External.classifySplitAliases.
      const classified = External.classifySplitAliases(
        this.split,
        mainAttrs,
        linkedAttrs,
        config,
        lsName,
        typeof ta === 'string' && ta.length > 0 ? ta : undefined,
      );
      const sharedAliases: string[] = [...classified.shared];
      const mainOnlyAliases: string[] = [...classified.mainOnly];
      const linkedOnlyAliases: string[] = [...classified.linkedOnly];
      // Record what this iteration classified. Foreign-linked aliases
      // (classifier returned them in `foreignLinked`) are deliberately
      // NOT recorded here — they belong to a sibling iteration. Only
      // at the post-loop guard do we assert every alias was covered
      // somewhere.
      for (const a of sharedAliases) everClassifiedAliases[a] = true;
      for (const a of mainOnlyAliases) everClassifiedAliases[a] = true;
      for (const a of linkedOnlyAliases) everClassifiedAliases[a] = true;
      // Zero shared splits AND main/linked contributions exist → auto-inject
      // one split per declared joinKey onto both sides. The user didn't pick
      // a shared dimension, but the cube's joinKeys config says "this pair of
      // columns is the canonical identity for the join". We wire them in as
      // synthetic splits `__join_<key>` on each side and drop them from the
      // final projection post-join — the caller sees only the columns they
      // asked for.
      //
      // Semantics note: the effective granularity of the result is the cross
      // product of user splits × joinKey tuples. For cubes where a joinKey
      // like `partitionId` identifies a product that maps 1:1 to the user's
      // dimensions (ean, etc.), this is the expected "one row per product"
      // behavior. For cubes where that's not true, the user should include
      // an explicit shared dimension to make their intended granularity
      // explicit.
      const syntheticAliasesForThisSource: string[] = [];
      const syntheticSplits: Record<string, Expression> = {};
      const syntheticSplitsLinked: Record<string, Expression> = {};
      if (sharedAliases.length === 0 && (config.joinKeys || []).length > 0) {
        // Resolve the key's type per side — main and linked may store the
        // same logical identifier with different native types (BIGINT vs
        // VARCHAR is common when one side stores the key raw and the other
        // derives it from a string column). We build each side's synthetic
        // ref with its side-local type so addExpression accepts the split,
        // and cast to STRING as the join medium so the in-memory matcher
        // aligns the rows regardless of native storage. STRING is the
        // common denominator — any joinKey can round-trip through it.
        const mainTypeForKey: Record<string, PlyType> = {};
        for (const a of this.rawAttributes || []) mainTypeForKey[a.name] = a.type;
        for (const k in this.derivedAttributes) {
          const dtype = (this.derivedAttributes[k] as any).type;
          if (dtype) mainTypeForKey[k] = dtype;
        }
        // Include timeAttribute — when a joinKey happens to be the time
        // column, the main-side type lookup must resolve it as TIME so the
        // synthetic split rebuilds with the correct native type before the
        // join cast to STRING.
        const taKey = (this as any).timeAttribute;
        if (typeof taKey === 'string' && taKey.length > 0) mainTypeForKey[taKey] = 'TIME';
        const linkedTypeForKey: Record<string, PlyType> = {};
        if (config.attributes) {
          for (const a of config.attributes as any[]) linkedTypeForKey[a.name] = a.type;
        }
        if (config.derivedAttributes) {
          // derivedAttributes may be string formulas or parsed Expressions;
          // their concrete type is whatever their formula resolves to on
          // the linked side's rawAttributes. For join purposes we cast to
          // STRING, so we only need a non-null placeholder here.
          for (const k in config.derivedAttributes) {
            if (!linkedTypeForKey[k]) linkedTypeForKey[k] = 'STRING';
          }
        }
        // Iterate exactly the subset declared as auto-injectable. The
        // cube owns this decision via `config.autoInjectJoinKeys`: keys
        // listed here become synthetic `__join_<key>` splits; keys
        // omitted remain legitimate join anchors for user-chosen
        // explicit shared splits but are never auto-added. Default:
        // everything in joinKeys is auto-injectable.
        const injectableKeys = External.resolveAutoInjectJoinKeys(config);
        for (const key of injectableKeys) {
          const alias = `__join_${key}`;
          if (this.split.splits[alias]) continue; // collision (unlikely) — skip
          const mainRef = $(key, mainTypeForKey[key] || 'STRING');
          const linkedRef = $(key, linkedTypeForKey[key] || 'STRING');
          syntheticSplits[alias] = mainRef.cast('STRING');
          syntheticSplitsLinked[alias] = linkedRef.cast('STRING');
          syntheticAliasesForThisSource.push(alias);
          sharedAliases.push(alias);
        }
      }

      // Linked side: shared splits (including synthetic) + this source's linked-only splits
      let linkedExternal: External = template;
      const linkedSplitMap: Record<string, Expression> = {};
      for (const a of sharedAliases) {
        // For synthetic joinKey splits use the linked-side variant so the
        // ref carries this side's native type. User-picked shared splits
        // are the same expression on both sides and reuse this.split.splits.
        linkedSplitMap[a] = this.split.splits[a] || syntheticSplitsLinked[a] || syntheticSplits[a];
      }
      for (const a of linkedOnlyAliases) linkedSplitMap[a] = this.split.splits[a];
      if (Object.keys(linkedSplitMap).length === 0) {
        throw new Error(
          `Cross-source query for "${lsName}" produced no usable split on the linked side`,
        );
      }
      linkedExternal = linkedExternal.addExpression(this.split.changeSplits(linkedSplitMap));
      if (!linkedExternal) {
        throw new Error(
          `Linked source "${lsName}" rejected its share of splits [${Object.keys(
            linkedSplitMap,
          ).join(', ')}] — the expressions must resolve in its schema`,
        );
      }

      for (const apply of linkedAppliesByName[lsName] || []) {
        const next = linkedExternal.addExpression(apply);
        if (!next) {
          throw new Error(
            `Linked source "${lsName}" rejected apply "${apply.name}" — the aggregate's refs must resolve in its schema`,
          );
        }
        linkedExternal = next;
      }

      // Resolve join mode per linked source. The cube MUST declare this
      // explicitly via config.joinMode — without a default, the engine
      // refuses to guess whether orphan main rows should survive. A
      // missing joinMode surfaces as a clear error pointing the cube
      // author at the required field rather than silently inner- or
      // left-joining.
      const resolvedJoinMode = External.resolveLinkedJoinMode(config);
      if (!resolvedJoinMode) {
        throw new Error(
          `Linked source "${lsName}" must declare \`joinMode\` ('inner' | 'left') — cross-source decomposition won't guess which orphan-row semantic you want. 'inner' drops main rows without a linked match (typical for monitoring cubes); 'left' keeps them with linked columns undefined.`,
        );
      }

      linkedExternals.push({
        name: lsName,
        external: linkedExternal,
        joinKeys: sharedAliases,
        joinMode: resolvedJoinMode,
      });

      // Stash the main-compatible split set for rebuilding main after the loop.
      // (We recompute per linked source but the intersection of "main-keepable"
      // aliases is the same across sources — linked-only for source A is only
      // valid on A, so it must be dropped from main regardless of B.)
      (this as any)._lastMainKeepableAliases = [...mainOnlyAliases, ...sharedAliases];
      // Remember the synthetic splits so the main side's split map can pull
      // from them (they are NOT in this.split.splits).
      (this as any)._lastSyntheticSplits = {
        ...((this as any)._lastSyntheticSplits || {}),
        ...syntheticSplits,
      };
      // Collect synthetic aliases across all sources, deduped — the execution
      // layer will strip these columns from the final joined dataset.
      for (const a of syntheticAliasesForThisSource) {
        if (!(this as any)._syntheticAliasMap) (this as any)._syntheticAliasMap = {};
        (this as any)._syntheticAliasMap[a] = true;
      }
    }

    // Dangling-alias guard — a split whose refs resolved in neither
    // main nor ANY linkedSource's attributes is a real error (and
    // would have been caught by pre-refactor classifier that threw).
    // The new classifier skips such aliases at each iteration via
    // `foreignLinked`; we surface the dangling case here so the error
    // message names every unclassified alias in one go.
    const dangling: string[] = [];
    for (const alias of this.split.keys) {
      if (!everClassifiedAliases[alias]) dangling.push(alias);
    }
    if (dangling.length > 0) {
      throw new Error(
        `Cross-source decomposition: split aliases [${dangling.join(
          ', ',
        )}] reference columns that resolve in neither main nor any declared linkedSource's attributes. Check that the linkedSource whose schema contains these refs is declared on the cube and has been introspected (attributes populated).`,
      );
    }

    // Build the main side: keep only splits whose refs resolve in main's own
    // schema, drop foreign applies. When no main-keepable split remains the
    // main external becomes TOTALS mode — one row of aggregates that the
    // linked side broadcasts across its groupings.
    const mainKeepableAliases: string[] = (this as any)._lastMainKeepableAliases || [];
    const syntheticSplitsAll: Record<string, Expression> = (this as any)._lastSyntheticSplits || {};
    const mainValue = this.valueOf();
    // The main sub-query projects the segregated LEAF aggregates, not the
    // derived measures. The scalar recombination (mainPostAggApplies) runs in
    // JS after the post-join re-aggregation. For a pure single-aggregate
    // measure (e.g. `sum`, `min`) segregation keeps it under its own name with
    // an empty post-aggregate, so this is identity for the simple case.
    mainValue.applies = mainLeafApplies;
    if (mainKeepableAliases.length > 0) {
      const mainSplitMap: Record<string, Expression> = {};
      for (const a of mainKeepableAliases)
        mainSplitMap[a] = this.split.splits[a] || syntheticSplitsAll[a];
      mainValue.split = this.split.changeSplits(mainSplitMap);
      // Build attributes. For synthetic splits the expression is a bare ref
      // with no resolved type yet — look the column's type up in the main
      // external's own schema (rawAttributes + derivedAttributes +
      // timeAttribute). The timeAttribute entry covers splits that group
      // by the time column directly (rare but legal — e.g. a TIME_FLOOR'd
      // bucket); without it the synthetic split would default to STRING
      // and lose the temporal type.
      const mainAttrTypeByName: Record<string, PlyType> = {};
      for (const a of this.rawAttributes || []) mainAttrTypeByName[a.name] = a.type;
      for (const k in this.derivedAttributes) {
        const dtype = (this.derivedAttributes[k] as any).type;
        if (dtype) mainAttrTypeByName[k] = dtype;
      }
      const taType = (this as any).timeAttribute;
      if (typeof taType === 'string' && taType.length > 0) {
        mainAttrTypeByName[taType] = 'TIME';
      }
      mainValue.attributes = [
        ...mainKeepableAliases.map(name => {
          const ex = this.split.splits[name] || syntheticSplitsAll[name];
          let t = Set.unwrapSetType(ex.type);
          if (!t && ex instanceof RefExpression && mainAttrTypeByName[ex.name]) {
            t = mainAttrTypeByName[ex.name];
          }
          return new AttributeInfo({ name, type: t || 'STRING' });
        }),
        ...mainLeafApplies.map(a => new AttributeInfo({ name: a.name, type: a.expression.type })),
      ];
    } else {
      // No main split → totals mode. rawAttributes comes back as attributes,
      // split drops, applies stay as the aggregate list.
      mainValue.mode = 'total';
      mainValue.split = null;
      mainValue.dataName = undefined;
      mainValue.rawAttributes = this.rawAttributes;
      mainValue.attributes = mainLeafApplies.map(
        a => new AttributeInfo({ name: a.name, type: a.expression.type }),
      );
      mainValue.sort = null;
      mainValue.limit = null;
    }

    // Sort / limit routing. If the sort references a main-side apply or a
    // main-keepable split alias, keep it on main (pre-join); the engine can
    // use it for topN optimization. If the sort targets something that lives
    // only post-join (a linked apply, for instance), strip it from main and
    // return it as postJoinSort so the caller can apply it to the joined
    // Dataset. The matching limit moves with the sort — applying a pre-join
    // limit while sorting post-join would silently drop rows.
    // Main-resolvable apply names = the LEAF columns the main sub-query
    // projects. A sort/HAVING on a derived measure (e.g. `avg_price`, which is
    // a post-aggregate recombination of leaf columns) is NOT main-resolvable
    // and is correctly forced post-join, where it runs after recombination.
    const mainApplyNames: Record<string, true> = {};
    for (const a of mainLeafApplies) mainApplyNames[a.name] = true;
    const mainKeepableNames: Record<string, true> = {};
    for (const a of mainKeepableAliases) mainKeepableNames[a] = true;

    // Sort/limit routing: when main's row identity matches the final
    // grid row identity, main-side sort + LIMIT is safe and enables
    // Druid's topN optimization. When the join fans main rows out into
    // multiple grid rows — because the user split includes a linked-
    // only dimension — main's LIMIT would cap at main-row count, not
    // grid-row count, and the result under/over-returns depending on
    // cardinality.
    //
    // Two signals force sort+limit post-join:
    //   1. The query has linked-only split aliases: the join expands
    //      main rows into N per (linked-only combination), so main's
    //      LIMIT doesn't speak to the grid-row count.
    //   2. The sort references something main can't resolve (a linked
    //      apply name or a linked-only split): main can't order by it.
    //
    // Otherwise (shared-only splits, sort on a main apply or shared
    // split) main's sort+limit stays put — the topN per-period fan-out
    // in timeshift decomposition and Druid's LIMIT clause remain
    // valid and efficient.
    const hasLinkedOnlySplit = linkedExternals.some(
      le =>
        le.external && (le.external as any).split?.keys?.some((k: string) => !mainKeepableNames[k]),
    );
    let postJoinSort: SortExpression | undefined;
    let postJoinLimit: LimitExpression | undefined;
    if (this.sort) {
      const sortRef = this.sort.expression;
      const sortOnMainSide =
        sortRef instanceof RefExpression &&
        (mainApplyNames[sortRef.name] || mainKeepableNames[sortRef.name]);
      const mustMovePostJoin = !sortOnMainSide || hasLinkedOnlySplit;
      if (mustMovePostJoin) {
        postJoinSort = this.sort;
        postJoinLimit = this.limit || undefined;
        mainValue.sort = null;
        mainValue.limit = null;
      }
    } else if (this.limit && hasLinkedOnlySplit) {
      // No sort but a limit + linked-only splits: the limit has to run
      // post-join to cap grid rows rather than main rows.
      postJoinLimit = this.limit;
      mainValue.limit = null;
    }

    // HAVING routing. The outer External's havingFilter was copied into
    // mainValue by valueOf(). If it references an apply routed to a linked
    // sub-External (e.g. `$avg_rating_linked > 0` where avg_rating_linked
    // lives on the reviews side), pushing it into main's SQL emits a
    // predicate over a column Druid can't resolve and the query 500s.
    //
    // Split per Ogievetsky's TODO in 0f9cbf3: clauses over main-resolvable
    // names stay on main's HAVING (Druid-side, fast); clauses over post-
    // join names (linked applies, linked-only splits) move to a post-join
    // .filter() on the joined Dataset. AND combines both halves; OR/NOT
    // that mix scopes are all post-join.
    let postJoinHavingFilter: Expression | undefined;
    if (mainValue.havingFilter && !mainValue.havingFilter.equals(Expression.TRUE)) {
      const mainResolvable: Record<string, true> = {};
      for (const a of mainLeafApplies) mainResolvable[a.name] = true;
      for (const a of mainKeepableAliases) mainResolvable[a] = true;
      const { main: havingOnMain, post: havingOnPost } = External.splitFilterByScope(
        mainValue.havingFilter,
        mainResolvable,
      );
      mainValue.havingFilter = havingOnMain;
      if (!havingOnPost.equals(Expression.TRUE)) {
        postJoinHavingFilter = havingOnPost;
      }

      // If any HAVING moved post-join, main's LIMIT would starve the
      // result: main returns N rows, HAVING drops some, we end up
      // with fewer than N final rows. Force both sort and limit to
      // post-join so the cap applies against the HAVING-filtered set.
      if (postJoinHavingFilter) {
        if (this.sort && !postJoinSort) {
          postJoinSort = this.sort;
          mainValue.sort = null;
        }
        if (this.limit && !postJoinLimit) {
          postJoinLimit = this.limit;
          mainValue.limit = null;
        }
      }
    }

    const mainExternal = External.fromValue(mainValue);

    const syntheticAliasMap: Record<string, true> | undefined = (this as any)._syntheticAliasMap;
    const syntheticJoinAliases = syntheticAliasMap ? Object.keys(syntheticAliasMap) : undefined;

    // Post-join re-aggregation grain (INV-1). The user's split keys with the
    // synthetic join aliases removed — the grain the JS-join must collapse to
    // after the join fans main's join-key-grain rows across the linked split.
    // When this equals the join-key grain (no linked-only split), there is no
    // fan-out and the re-agg is a no-op; we still publish the keys so the
    // executor's net is unconditional.
    const syntheticSet = syntheticAliasMap || {};
    const reAggKeys = (this.split ? this.split.keys : []).filter(k => !syntheticSet[k]);
    // Trait per LEAF aggregate, driving the post-join re-agg reducer. Keyed by
    // the segregated leaves (the columns the main sub-query actually projects),
    // NOT by the derived measures. This is the fix: the OLD code keyed by the
    // avg-rewritten derived applies, where `average` resolved to the ratio
    // `divide(sum,count)` whose root is non-aggregate → trait 'none', so
    // re-aggregation refused to collapse the fan-out and `assertDatasetShape`
    // 500'd. The leaves are single aggregates (sum/count/min/max) that DO
    // recombine; the derived measure is reconstructed afterwards by replaying
    // `postAggregateApplies` over the re-aggregated leaf columns.
    const reAggApplyTraits: Record<string, 'sum' | 'min' | 'max' | 'none'> = {};
    for (const a of mainLeafApplies) {
      if (a.expression && a.expression.type === 'DATASET') continue; // scope registration
      reAggApplyTraits[a.name] = External.resolveApplyDecomposeTrait(a);
    }

    return {
      mainExternal,
      linkedExternals,
      postJoinSort,
      postJoinLimit,
      syntheticJoinAliases,
      postJoinHavingFilter,
      reAggKeys,
      reAggApplyTraits,
      postAggregateApplies: mainPostAggApplies,
    };
  }
}
