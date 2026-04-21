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
  linkedSources?: Record<
    string,
    {
      source: string;
      joinKeys: string[];
      // Schema of the linked datasource. When omitted plywood falls back
      // to inheriting from main's rawAttributes, which is rarely correct for
      // cross-datasource joins — callers should supply this explicitly.
      attributes?: Attributes;
      derivedAttributes?: Record<string, Expression>;
    }
  >;
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
  linkedSources?: Record<
    string,
    {
      source: string;
      joinKeys: string[];
      attributes?: AttributeJSs;
      derivedAttributes?: Record<string, ExpressionJS>;
    }
  >;
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

    if (op === 'and' || op === 'or') {
      const left = External.pruneFilterToSchema((filter as any).operand, schemaNames);
      const right = External.pruneFilterToSchema((filter as any).expression, schemaNames);
      if (op === 'and') return left.and(right).simplify();
      return left.or(right).simplify();
    }

    if (op === 'not') {
      const inner = External.pruneFilterToSchema((filter as any).operand, schemaNames);
      return inner.not().simplify();
    }

    // Atomic predicate: keep iff every free reference exists in the schema.
    const refs = filter.getFreeReferences();
    for (const r of refs) {
      if (!schemaNames[r]) return Expression.TRUE;
    }
    return filter;
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
        const ls = parameters.linkedSources[name];
        value.linkedSources[name] = {
          source: ls.source,
          joinKeys: ls.joinKeys,
          attributes: ls.attributes ? AttributeInfo.fromJSs(ls.attributes) : undefined,
          derivedAttributes: ls.derivedAttributes
            ? Expression.expressionLookupFromJS(ls.derivedAttributes)
            : undefined,
        };
      }
    }

    value.filter = parameters.filter ? Expression.fromJS(parameters.filter) : Expression.TRUE;

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

        const linkedValue: ExternalValue = {
          engine: value.engine,
          version: value.version,
          source: ls.source,
          suppress: true,
          rollup: value.rollup,
          concealBuckets: value.concealBuckets,
          requester: value.requester,
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
  public linkedSources: Record<
    string,
    {
      source: string;
      joinKeys: string[];
      attributes?: Attributes;
      derivedAttributes?: Record<string, Expression>;
    }
  >;

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
          if (mainIsTotals && linked.length > 0) {
            joined = linked[0] as Dataset;
            joined = joined.join(main as Dataset);
            for (let i = 1; i < linked.length; i++) {
              joined = joined.join(linked[i] as Dataset);
            }
          } else {
            joined = main as Dataset;
            for (const side of linked) {
              joined = joined.join(side as Dataset);
            }
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
          return joined;
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
  public getCrossExternalDecomposition(): {
    mainExternal: External;
    linkedExternals: { name: string; external: External; joinKeys: string[] }[];
    postJoinSort?: SortExpression;
    postJoinLimit?: LimitExpression;
    // Aliases the decomposition itself inserted into both sides' splits so
    // the join has something to align on (declared joinKeys from the linked
    // source's config). These are implementation detail — the execution
    // layer drops them from the final dataset so the caller sees only the
    // columns they asked for.
    syntheticJoinAliases?: string[];
  } | null {
    if (this.mode !== 'split') return null;
    if (!this.applies || this.applies.length === 0) return null;
    if (!this.linkedSources || Object.keys(this.linkedSources).length === 0) return null;

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

    for (const apply of this.applies) {
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
    const involvedLinkedNames: Record<string, true> = Object.keys(linkedAppliesByName).reduce(
      (acc, n) => ((acc[n] = true), acc),
      {} as Record<string, true>,
    );
    for (const alias of this.split.keys) {
      const ex = this.split.splits[alias];
      const refs = ex.getFreeReferences();
      if (refs.length === 0) continue;
      const mainAttrs: Record<string, true> = {};
      for (const a of this.rawAttributes || []) mainAttrs[a.name] = true;
      for (const k in this.derivedAttributes) mainAttrs[k] = true;
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

    const linkedExternals: { name: string; external: External; joinKeys: string[] }[] = [];

    // Once we commit to decomposing, any further inability to materialize
    // sub-queries is a hard error — falling back to the single-query path
    // would silently corrupt the result by pretending the foreign ref is
    // a native column. Clear errors > silent wrong data.
    for (const lsName in involvedLinkedNames) {
      const config = this.linkedSources[lsName];
      // A main split alias is valid on the linked side iff every free
      // reference in its expression is a declared joinKey column. This
      // covers plain refs ($competitor), derived expressions (timeBucket
      // ($__time, "P1D")), and rejects mixed forms like
      // $competitor.concat($price) where only some refs are shared.
      const joinKeyCols: Record<string, true> = {};
      for (const c of config.joinKeys || []) joinKeyCols[c] = true;
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
      const template = External.fromValue({
        engine: this.engine,
        version: this.version,
        source: config.source,
        suppress: true,
        rollup: this.rollup,
        concealBuckets: this.concealBuckets,
        requester: this.requester,
        attributes: normalizedAttributes,
        derivedAttributes: normalizedDerived,
        filter: this.filter,
        timeAttribute: (this as any).timeAttribute,
        customAggregations: (this as any).customAggregations,
        customTransforms: (this as any).customTransforms,
        allowEternity: (this as any).allowEternity,
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
      // Main's own schema (rawAttributes + derivedAttributes) — used to
      // classify splits that are main-compatible. rawAttributes is populated
      // from attributes via the constructor.
      const mainAttrs: Record<string, true> = {};
      for (const a of this.rawAttributes || []) mainAttrs[a.name] = true;
      for (const k in this.derivedAttributes) mainAttrs[k] = true;

      // Partition main's current split aliases by where each is valid:
      //   shared     = refs ⊆ joinKeys (goes to BOTH sides, becomes the join key)
      //   mainOnly   = refs ⊆ mainAttrs but not all in joinKeys (stays on main)
      //   linkedOnly = refs ⊆ linkedAttrs (only this linked source) → goes only to linked
      const sharedAliases: string[] = [];
      const mainOnlyAliases: string[] = [];
      const linkedOnlyAliases: string[] = [];
      for (const alias of this.split.keys) {
        const ex = this.split.splits[alias];
        const refs = ex.getFreeReferences();
        if (refs.length === 0) {
          mainOnlyAliases.push(alias);
          continue;
        }
        const isShared = refs.every(r => joinKeyCols[r]);
        const isMainResolvable = refs.every(r => mainAttrs[r]);
        const isLinkedResolvable = refs.every(r => linkedAttrs[r]);
        if (isShared) sharedAliases.push(alias);
        else if (isMainResolvable) mainOnlyAliases.push(alias);
        else if (isLinkedResolvable) linkedOnlyAliases.push(alias);
        else {
          throw new Error(
            `Split alias "${alias}" references columns that resolve in neither main nor "${lsName}" (refs: [${refs.join(
              ', ',
            )}])`,
          );
        }
      }
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
        for (const key of config.joinKeys) {
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

      linkedExternals.push({
        name: lsName,
        external: linkedExternal,
        joinKeys: sharedAliases,
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

    // Build the main side: keep only splits whose refs resolve in main's own
    // schema, drop foreign applies. When no main-keepable split remains the
    // main external becomes TOTALS mode — one row of aggregates that the
    // linked side broadcasts across its groupings.
    const mainKeepableAliases: string[] = (this as any)._lastMainKeepableAliases || [];
    const syntheticSplitsAll: Record<string, Expression> = (this as any)._lastSyntheticSplits || {};
    const mainValue = this.valueOf();
    mainValue.applies = mainApplies;
    if (mainKeepableAliases.length > 0) {
      const mainSplitMap: Record<string, Expression> = {};
      for (const a of mainKeepableAliases)
        mainSplitMap[a] = this.split.splits[a] || syntheticSplitsAll[a];
      mainValue.split = this.split.changeSplits(mainSplitMap);
      // Build attributes. For synthetic splits the expression is a bare ref
      // with no resolved type yet — look the column's type up in the main
      // external's own schema (rawAttributes + derivedAttributes).
      const mainAttrTypeByName: Record<string, PlyType> = {};
      for (const a of this.rawAttributes || []) mainAttrTypeByName[a.name] = a.type;
      for (const k in this.derivedAttributes) {
        const dtype = (this.derivedAttributes[k] as any).type;
        if (dtype) mainAttrTypeByName[k] = dtype;
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
        ...mainApplies.map(a => new AttributeInfo({ name: a.name, type: a.expression.type })),
      ];
    } else {
      // No main split → totals mode. rawAttributes comes back as attributes,
      // split drops, applies stay as the aggregate list.
      mainValue.mode = 'total';
      mainValue.split = null;
      mainValue.dataName = undefined;
      mainValue.rawAttributes = this.rawAttributes;
      mainValue.attributes = mainApplies.map(
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
    const mainApplyNames: Record<string, true> = {};
    for (const a of mainApplies) mainApplyNames[a.name] = true;
    const mainKeepableNames: Record<string, true> = {};
    for (const a of mainKeepableAliases) mainKeepableNames[a] = true;

    let postJoinSort: SortExpression | undefined;
    let postJoinLimit: LimitExpression | undefined;
    if (this.sort) {
      const sortRef = this.sort.expression;
      const sortOnMainSide =
        sortRef instanceof RefExpression &&
        (mainApplyNames[sortRef.name] || mainKeepableNames[sortRef.name]);
      if (!sortOnMainSide) {
        postJoinSort = this.sort;
        postJoinLimit = this.limit || undefined;
        mainValue.sort = null;
        mainValue.limit = null;
      }
    }

    const mainExternal = External.fromValue(mainValue);

    const syntheticAliasMap: Record<string, true> | undefined = (this as any)._syntheticAliasMap;
    const syntheticJoinAliases = syntheticAliasMap ? Object.keys(syntheticAliasMap) : undefined;

    return {
      mainExternal,
      linkedExternals,
      postJoinSort,
      postJoinLimit,
      syntheticJoinAliases,
    };
  }
}
