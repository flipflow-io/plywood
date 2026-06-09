/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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

import {
  $,
  AverageExpression,
  ChainableUnaryExpression,
  CountExpression,
  Expression,
} from '../../expressions';

/**
 * COMPANION DECOMPOSITION ALGEBRA (Concern 2 / invariant I1).
 *
 * When a resplit measure forces the outer query to read FROM cte_subsplit, every
 * OTHER ("companion") plain measure must be lowered as outer-agg(inner-agg over the
 * finer sub-split). That two-level form equals the raw single-level aggregate ONLY
 * for aggregates that re-aggregate losslessly over a finer partition:
 *
 *   SUM      additive monoid:   SUM(SUM_k) == SUM        -> trait 'sum'
 *   COUNT    additive monoid:   SUM(COUNT_k) == COUNT    -> trait 'sum' (inner COUNT, outer SUM)
 *   MIN      idempotent semilattice: MIN(MIN_k) == MIN   -> trait 'min'
 *   MAX      idempotent semilattice: MAX(MAX_k) == MAX   -> trait 'max'
 *   AVG      NOT a monoid as written (AVG(AVG_k) != AVG), but decomposable by
 *            carrying TWO channels: inner SUM(expr) + inner COUNT, outer
 *            SUM(sum_k) / SUM(cnt_k). Value-exact, unambiguous.
 *   COUNT DISTINCT / QUANTILE / COLLECT / MODE / STDDEV / opaque SqlAggregate
 *            NOT decomposable from per-sub-group SCALARS (distinct sets overlap;
 *            quantiles/sketches/moments cannot be reconstructed from one number).
 *            -> nonDecomposable.
 *
 * The decomposability taxonomy is read from the canonical `static decomposable:
 * DecomposeTrait` declared on every aggregate class (see
 * src/expressions/mixins/aggregate.ts) — the SAME single source of truth used by
 * Expression.isMeasureDecomposable for the cross-source path. We do NOT re-derive
 * a parallel classification here; we map the trait to the resplit lowering.
 *
 * Result contract: either a decomposable lowering (a list of inner applies to add
 * to cte_subsplit + an outer Expression referencing those inner alias columns), or
 * a nonDecomposable verdict carrying the op and a human reason for the caller's
 * product policy (today: STRICT throw; future: route to a base-rows sibling query).
 */

export interface CompanionDecomposable {
  nonDecomposable: false;
  /** Inner applies to add to cte_subsplit (1 for sum/min/max/count, 2 for avg). */
  innerApplies: { name: string; expression: Expression }[];
  /** Outer expression (rebased on Expression._) referencing the inner alias cols. */
  outer: Expression;
}

export interface CompanionNonDecomposable {
  nonDecomposable: true;
  op: string;
  reason: string;
}

export type CompanionDecomposition = CompanionDecomposable | CompanionNonDecomposable;

/**
 * Resolve the canonical DecomposeTrait for an aggregate expression. The Aggregate
 * mixin clobbers `prototype.constructor`, so resolve via the op-keyed classMap
 * exactly like Expression.isMeasureDecomposable does.
 */
function traitOf(ex: Expression): string | undefined {
  const ctor = (Expression as any).classMap[ex.op];
  return ctor && ctor.decomposable;
}

/**
 * Lower a single companion aggregate `ex` (e.g. $main.sum($qty)) for re-aggregation
 * over the resplit sub-split.
 *
 * @param ex         the companion aggregate expression (operand already Expression._-rooted)
 * @param mkTempName mints a fresh unique inner-CTE column alias (closure over the
 *                   caller's getUniqueAlias so naming stays consistent with resplit applies)
 */
export function decomposeCompanionAggregate(
  ex: Expression,
  mkTempName: () => string,
): CompanionDecomposition {
  // AVG is decomposable but ONLY via two carried channels. Handle it explicitly,
  // BEFORE the trait switch (its trait is 'none' for the cross-source JS-join path,
  // which is a different — lossier — context than this SQL re-aggregation).
  if (ex instanceof AverageExpression) {
    const operand = ex.operand; // Expression._
    const measureEx = ex.expression; // the averaged column, e.g. $pvp
    const sumName = mkTempName();
    const cntName = mkTempName();
    return {
      nonDecomposable: false,
      innerApplies: [
        { name: sumName, expression: operand.sum(measureEx) },
        // NULL-aware count of the AVERAGED column — NOT COUNT(*). SQL AVG(x)
        // skips NULL x, so the denominator channel must count only non-null x
        // (CountExpression lowers a filtered count to SUM(CASE WHEN x IS NOT
        // NULL THEN 1 ELSE 0 END)). A bare COUNT(*) here counts sub-groups
        // whose averaged value is NULL and understates the recombined average
        // (Ogievetsky BUG 1, companion site). Same correction as
        // AverageExpression.decomposeAverage — one count semantics everywhere.
        { name: cntName, expression: operand.filter(measureEx.isnt(null)).simplify().count() },
      ],
      // outer = SUM(sum_k) / SUM(cnt_k)
      outer: Expression._.sum($(sumName)).divide(Expression._.sum($(cntName))),
    };
  }

  const trait = traitOf(ex);

  // COUNT is additive but its inner form is COUNT(*) while the outer reducer is SUM
  // (you sum the per-sub-group counts, you do not COUNT them). Output type stays NUMBER.
  if (ex instanceof CountExpression) {
    const tempName = mkTempName();
    return {
      nonDecomposable: false,
      innerApplies: [{ name: tempName, expression: ex }],
      outer: Expression._.sum($(tempName)),
    };
  }

  // SUM / MIN / MAX: the outer reducer is the SAME operation as the inner aggregate
  // applied over the per-sub-group scalar. SUM(SUM_k)==SUM, MIN(MIN_k)==MIN, MAX(MAX_k)==MAX.
  if (
    (trait === 'sum' || trait === 'min' || trait === 'max') &&
    ex instanceof ChainableUnaryExpression
  ) {
    const tempName = mkTempName();
    return {
      nonDecomposable: false,
      innerApplies: [{ name: tempName, expression: ex }],
      // re-apply the SAME aggregate over the inner alias column
      outer: ex.changeOperand(Expression._).changeExpression($(tempName)),
    };
  }

  // Everything else (trait 'none': CountDistinct, Quantile, Collect, Mode, stddev,
  // opaque SqlAggregate, or any aggregate whose trait can't be reasoned about) is
  // NOT reconstructable from a per-sub-group scalar. Refuse to emit silently-wrong SQL.
  return {
    nonDecomposable: true,
    op: ex.op,
    reason:
      `aggregate '${ex.op}' is not re-aggregable over a resplit sub-split: its value ` +
      `cannot be reconstructed from one scalar per sub-group (distinct sets overlap; ` +
      `quantiles/sketches/moments are not scalar-decomposable). Compute it in a ` +
      `separate query/JOIN over the base rows, or use a mergeable sketch.`,
  };
}
