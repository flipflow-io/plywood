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

import { $, ApplyExpression, Expression, SplitExpression } from '../../expressions';

/**
 * PER-METRIC SPLIT DECOMPOSITION (PerMetricSplitDecomposition).
 *
 * A single query may carry DIFFERENT METRICS, EACH WITH A DIFFERENT inner split:
 *
 *   $main.split($brand)                         // common grain G = brand (MODE 2)
 *     .apply('AvgByRef',  avg over (avg pvp resplit-by reference))
 *     .apply('MaxByComp', max over (max price resplit-by competitor))
 *
 * Each (metric, its resplit split) is a LEG. Legs sharing the SAME resplit split
 * merge into ONE sub-query (the existing nested-CTE path, unchanged). 2+ DISTINCT
 * resplit splits is the new path: one sub-query per distinct split, folded in
 * memory at the common grain G (the outer split, or ∅ for totals).
 *
 * This module is the PURE classifier: it partitions the applies into legs keyed
 * by their distinct resplit SplitExpression, segregates each leg's outer
 * aggregate into homomorphic leaf channels (so AVG carries SUM/COUNT and is
 * recombined post-join, never averaged-of-averages), and decides whether the
 * query is foldable or must be rejected (gates). The External cloning and the
 * in-memory fold itself live in the SQLExternal host, which owns valueOf /
 * fromValue and the Dataset join primitives.
 */

/** One leg: a distinct resplit split plus the leaf-channel applies that feed it. */
export interface PerMetricLeg {
  /** The distinct resplit SplitExpression for this leg (null for a plain G/scalar leg). */
  resplitSplit: SplitExpression | null;
  /** A stable signature of the resplit split, for grouping/diagnostics. */
  splitKey: string;
  /**
   * The applies this leg's sub-query carries. For an avg leg these are the
   * segregated leaf channels (`!T_k = sum/count`); for sum/min/max and plain
   * companions they are the originals kept whole.
   */
  legApplies: ApplyExpression[];
}

export interface PerMetricSplitPlan {
  legs: PerMetricLeg[];
  /**
   * Post-aggregate recombination applies, replayed IN MEMORY after the legs are
   * folded to G grain: avg ratios rebuilt from their SUM/COUNT channels, plus any
   * user-declared cross-leg derived measures (e.g. `Ratio = AvgByRef / MaxByComp`).
   * The synthetic `!T_*` leaf columns are dropped afterwards by the host.
   */
  postAggregateApplies: ApplyExpression[];
  /**
   * Per leaf-channel column → re-aggregation trait, used by
   * reAggregateToSplitGrain when a leg is finer than G. Avg channels resolve to
   * 'sum'; min/max keep their trait.
   */
  reAggApplyTraits: Record<string, 'sum' | 'min' | 'max' | 'none'>;
}

/**
 * Thrown when a per-metric query cannot be folded: a non-decomposable measure
 * ('none' trait — countDistinct/quantile/collect/mode/sqlAggregate) carried as a
 * finer-than-G leg, or genuinely disjoint splits (MODE 3). The message is kept
 * compatible with the historical "same split" / "use JOIN" gate phrasing so the
 * caller's contract (and existing tests) keep matching.
 */
export class PerMetricSplitRejection extends Error {}

/*
 * RETIRED: `selectActivePostAggregates(postAggregateApplies, sampleRow)`.
 *
 * It decided keep-vs-rebuild for the WHOLE result from a SINGLE sample row
 * (`joined.data[0]`). A full/left join sorts rows by key, so an orphan with null
 * `!T_*` channels could land at data[0] and globally drop the avg measure (and
 * leak scaffolding). The decision must be PER ROW, so it was superseded by
 * `External.applyPerRowPostAggregateRecombination` (baseExternal.ts:~1763) —
 * now the ONE recombination primitive for BOTH the per-metric fold
 * (sqlExternal.ts:~466) and the cross-source fold (baseExternal.ts:~3825). The
 * function had no remaining caller and is removed here to keep that single
 * source of truth honest. The divergence is pinned by the GREEN primitive specs
 * in simulateDruidAvgDecompositionExhaustive / simulateDruidCrossSourceAvgRecombination.
 */

/**
 * Build the per-metric plan, or return null when this is NOT a distinct-multi-
 * split query (0 or 1 distinct resplit splits → the existing single-CTE path
 * handles it unchanged, so we stay out of the way).
 *
 * @param applies            the external's applies (already derived-attr inlined)
 * @param parseResplitAgg    SQLExternal.parseResplitAgg (resplit detector)
 * @param segregate          External.segregationAggregateApplies (leaf channels)
 * @param resolveTrait       External.resolveApplyDecomposeTrait (decomposability)
 */
export function buildPerMetricSplitPlan(
  applies: ApplyExpression[],
  parseResplitAgg: (ex: Expression) => { resplitSplit: SplitExpression } | null,
  segregate: (applies: ApplyExpression[]) => {
    aggregateApplies: ApplyExpression[];
    postAggregateApplies: ApplyExpression[];
  },
  resolveTrait: (apply: ApplyExpression) => 'sum' | 'min' | 'max' | 'none',
): PerMetricSplitPlan | null {
  if (!applies || applies.length === 0) return null;

  // Resolve the single resplit split (if any) of an apply whose value is exactly
  // ONE aggregate (a leaf channel). The leaf was minted by global segregation, so
  // it carries at most one resplit; plain G/scalar companions carry none.
  const splitKeyOf = (apply: ApplyExpression): { split: SplitExpression | null; key: string } => {
    let found: SplitExpression | null = null;
    apply.expression.forEach(ex => {
      if (found) return;
      if (ex.isAggregate && ex.isAggregate()) {
        const r = parseResplitAgg(ex);
        if (r) found = r.resplitSplit;
      }
    });
    return { split: found, key: found ? found.toString() : '∅' };
  };

  // Early distinct-split count on the ORIGINAL applies: engage ONLY when 2+
  // distinct resplit splits are present. With 0 or 1 the existing single-CTE
  // lowering is correct and must be left untouched (the no-regression sentinels
  // pin this). Counting here — before segregation — avoids minting channels for
  // a query that should not take this path at all.
  const originalDistinct: Record<string, true> = {};
  for (const apply of applies) {
    const { split, key } = splitKeyOf(apply);
    if (split) originalDistinct[key] = true;
  }
  if (Object.keys(originalDistinct).length < 2) return null;

  // GLOBAL segregate-then-recombine across ALL applies at once. This mints every
  // homomorphic leaf channel exactly once (a shared aggregate — e.g. a max reused
  // by both a standalone measure and a derived ratio — is deduped to one column),
  // and produces the post-aggregate recombination referencing those channels. Avg
  // is rewritten to SUM/COUNT first so it carries two channels, never averaged of
  // averages. Crucially each leaf channel now carries AT MOST ONE resplit split,
  // so it can be assigned to its leg unambiguously.
  const decomposed = applies.map(a => a.changeExpression(a.expression.decomposeAverage()));
  const seg = segregate(decomposed);

  // Assign each leaf channel to its leg by its OWN resplit split. Plain G/scalar
  // companions (no resplit) collect under the '∅' bucket (one G-grain leg).
  const legApplyBuckets: {
    key: string;
    split: SplitExpression | null;
    applies: ApplyExpression[];
  }[] = [];
  const bucketIndex: Record<string, number> = {};
  const reAggApplyTraits: Record<string, 'sum' | 'min' | 'max' | 'none'> = {};

  for (const leaf of seg.aggregateApplies) {
    const { split, key } = splitKeyOf(leaf);

    // GATE: a non-decomposable ('none' trait) measure on a FINER-than-G leg
    // cannot be reconstructed from one scalar per sub-group. Two ways it shows:
    //   (a) the leaf's OWN outer reducer is 'none' (e.g. a bare countDistinct), or
    //   (b) a 'none'-trait aggregate sits INSIDE the leg's resplit (e.g.
    //       `split($competitor).apply(D, countDistinct($seller)).sum($D)` — the
    //       outer sum is 'sum', but summing per-competitor distinct counts is a
    //       silent wrong lowering across distinct splits).
    // (A 'none' measure whose split == G needs no collapse → allowed; only the
    // finer-than-G case is rejected.) Keep the historical phrasing so callers
    // still match (the gate specs assert /same split|not re-aggregable|JOIN/).
    const trait = resolveTrait(leaf);
    reAggApplyTraits[leaf.name] = trait;
    let hasInnerNonDecomposable = false;
    leaf.expression.forEach(ex => {
      // Skip two op kinds whose 'none' trait does NOT make the leg unfoldable:
      //   - `split`: the resplit mechanism itself, not a measure.
      //   - `average`: always rewritten to SUM/COUNT (here) or computed directly
      //     in the inner CTE per sub-key — a handled, value-exact case.
      // Everything else with a 'none' trait is genuinely opaque (countDistinct,
      // quantile, mode, collect, sqlAggregate) and cannot be losslessly summed
      // across distinct sub-groups → a silent wrong lowering if folded.
      if (ex.op !== 'split' && ex.op !== 'average' && ex.isAggregate && ex.isAggregate()) {
        const ctor = (Expression as any).classMap[ex.op];
        if (ctor && ctor.decomposable === 'none') hasInnerNonDecomposable = true;
      }
    });
    if ((trait === 'none' || hasInnerNonDecomposable) && split) {
      throw new PerMetricSplitRejection(
        `Per-metric split: measure on a finer-than-grain leg is not re-aggregable ` +
          `(${leaf.expression.op}); compute it in a separate query/JOIN.`,
      );
    }

    let idx = bucketIndex[key];
    if (idx === undefined) {
      idx = legApplyBuckets.length;
      bucketIndex[key] = idx;
      legApplyBuckets.push({ key, split, applies: [] });
    }
    legApplyBuckets[idx].applies.push(leaf);
  }

  const legs: PerMetricLeg[] = legApplyBuckets.map(b => ({
    resplitSplit: b.split,
    splitKey: b.key,
    legApplies: b.applies,
  }));

  // Collapse common sub-expressions across the recombination applies so a
  // DERIVED measure references the FINISHED measure it builds on, not that
  // measure's raw `!T_*` channels. Example: segregation yields
  //   AvgByRef = !T_0 / !T_1
  //   Ratio    = !T_0 / !T_1 / MaxByComp
  // — rewrite Ratio's `(!T_0 / !T_1)` sub-tree to `$AvgByRef`, giving
  //   Ratio = $AvgByRef / $MaxByComp.
  // This makes the recombination correct under BOTH transports: when the leg
  // returned the SUM/COUNT channels, `AvgByRef` is rebuilt first and `Ratio`
  // reads it; when the leg returned the FINISHED measure column instead (channels
  // null), the channel-based avg rebuild is skipped but `Ratio` still resolves
  // from the finished `$AvgByRef` / `$MaxByComp` columns.
  const post = seg.postAggregateApplies;
  const collapsed: ApplyExpression[] = [];
  for (let i = 0; i < post.length; i++) {
    let expr = post[i].expression;
    for (let j = 0; j < i; j++) {
      const earlier = post[j];
      const earlierBody = earlier.expression.toString();
      expr = expr.substitute(ex => {
        if (ex.toString() === earlierBody) return $(earlier.name, ex.type);
        return null;
      });
    }
    collapsed.push(post[i].changeExpression(expr));
  }

  return { legs, postAggregateApplies: collapsed, reAggApplyTraits };
}
