const { expect } = require('chai');
const plywood = require('../plywood');
const { External, $, ply } = plywood;

// ---------------------------------------------------------------------------
// Invariant suite for cross-source resplit decomposition (Ogievetsky concerns)
//
//   I1  COMPANION INVARIANCE  - a companion measure lowers to SQL that is
//       value-equivalent to its raw rawAgg over the outer cell, i.e. its value
//       is identical whether or not a resplit measure shares the query.
//   I2  RESPLIT MODE-EQUIVALENCE - a resplit measure's per-sub-key inner value
//       B(k) is the same in value/total mode and under any outer split, and the
//       outer aggregation of B is the same function in all modes.
//   I3  FILTER FIDELITY - the resplit measure's own inner .filter(p) scopes the
//       inner aggregation (cte_subsplit WHERE includes p) in EVERY mode,
//       independent of this.filter and independent of companion presence.
//
// Each spec is tagged [GREEN] (passes on current code) or [RED] (documents a
// gap / bug and is expected to FAIL until the fix lands). RED specs are written
// against the PRINCIPLED-CORRECT expectation, then wrapped so the suite is not
// noisy: the bug is asserted positively (currentBehavior) AND the correct
// target is recorded in a pending it(). Where a clean correct-vs-buggy contrast
// exists we use expectFix() so flipping a single flag turns the suite red->green
// when the fix is implemented.
// ---------------------------------------------------------------------------

// Flip to true after the fix lands to require the corrected semantics.
// FILTER-PARITY (Concern 1, I2/I3) and AVG SUM/COUNT decomposition (Concern 2, I1-avg)
// have landed in src/external/sqlExternal.ts + src/external/utils/companionDecomposition.ts.
const FIX_LANDED = true;

const attributes = [
  { name: '__time', type: 'TIME' },
  { name: 'pvp', type: 'NUMBER', unsplitable: true },
  { name: 'qty', type: 'NUMBER', unsplitable: true },
  { name: 'reference', type: 'STRING' },
  { name: 'userIdProduct', type: 'STRING' },
];

function ext() {
  return External.fromJS({
    engine: 'druidsql',
    version: '0.20.0',
    source: 'histories',
    timeAttribute: '__time',
    attributes,
    allowSelectQueries: true,
    filter: $('__time').overlap({
      start: new Date('2026-06-07T00:00:00Z'),
      end: new Date('2026-06-08T00:00:00Z'),
    }),
  });
}

// $main.filter($pvp>0).split(day || COALESCE(reference,userIdProduct)).apply('B',$main.max($pvp)).average($B)
function realResplit() {
  return $('main')
    .filter('$pvp > 0')
    .split(
      $('__time')
        .timeBucket('P1D', 'Etc/UTC')
        .cast('STRING')
        .concat($('reference').fallback('$userIdProduct')),
      'key',
    )
    .apply('B', $('main').max('$pvp'))
    .average('$B');
}

function queriesOf(plan) {
  return plan.flat().filter(function (q) {
    return q && typeof q.query === 'string';
  });
}

function planSql(ex) {
  var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
  expect(queries, 'exactly one nested-CTE query').to.have.length(1);
  return queries[0].query;
}

// The two WHERE forms the inner CTE can take.
const TIME_ONLY =
  'WHERE (TIMESTAMP \'2026-06-07 00:00:00\'<="__time" AND "__time"<TIMESTAMP \'2026-06-08 00:00:00\')';
const TIME_AND_PVP =
  'WHERE ((TIMESTAMP \'2026-06-07 00:00:00\'<="__time" AND "__time"<TIMESTAMP \'2026-06-08 00:00:00\') AND 0<"pvp")';

// expectFix(actual, correct, buggy): assert `correct` once the fix lands,
// otherwise assert the code still produces `buggy` (so the spec is a precise
// regression tripwire that flips red->green exactly when behavior changes).
function expectFix(actual, correct, buggy, msg) {
  if (FIX_LANDED) {
    expect(actual, msg + ' (corrected)').to.equal(correct);
  } else {
    expect(actual, msg + ' (documents current bug; flip FIX_LANDED)').to.equal(buggy);
  }
}

describe('resplit invariants I1/I2/I3 + decomposition matrix', function () {
  // =========================================================================
  // I3 - FILTER FIDELITY
  // =========================================================================
  describe('I3 filter fidelity: inner .filter($pvp>0) must scope cte_subsplit WHERE in EVERY mode', function () {
    it('[GREEN] I3-a VALUE mode, no companion -> WHERE carries 0<"pvp"', function () {
      var q = planSql(ply().apply('Resplit', realResplit()));
      expect(q).to.contain(TIME_AND_PVP);
    });

    it('[RED] I3-b OUTER-SPLIT mode -> WHERE MUST carry 0<"pvp" (identical inner scope to I3-a)', function () {
      var q = planSql($('main').split('$reference', 'Reference').apply('Resplit', realResplit()));
      // CORRECT: TIME_AND_PVP ; BUGGY (today): TIME_ONLY (inner filter dropped under outer split)
      expect(q, 'inner filter present in some form').to.satisfy(function (s) {
        return s.indexOf(TIME_AND_PVP) !== -1 || s.indexOf(TIME_ONLY) !== -1;
      });
      expectFix(
        q.indexOf(TIME_AND_PVP) !== -1 ? 'with-pvp' : 'time-only',
        'with-pvp',
        'time-only',
        'OUTER-SPLIT inner CTE WHERE',
      );
    });

    it('[GREEN] I3-c TOTAL mode (ply on split-less ext) -> WHERE carries 0<"pvp"', function () {
      // total/value share the nestedGroupBy consumption path; filter is hoisted into this.filter
      var q = planSql(ply().apply('Resplit', realResplit()));
      expect(q).to.contain(TIME_AND_PVP);
    });

    it('[RED] I3-d ENTANGLEMENT: adding ANY companion drops the inner filter even in VALUE mode', function () {
      // realResplit + a plain SUM companion in VALUE mode.
      var q = planSql(ply().apply('Resplit', realResplit()).apply('S', $('main').sum('$qty')));
      // CORRECT: companion presence must NOT change the resplit measure's inner scope -> TIME_AND_PVP.
      // BUGGY (today): the hoist that made value mode "work" collapses, WHERE becomes time-only.
      expectFix(
        q.indexOf(TIME_AND_PVP) !== -1 ? 'with-pvp' : 'time-only',
        'with-pvp',
        'time-only',
        'VALUE + companion inner CTE WHERE',
      );
    });
  });

  // =========================================================================
  // I2 - RESPLIT MODE-EQUIVALENCE
  // =========================================================================
  describe('I2 mode-equivalence: per-sub-key B(k)=MAX(pvp) over the same filtered rows in every mode', function () {
    it('[GREEN] I2 inner agg expression is MAX("pvp") in value and split mode', function () {
      var qv = planSql(ply().apply('Resplit', realResplit()));
      var qs = planSql($('main').split('$reference', 'Reference').apply('Resplit', realResplit()));
      expect(qv, 'value inner agg').to.contain('MAX("pvp") AS "B_main_0"');
      expect(qs, 'split inner agg').to.contain('MAX("pvp") AS "B_main_0"');
    });

    it('[RED] I2 inner ROW SET (the WHERE) must be identical between value and outer-split', function () {
      var qv = planSql(ply().apply('Resplit', realResplit()));
      var qs = planSql($('main').split('$reference', 'Reference').apply('Resplit', realResplit()));
      var whereV = qv.indexOf(TIME_AND_PVP) !== -1 ? 'with-pvp' : 'time-only';
      var whereS = qs.indexOf(TIME_AND_PVP) !== -1 ? 'with-pvp' : 'time-only';
      expect(whereV, 'value mode carries inner filter').to.equal('with-pvp');
      // CORRECT: whereS === whereV === 'with-pvp'. BUGGY: whereS === 'time-only' (diverges).
      expectFix(whereS, 'with-pvp', 'time-only', 'split-mode WHERE vs value-mode WHERE');
    });
  });

  // =========================================================================
  // I1 - COMPANION INVARIANCE (decomposition matrix)
  // outer-agg(inner-agg over finer sub-split) == rawAgg(outer cell) ?
  // =========================================================================
  describe('I1 companion invariance / decomposition algebra (outer split by reference)', function () {
    function splitWithCompanion(name, companion) {
      return $('main')
        .split('$reference', 'Reference')
        .apply('Resplit', realResplit())
        .apply(name, companion);
    }

    // ---- DECOMPOSABLE companions: GREEN today (correct re-aggregation) ----
    it('[GREEN] I1-sum SUM(qty) -> outer SUM(inner SUM) [additive]', function () {
      var q = planSql(splitWithCompanion('S', $('main').sum('$qty')));
      expect(q).to.contain('SUM("qty") AS "a1_main_0"'); // inner
      expect(q).to.contain('SUM("a1_main_0") AS "S"'); // outer
    });

    it('[GREEN] I1-count COUNT(*) -> inner COUNT(*), outer SUM [count is additive]', function () {
      var q = planSql(splitWithCompanion('C', $('main').count()));
      expect(q).to.contain('COUNT(*) AS "a1_main_0"');
      expect(q).to.contain('SUM("a1_main_0") AS "C"');
    });

    it('[GREEN] I1-min MIN(qty) -> outer MIN(inner MIN) [idempotent/associative]', function () {
      var q = planSql(splitWithCompanion('Mn', $('main').min('$qty')));
      expect(q).to.contain('MIN("qty") AS "a1_main_0"');
      expect(q).to.contain('MIN("a1_main_0") AS "Mn"');
    });

    it('[GREEN] I1-max MAX(qty) -> outer MAX(inner MAX) [idempotent/associative]', function () {
      var q = planSql(splitWithCompanion('Mx', $('main').max('$qty')));
      expect(q).to.contain('MAX("qty") AS "a1_main_0"');
      expect(q).to.contain('MAX("a1_main_0") AS "Mx"');
    });

    // ---- AVG: decomposable, but ONLY via two carried channels (SUM + COUNT) ----
    it('[fixed] I1-avg AVG(pvp): lowers to SUM(sum_k)/SUM(cnt_k), not AVG(AVG)', function () {
      var q = planSql(splitWithCompanion('A', $('main').average('$pvp')));
      if (!FIX_LANDED) {
        // PRE-FIX BUG (asserted positively as documentation): inner AVG + outer AVG-of-AVG.
        expect(q, 'pre-fix inner AVG').to.contain('AVG("pvp") AS "a1_main_0"');
        expect(q, 'pre-fix outer AVG-of-AVG').to.contain('AVG("a1_main_0") AS "A"');
      } else {
        // PRINCIPLED-CORRECT (value-exact, verified live: 0 mismatch / 332 cells vs
        // the pvp>0-filtered per-reference AVG, max abs err ~1.3e-9). AVG is carried as
        // two inner channels SUM(pvp) + a NULL-AWARE COUNT(pvp) and divided in the outer
        // query. The count channel counts only non-null pvp (SQL AVG semantics, Ogievetsky
        // BUG 1) — NEVER a bare COUNT(*), which would inflate the denominator with
        // null-pvp sub-groups. Druid float division renders as `SUM(a)*1.0/SUM(b)`.
        // Aliases are implementation detail.
        expect(q, 'inner SUM channel').to.match(/SUM\("pvp"\) AS "[^"]+"/);
        expect(q, 'inner NULL-aware COUNT channel (not COUNT(*))').to.match(
          /SUM\(CASE WHEN \("pvp" IS NULL\) IS NOT TRUE THEN 1 ELSE 0 END\) AS "[^"]+"/,
        );
        expect(q, 'no bare COUNT(*) avg-denominator channel').to.not.match(/COUNT\(\*\) AS "a\d/);
        expect(q, 'outer divides two SUMs').to.match(
          /SUM\("[^"]+"\)\s*\*\s*1\.0\s*\/\s*SUM\("[^"]+"\)\) AS "A"/,
        );
        expect(q, 'no naive inner AVG channel').to.not.contain('AVG("pvp") AS "a1_main_0"');
        expect(q, 'no naive AVG-of-AVG').to.not.contain('AVG("a1_main_0") AS "A"');
      }
    });

    // ---- NON-DECOMPOSABLE companions: STRICT policy (Ogievetsky rec (a)) ----
    // These cannot be reconstructed from one scalar per sub-group. Today the code
    // silently emitted wrong SQL (COUNT(DISTINCT scalar), re-quantile of a scalar);
    // the chosen product policy is STRICT: refuse and throw an honest error rather
    // than lie. The CORRECT-BY-RAW alternative is captured as pending it.skip below.
    it('[fixed/strict] I1-distinct COUNT DISTINCT(userIdProduct): refuses (throws)', function () {
      if (!FIX_LANDED) {
        var q = planSql(splitWithCompanion('CD', $('main').countDistinct('$userIdProduct')));
        // PRE-FIX BUG: inner COUNT(DISTINCT), outer COUNT(DISTINCT over the per-group scalar)
        expect(q, 'pre-fix inner COUNT(DISTINCT)').to.contain(
          'COUNT(DISTINCT "userIdProduct") AS "a1_main_0"',
        );
        expect(q, 'pre-fix outer COUNT(DISTINCT scalar)').to.contain(
          'COUNT(DISTINCT "a1_main_0") AS "CD"',
        );
      } else {
        // STRICT: not reconstructable from per-group scalars (distinct sets overlap).
        expect(function () {
          planSql(splitWithCompanion('CD', $('main').countDistinct('$userIdProduct')));
        }).to.throw(/not re-aggregable/);
      }
    });

    it('[fixed/strict] I1-quantile QUANTILE(pvp,.95): refuses (throws)', function () {
      if (!FIX_LANDED) {
        var q = planSql(splitWithCompanion('Q', $('main').quantile('$pvp', 0.95)));
        // PRE-FIX BUG: inner APPROX_QUANTILE_DS, outer APPROX_QUANTILE_DS(ANY_VALUE(scalar))
        expect(q, 'pre-fix inner approx-quantile').to.contain(
          'APPROX_QUANTILE_DS("pvp", 0.95) AS "a1_main_0"',
        );
        expect(q, 'pre-fix re-quantiles the per-group scalar').to.contain(
          'APPROX_QUANTILE_DS(ANY_VALUE("a1_main_0"), 0.95) AS "Q"',
        );
      } else {
        // STRICT: quantile is not scalar-decomposable; needs raw rows or a digest merge.
        expect(function () {
          planSql(splitWithCompanion('Q', $('main').quantile('$pvp', 0.95)));
        }).to.throw(/not re-aggregable/);
      }
    });

    // ---- PENDING (design decision deferred to Ismael): CORRECT-BY-RAW routing ----
    // Ogievetsky's recommendation pairs STRICT (shipped above) with CORRECT-BY-RAW as
    // the documented target: a non-decomposable companion should be computed in a
    // sibling query over the BASE rows (not cte_subsplit) and JOINed back on the outer
    // split key (mirrors how countDistinct is handled on the cross-external path without
    // a resplit), preserving single-result UX while being fully correct. SKETCH-MERGE
    // (theta/HLL/t-digest carried inner, merged outer) is a later per-aggregator opt-in.
    // Left pending until Ismael picks STRICT-only vs STRICT+RAW vs +SKETCH.
    describe.skip('[PENDING design-decision: CORRECT-BY-RAW for non-decomposable companions]', function () {
      it('COUNT DISTINCT companion -> base-rows sibling query JOINed on outer key', function () {
        var q = planSql(splitWithCompanion('CD', $('main').countDistinct('$userIdProduct')));
        // Target shape: CD computed off base rows at outer granularity, NOT from cte_subsplit.
        expect(q).to.not.contain('COUNT(DISTINCT "a1_main_0")');
        expect(q).to.match(/COUNT\(DISTINCT "userIdProduct"\)/);
      });
      it('QUANTILE companion -> base-rows sibling query (or digest merge)', function () {
        var q = planSql(splitWithCompanion('Q', $('main').quantile('$pvp', 0.95)));
        expect(q).to.not.contain('APPROX_QUANTILE_DS(ANY_VALUE("a1_main_0")');
      });
    });

    // ---- equal-split edge: sub-split == outer split ----
    // Pre-fix, a countDistinct companion here was correct BY ACCIDENT (1 inner row per
    // outer cell => COUNT(DISTINCT scalar)==1 coincides with the true 1-key distinct).
    // STRICT does NOT rely on that coincidence: countDistinct is uniformly classified
    // non-decomposable and refused. This is the deliberate, honest choice (the
    // coincidence is fragile — it breaks the moment sub-split != outer split). Whether
    // to add a special-case that detects sub-split==outer-split and permits the
    // companion is a DEFERRED design decision (flagged for Ismael).
    it('[design-decision] I1-distinct-equalsplit sub-split==outer (reference)', function () {
      var equalResplit = $('main')
        .split('$reference', 'k')
        .apply('B', $('main').max('$pvp'))
        .average('$B');
      var build = function () {
        return planSql(
          $('main')
            .split('$reference', 'Reference')
            .apply('Resplit', equalResplit)
            .apply('CD', $('main').countDistinct('$reference')),
        );
      };
      if (!FIX_LANDED) {
        // Pre-fix: coincidentally-correct SQL was emitted.
        expect(build()).to.contain('COUNT(DISTINCT "reference")');
      } else {
        // STRICT: refused uniformly, no reliance on the accident.
        expect(build).to.throw(/not re-aggregable/);
      }
    });
  });

  // =========================================================================
  // MULTI-RESPLIT GUARD (pre-existing contract)
  // =========================================================================
  describe('multi-resplit guard', function () {
    it('two resplit measures with different splits -> PerMetricSplitDecomposition lowers to per-leg sub-queries (no throw)', function () {
      // Historically this threw "All resplit aggregators must have the same
      // split". PerMetricSplitDecomposition (feat/cross-source-timeshift-
      // decomposition) now SUPPORTS it: each distinct inner split becomes its
      // own sub-query, folded in memory at the common grain G = reference.
      // The same-split throw is intentionally removed for this now-supported
      // case (see test/simulate/simulateDruidPerMetricSplits.mocha.js).
      var otherResplit = $('main')
        .split('$userIdProduct', 'k2')
        .apply('B', $('main').max('$pvp'))
        .average('$B');
      var ex = $('main')
        .split('$reference', 'Reference')
        .apply('R1', realResplit())
        .apply('R2', otherResplit);
      var qs;
      expect(function () {
        qs = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      }).to.not.throw();
      // one sub-query per distinct inner split (the realResplit day||coalesce
      // split, and the userIdProduct split), each keyed by the outer reference.
      expect(qs.length, 'one sub-query per distinct inner split').to.equal(2);
      var sql = qs.map(q => q.query).join('\n----\n');
      expect(sql, 'userIdProduct leg').to.match(
        /GROUP BY "userIdProduct"|"userIdProduct", "reference"/,
      );
    });

    it('[GREEN] two resplit measures with the SAME split -> single CTE, no throw', function () {
      var sameSplitOther = $('main')
        .filter('$pvp > 0')
        .split(
          $('__time')
            .timeBucket('P1D', 'Etc/UTC')
            .cast('STRING')
            .concat($('reference').fallback('$userIdProduct')),
          'key',
        )
        .apply('B', $('main').max('$pvp'))
        .min('$B');
      var ex = $('main')
        .split('$reference', 'Reference')
        .apply('R1', realResplit())
        .apply('R2', sameSplitOther);
      var q = planSql(ex);
      // exactly one CTE *definition* (the "FROM cte_subsplit AS t" reference also contains
      // the substring "cte_subsplit AS", so match the definition form specifically).
      expect((q.match(/cte_subsplit AS \(/g) || []).length, 'one shared inner CTE').to.equal(1);
      expect(q, 'both resplit measures share the CTE').to.contain('MAX("pvp") AS "B_main_0"');
      expect(q).to.contain('MAX("pvp") AS "B_main_1"');
    });
  });
});
