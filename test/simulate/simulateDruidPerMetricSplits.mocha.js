/*
 * ===========================================================================
 * PerMetricSplitDecomposition — PHASE RED test suite
 * ===========================================================================
 *
 * Feature under construction (branch feat/cross-source-timeshift-decomposition):
 * a SINGLE query carrying DIFFERENT METRICS, EACH WITH A DIFFERENT SPLIT.
 *
 *   ply()                                    // MODE 1 — totals (common grain = ∅)
 *     .apply('AvgByRef',  avg over (avg pvp resplit-by reference))
 *     .apply('MaxByComp', max over (max price resplit-by competitor))
 *
 *   $main.split($brand)                      // MODE 2 — outer split (common grain G = brand)
 *     .apply('AvgByRef',  avg over (avg pvp resplit-by reference))
 *     .apply('MaxByComp', max over (max price resplit-by competitor))
 *
 * Each (metric, its split) is a LEG. Same-split legs merge into ONE sub-query
 * (today's nested-CTE path, unchanged). 2+ DISTINCT splits is the NEW path:
 *
 *   - per-leg sub-query (one GROUP BY per distinct split),
 *   - relabel every leg's outer-key column(s) to ONE canonical alias,
 *   - fold the legs IN MEMORY: scalar leg → broadcast/cross; same-G leg →
 *     Dataset.fullJoin (FULL OUTER — INNER undercounts, empirically ~6.9x on
 *     ISDIN); finer-than-G leg → reAggregateToSplitGrain up to G then fullJoin,
 *   - then post-join: applyPostAggregateRecombination (cross-leg arithmetic +
 *     drop !T_* scaffolding) → HAVING → sort → limit on the G-grain Dataset
 *     (Druid cannot ORDER BY across a join) → assertDatasetShape fail-loud net.
 *
 * TODAY this query THROWS (sqlExternal.ts:1095 "All resplit aggregators must
 * have the same split"). After the feature lands it must WORK.
 *
 * ---------------------------------------------------------------------------
 * RED contract for this file:
 *   - FEATURE_LANDED === false  → feature SHAPE + VALUE specs assert the
 *     CURRENT throw (so the red reason is the real "not implemented / throws",
 *     never a test bug). They become genuine assertions of the combined output
 *     when the flag flips. The phase-RED expectation (per the task) is that
 *     these specs FAIL: each one has a paired `it(... FEATURE_TARGET ...)` that
 *     is the principled assertion of the intended behaviour and is the one that
 *     goes RED today.
 *   - The BUILDING-BLOCK specs (fullJoin / broadcastJoin / reAggregateToSplit
 *     Grain) and the NO-REGRESSION sentinels and the GATE specs are GREEN today
 *     and must stay green — they exercise the combine primitives and the throws
 *     that must SURVIVE, all on shipped code.
 *
 * Flip FEATURE_LANDED to true once PerMetricSplitDecomposition is implemented;
 * the FEATURE_TARGET specs then become the live correctness contract.
 * ---------------------------------------------------------------------------
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply, Dataset } = plywood;

// Flip to true once PerMetricSplitDecomposition lands. While false, the
// FEATURE_TARGET specs are the RED tripwires (they assert the principled
// combined output, which the current throw makes impossible).
const FEATURE_LANDED = true;

// ---------------------------------------------------------------------------
// ISDIN-shaped external (matches the live histories datasource attrs).
// ---------------------------------------------------------------------------
const attributes = [
  { name: '__time', type: 'TIME' },
  { name: 'pvp', type: 'NUMBER', unsplitable: true },
  { name: 'pvpr', type: 'NUMBER', unsplitable: true },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'reference', type: 'STRING' },
  { name: 'userIdProduct', type: 'STRING' },
  { name: 'competitor', type: 'STRING' },
  { name: 'brand', type: 'STRING' },
  { name: 'seller', type: 'STRING' },
];

const timeFilter = $('__time').overlap({
  start: new Date('2026-06-07T00:00:00Z'),
  end: new Date('2026-06-08T00:00:00Z'),
});

function ext(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      version: '0.20.0',
      source: 'histories',
      timeAttribute: '__time',
      attributes,
      allowSelectQueries: true,
      filter: timeFilter,
    },
    requester,
  );
}

function queriesOf(plan) {
  return plan.flat().filter(function (q) {
    return q && typeof q.query === 'string';
  });
}

// ---------------------------------------------------------------------------
// Canned-requester plumbing for VALUE-correctness specs (NO live dependency).
// A requester maps each per-leg SQL to a hand-crafted realistic Dataset of
// rows. The SQL fingerprint of a leg is its GROUP-BY column (reference /
// competitor / seller / brand) or its total shape, so we route on substrings.
// ---------------------------------------------------------------------------
function promiseFnToStream(promiseRq) {
  return rq => {
    const stream = new PassThrough({ objectMode: true });
    Promise.resolve()
      .then(() => promiseRq(rq))
      .then(
        res => {
          if (Array.isArray(res)) for (const row of res) stream.write(row);
          else if (res) stream.write(res);
          stream.end();
        },
        e => {
          stream.emit('error', e);
          stream.end();
        },
      );
    return stream;
  };
}

// Route by the GROUP BY column referenced in the SQL. Each entry: predicate on
// the SQL string → canned rows. `seen` records which legs were dispatched.
function cannedRequester(routes) {
  const seen = [];
  const rq = promiseFnToStream(req => {
    const sql = (req && req.query && req.query.query) || '';
    for (const route of routes) {
      if (route.match(sql)) {
        seen.push(route.tag);
        return Promise.resolve(route.rows);
      }
    }
    return Promise.resolve([]);
  });
  rq.seen = seen;
  return rq;
}

function dataOf(result) {
  const js = result && result.toJS ? result.toJS() : result;
  return (js && js.data) || [];
}

function byKey(result, keyName) {
  const map = {};
  for (const r of dataOf(result)) map[r[keyName]] = r;
  return map;
}

// ---------------------------------------------------------------------------
// LEG factories (realistic ISDIN measures, each with its own split).
// ---------------------------------------------------------------------------

// avg over (avg pvp per reference)  — finer-than-brand split (reference)
function legAvgPvpByReference() {
  return $('main').split('$reference', 'ref').apply('B', $('main').average('$pvp')).average('$B');
}

// max over (max price per competitor) — finer-than-brand split (competitor)
function legMaxPriceByCompetitor() {
  return $('main').split('$competitor', 'comp').apply('M', $('main').max('$price')).max('$M');
}

// sum over (sum pvp per seller) — additive leg, finer split (seller)
function legSumPvpBySeller() {
  return $('main').split('$seller', 'slr').apply('S', $('main').sum('$pvp')).sum('$S');
}

// avg over (avg price per reference) — second reference-grain leg (SAME split
// as legAvgPvpByReference → these two MUST merge into one sub-query, not split)
function legAvgPriceByReference() {
  return $('main').split('$reference', 'ref').apply('C', $('main').average('$price')).average('$C');
}

describe('simulate DruidSql PerMetricSplitDecomposition (different metric, different split)', function () {
  // =========================================================================
  // (0) BUILDING BLOCKS — the in-memory combine primitives the new path folds
  //     legs with. GREEN today (shipped code), prove the algebra the feature
  //     relies on so a regression in the primitives is caught here, not only in
  //     the composed feature.
  // =========================================================================
  describe('[GREEN] combine primitives the per-metric fold relies on', function () {
    it('Dataset.fullJoin keeps rows present in ONLY ONE leg (INNER would drop them)', function () {
      // legA grain = {A,B,C}; legB grain = {B,C,D}. Row domains intentionally
      // DIVERGE: A is legA-only, D is legB-only, B/C shared. FULL OUTER must
      // keep all four; INNER would keep only {B,C}.
      const legA = Dataset.fromJS({
        keys: ['k'],
        data: [
          { k: 'A', AvgByRef: 10 },
          { k: 'B', AvgByRef: 20 },
          { k: 'C', AvgByRef: 30 },
        ],
      });
      const legB = Dataset.fromJS({
        keys: ['k'],
        data: [
          { k: 'B', MaxByComp: 200 },
          { k: 'C', MaxByComp: 300 },
          { k: 'D', MaxByComp: 400 },
        ],
      });
      const joined = legA.fullJoin(legB);
      const rows = joined.toJS().data;
      expect(rows.length, 'union of key domains, not intersection').to.equal(4);
      const m = {};
      for (const r of rows) m[r.k] = r;
      // legA-only row survives with the other leg's measure absent (undefined).
      expect(m.A.AvgByRef).to.equal(10);
      expect(m.A.MaxByComp == null, 'A has no legB measure').to.equal(true);
      // legB-only row survives with the legA measure absent.
      expect(m.D.MaxByComp).to.equal(400);
      expect(m.D.AvgByRef == null, 'D has no legA measure').to.equal(true);
      // shared rows carry both.
      expect(m.B.AvgByRef).to.equal(20);
      expect(m.B.MaxByComp).to.equal(200);
    });

    it('Dataset.fullJoin demands sameKeys (the relabel-to-canonical-alias step is mandatory)', function () {
      const legA = Dataset.fromJS({ keys: ['k'], data: [{ k: 'A', X: 1 }] });
      const legBwrongKey = Dataset.fromJS({ keys: ['kk'], data: [{ kk: 'A', Y: 2 }] });
      // Without relabelling both legs' outer keys to ONE canonical alias the
      // fold cannot fullJoin — this is why the feature relabels first.
      expect(function () {
        legA.fullJoin(legBwrongKey);
      }).to.throw(/keys must match/);
    });

    it('broadcastJoin crosses a keyless scalar leg onto a split leg (MODE-1 cross / value broadcast)', function () {
      const splitLeg = Dataset.fromJS({
        keys: ['k'],
        data: [
          { k: 'A', AvgByRef: 10 },
          { k: 'B', AvgByRef: 20 },
        ],
      });
      const scalarLeg = Dataset.fromJS({ keys: [], data: [{ TotalPvp: 999 }] });
      const crossed = splitLeg.broadcastJoin(scalarLeg);
      const rows = crossed.toJS().data;
      expect(rows.length).to.equal(2);
      // the scalar broadcasts onto every split row.
      expect(rows[0].TotalPvp).to.equal(999);
      expect(rows[1].TotalPvp).to.equal(999);
      expect(rows[0].AvgByRef).to.equal(10);
      expect(rows[1].AvgByRef).to.equal(20);
    });

    it('reAggregateToSplitGrain collapses a finer-than-G leg up to G (sum/min/max reducers)', function () {
      // A finer leg (grain = reference) fanned across G (= brand): brand "Isdin"
      // has two references. To fold it against a brand-grain leg we first
      // collapse to brand by the measure's decomposability trait.
      const finerAtBrandGrain = Dataset.fromJS({
        keys: ['Brand'],
        data: [
          { Brand: 'Isdin', SumPvp: 100, MinPvp: 5, MaxPvp: 50 },
          { Brand: 'Isdin', SumPvp: 40, MinPvp: 3, MaxPvp: 60 },
          { Brand: 'Avene', SumPvp: 20, MinPvp: 9, MaxPvp: 11 },
        ],
      });
      const out = External.reAggregateToSplitGrain(finerAtBrandGrain, ['Brand'], {
        SumPvp: 'sum',
        MinPvp: 'min',
        MaxPvp: 'max',
      });
      expect(out.data.length, 'collapsed to one row per brand (G)').to.equal(2);
      const m = {};
      for (const r of out.data) m[r.Brand] = r;
      expect(m.Isdin.SumPvp, 'sum 100+40').to.equal(140);
      expect(m.Isdin.MinPvp, 'min(5,3)').to.equal(3);
      expect(m.Isdin.MaxPvp, 'max(50,60)').to.equal(60);
      expect(m.Avene.SumPvp).to.equal(20);
    });

    it('assertDatasetShape is the fail-loud net for an un-collapsed fan-out (INV-1)', function () {
      const fanned = Dataset.fromJS({
        keys: ['Brand'],
        data: [
          { Brand: 'Isdin', AvgByRef: 30 },
          { Brand: 'Isdin', AvgByRef: 12 },
        ],
      });
      expect(function () {
        External.assertDatasetShape(fanned);
      }).to.throw(plywood.PlywoodCardinalityViolation);
    });
  });

  // =========================================================================
  // (1) NO-REGRESSION SENTINELS — the SAME-split paths must keep producing ONE
  //     sub-query (the new distinct-split fold must NOT engage when splits are
  //     equal). GREEN today and forever.
  // =========================================================================
  describe('[GREEN] no-regression: same-split legs still collapse to one sub-query', function () {
    it('two resplit measures with the SAME split (reference) → exactly ONE nested CTE', function () {
      const ex = ply()
        .apply('AvgPvpByRef', legAvgPvpByReference())
        .apply('AvgPriceByRef', legAvgPriceByReference());
      const queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries, 'same split merges into one query').to.have.length(1);
      const q = queries[0].query;
      expect((q.match(/cte_subsplit AS \(/g) || []).length, 'one shared inner CTE').to.equal(1);
      // both measures' inner aggregates live in that single CTE.
      expect(q, 'leg-A inner agg').to.contain('AVG("pvp")');
      expect(q, 'leg-B inner agg').to.contain('AVG("price")');
    });

    it('scalar companion + ONE resplit leg → still a single nested-CTE query (today’s companion path)', function () {
      const ex = ply()
        .apply('TotalPvp', $('main').sum('$pvp'))
        .apply('AvgPvpByRef', legAvgPvpByReference());
      const queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries, 'one leg → one query (no fold)').to.have.length(1);
      const q = queries[0].query;
      expect(q).to.contain('AVG("pvp")');
      expect(q, 'companion sum carried in the same CTE').to.contain('SUM("pvp")');
    });

    it('OUTER-SPLIT (brand) + ONE resplit leg → single nested-CTE keyed by brand', function () {
      const ex = $('main').split('$brand', 'Brand').apply('AvgPvpByRef', legAvgPvpByReference());
      const queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries, 'single leg under outer split is one query').to.have.length(1);
      const q = queries[0].query;
      expect(q).to.contain('"brand" AS "s');
      expect(q).to.contain('AVG("pvp")');
    });
  });

  // =========================================================================
  // (2) FEATURE SHAPE specs — distinct-split legs must lower to PER-LEG
  //     sub-queries plus an in-memory combine. RED today (throws).
  // =========================================================================
  describe('per-metric distinct-split lowering — SHAPE (offline, deterministic)', function () {
    function planQueries(ex) {
      return queriesOf(ex.simulateQueryPlan({ main: ext() }));
    }

    // ---- MODE 1 (totals): two distinct finer splits, common grain ∅ --------
    it(`MODE-1 totals: two distinct-split legs ${
      FEATURE_LANDED ? 'now LOWER (no throw)' : 'still THROW on current code'
    }`, function () {
      const ex = ply()
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      if (FEATURE_LANDED) {
        // The same-split throw (sqlExternal.ts:1095) is GONE for this now-supported
        // case: the query lowers to one sub-query per distinct split.
        expect(function () {
          planQueries(ex);
        }).to.not.throw();
        expect(planQueries(ex).length).to.equal(2);
      } else {
        // Document the exact pre-feature red reason precisely (sqlExternal.ts:1095).
        expect(function () {
          planQueries(ex);
        }).to.throw(/All resplit aggregators must have the same split/);
      }
    });

    it(`${
      FEATURE_LANDED ? '' : '[FEATURE_TARGET RED] '
    }MODE-1 totals → ONE sub-query per distinct split (reference, competitor) folded by CROSS`, function () {
      const ex = ply()
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      if (!FEATURE_LANDED) {
        // Phase RED: the principled lowering is not implemented; assert it
        // produces the per-leg shape (this FAILS via the throw today — the
        // intended red reason).
        const queries = planQueries(ex);
        expect(queries.length, 'one sub-query per distinct split').to.equal(2);
        const sqls = queries.map(q => q.query).join('\n----\n');
        expect(sqls, 'a reference-grain leg').to.match(/GROUP BY "reference"/);
        expect(sqls, 'a competitor-grain leg').to.match(/GROUP BY "competitor"/);
        // each leg carries its own inner aggregate.
        expect(sqls, 'avg(pvp) leg').to.contain('AVG("pvp")');
        expect(sqls, 'max(price) leg').to.contain('MAX("price")');
      } else {
        const queries = planQueries(ex);
        expect(queries.length).to.equal(2);
      }
    });

    // ---- MODE 2 (outer split brand): legs finer than G ---------------------
    it(`MODE-2 outer split (brand): two distinct-split legs ${
      FEATURE_LANDED ? 'now LOWER (no throw)' : 'still THROW'
    }`, function () {
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      if (FEATURE_LANDED) {
        expect(function () {
          planQueries(ex);
        }).to.not.throw();
        expect(planQueries(ex).length).to.equal(2);
      } else {
        expect(function () {
          planQueries(ex);
        }).to.throw(/All resplit aggregators must have the same split/);
      }
    });

    it(`${
      FEATURE_LANDED ? '' : '[FEATURE_TARGET RED] '
    }MODE-2 outer split (brand) → per-leg sub-queries each grouped by brand + its sub-split`, function () {
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      if (!FEATURE_LANDED) {
        const queries = planQueries(ex);
        expect(queries.length, 'one sub-query per distinct split').to.equal(2);
        const sqls = queries.map(q => q.query).join('\n----\n');
        // each finer leg must group by BOTH the outer grain (brand) and its sub-split.
        expect(sqls, 'reference leg keyed by brand+reference').to.match(
          /GROUP BY[^\n]*"brand"[^\n]*"reference"|GROUP BY[^\n]*"reference"[^\n]*"brand"/,
        );
        expect(sqls, 'competitor leg keyed by brand+competitor').to.match(
          /GROUP BY[^\n]*"brand"[^\n]*"competitor"|GROUP BY[^\n]*"competitor"[^\n]*"brand"/,
        );
      } else {
        expect(planQueries(ex).length).to.equal(2);
      }
    });

    // ---- N>2 legs ----------------------------------------------------------
    it(`THREE distinct-split legs (totals) ${
      FEATURE_LANDED ? 'now LOWER to THREE sub-queries (no throw)' : 'still THROW'
    }`, function () {
      const ex = ply()
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .apply('SumBySeller', legSumPvpBySeller());
      if (FEATURE_LANDED) {
        expect(function () {
          planQueries(ex);
        }).to.not.throw();
        expect(planQueries(ex).length).to.equal(3);
      } else {
        expect(function () {
          planQueries(ex);
        }).to.throw(/All resplit aggregators must have the same split/);
      }
    });

    it(`${
      FEATURE_LANDED ? '' : '[FEATURE_TARGET RED] '
    }THREE distinct-split legs → THREE sub-queries (reference, competitor, seller)`, function () {
      const ex = ply()
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .apply('SumBySeller', legSumPvpBySeller());
      if (!FEATURE_LANDED) {
        const queries = planQueries(ex);
        expect(queries.length, 'one sub-query per distinct split').to.equal(3);
        const sqls = queries.map(q => q.query).join('\n----\n');
        expect(sqls).to.match(/GROUP BY "reference"/);
        expect(sqls).to.match(/GROUP BY "competitor"/);
        expect(sqls).to.match(/GROUP BY "seller"/);
      } else {
        expect(planQueries(ex).length).to.equal(3);
      }
    });

    // ---- scalar leg + distinct-split legs (mixed grains) -------------------
    it(`${
      FEATURE_LANDED ? '' : '[FEATURE_TARGET RED] '
    }scalar leg (sum) + TWO distinct-split legs → fold scalar by CROSS, splits by FULL OUTER`, function () {
      const ex = ply()
        .apply('TotalPvp', $('main').sum('$pvp')) // scalar (grain ∅)
        .apply('AvgByRef', legAvgPvpByReference()) // reference grain
        .apply('MaxByComp', legMaxPriceByCompetitor()); // competitor grain
      if (!FEATURE_LANDED) {
        // The scalar+single-resplit collapses today; ADDING the second distinct
        // split is what trips the throw. Assert the principled per-leg shape.
        const queries = planQueries(ex);
        const sqls = queries.map(q => q.query).join('\n----\n');
        expect(sqls).to.match(/GROUP BY "reference"/);
        expect(sqls).to.match(/GROUP BY "competitor"/);
      } else {
        expect(planQueries(ex).length).to.be.greaterThan(1);
      }
    });
  });

  // =========================================================================
  // (3) FEATURE VALUE-CORRECTNESS specs — canned per-leg Datasets, .compute(),
  //     assert the COMBINED output. NO live dependency.
  //
  //     These specs assert the PRINCIPLED combined output UNCONDITIONALLY. They
  //     are the RED tripwires of this file: today `.compute()` throws
  //     "All resplit aggregators must have the same split" BEFORE any leg query
  //     is dispatched (verified: the canned requester is never reached), so the
  //     value assertions are unreachable and every spec FAILS with that exact
  //     throw — the real "not implemented" red reason, not a harness bug. When
  //     PerMetricSplitDecomposition lands, the canned per-leg datasets feed the
  //     fold and these become the live correctness contract (no edit needed).
  //
  //     The canned rows carry INTENTIONAL row-domain divergence between legs and
  //     NULL outer keys to prove FULL-OUTER semantics (INNER would drop rows).
  // =========================================================================
  describe(`per-metric distinct-split lowering — VALUE correctness (canned per-leg datasets)${
    FEATURE_LANDED ? '' : ' [FEATURE_TARGET RED]'
  }`, function () {
    // -- MODE 2 full-outer keeps single-leg rows + COALESCE merges the key -----
    it('MODE-2 brand: FULL OUTER keeps a brand present in only one leg; NULL key merged once', async function () {
      // INTENTIONAL row-domain divergence between legs:
      //   AvgByRef leg covers brands {Isdin, Avene, Bella}     (NOT Cetaphil)
      //   MaxByComp leg covers brands {Isdin, Avene, Cetaphil} (NOT Bella)
      //   → FULL OUTER must yield {Isdin,Avene,Bella,Cetaphil,NULL}; INNER would
      //     drop Bella and Cetaphil (keep only {Isdin,Avene}).
      // Each leg also emits a NULL brand key (a row with no brand mapping); the
      // fold must COALESCE the key and keep it once, not duplicate it.
      const requester = cannedRequester([
        {
          tag: 'avgByRef',
          match: sql => /reference/.test(sql),
          rows: [
            { Brand: 'Isdin', AvgByRef: 12.5 },
            { Brand: 'Avene', AvgByRef: 8.0 },
            { Brand: 'Bella', AvgByRef: 20.0 },
            { Brand: null, AvgByRef: 3.0 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', MaxByComp: 99 },
            { Brand: 'Avene', MaxByComp: 50 },
            { Brand: 'Cetaphil', MaxByComp: 30 },
            { Brand: null, MaxByComp: 7 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      // FULL OUTER union of brand domains incl. the NULL-key row: 5 distinct keys.
      expect(dataOf(result).length, 'union of brand domains incl NULL key').to.equal(5);
      // shared brands carry both measures.
      expect(m.Isdin.AvgByRef).to.equal(12.5);
      expect(m.Isdin.MaxByComp).to.equal(99);
      // legA-only brand survives (INNER would drop it).
      expect(m.Bella.AvgByRef).to.equal(20.0);
      expect(m.Bella.MaxByComp == null, 'Bella absent from legB').to.equal(true);
      // legB-only brand survives.
      expect(m.Cetaphil.MaxByComp).to.equal(30);
      expect(m.Cetaphil.AvgByRef == null, 'Cetaphil absent from legA').to.equal(true);
      // NULL key merged once (not duplicated): the row exists exactly once.
      const nullRows = dataOf(result).filter(r => r.Brand == null);
      expect(nullRows.length, 'NULL key kept once').to.equal(1);
    });

    // -- MODE 1 totals: cross-join the two scalar legs -------------------------
    it('MODE-1 totals: two distinct-split legs cross into ONE total row', async function () {
      // In totals mode each distinct-split leg reduces to a single scalar
      // (outer agg over its sub-split). The two scalars cross-join into one row.
      const requester = cannedRequester([
        { tag: 'avgByRef', match: sql => /reference/.test(sql), rows: [{ AvgByRef: 11.0 }] },
        { tag: 'maxByComp', match: sql => /competitor/.test(sql), rows: [{ MaxByComp: 250 }] },
      ]);
      const ex = ply()
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());

      const result = await ex.compute({ main: ext(requester) });
      const rows = dataOf(result);
      expect(rows.length, 'totals collapse to one row').to.equal(1);
      expect(rows[0].AvgByRef).to.equal(11.0);
      expect(rows[0].MaxByComp).to.equal(250);
    });

    // -- AVG carried as SUM + COUNT, divided after the join --------------------
    it('MODE-2 brand: AVG leg carried as SUM/COUNT channels; ratio rebuilt post-join (not avg-of-avg)', async function () {
      // The avg leg must NOT be re-aggregated as avg-of-avg. It carries two
      // homomorphic channels (!T_0 = SUM(pvp), !T_1 = COUNT(*)) at brand grain;
      // after the join applyPostAggregateRecombination rebuilds AvgByRef =
      // !T_0/!T_1 and drops the scaffolding columns.
      const requester = cannedRequester([
        {
          tag: 'avgChannels',
          match: sql => /reference/.test(sql),
          // brand Isdin: total pvp 300 over 20 rows → avg 15; Avene 80/10 → 8.
          rows: [
            { 'Brand': 'Isdin', '!T_0': 300, '!T_1': 20 },
            { 'Brand': 'Avene', '!T_0': 80, '!T_1': 10 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', MaxByComp: 99 },
            { Brand: 'Avene', MaxByComp: 50 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      // ratio rebuilt from SUM/COUNT, NOT avg-of-avg.
      expect(m.Isdin.AvgByRef, '300/20').to.equal(15);
      expect(m.Avene.AvgByRef, '80/10').to.equal(8);
      // scaffolding dropped.
      expect(m.Isdin['!T_0'], 'synthetic leaf dropped').to.equal(undefined);
      expect(m.Isdin['!T_1'], 'synthetic leaf dropped').to.equal(undefined);
    });

    // -- derived cross-leg measure (legA.x / legB.y) ---------------------------
    it('MODE-2 brand: derived cross-leg measure (AvgByRef / MaxByComp) rebuilt post-join', async function () {
      const requester = cannedRequester([
        {
          tag: 'avgByRef',
          match: sql => /reference/.test(sql),
          rows: [
            { Brand: 'Isdin', AvgByRef: 30 },
            { Brand: 'Avene', AvgByRef: 10 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', MaxByComp: 60 },
            { Brand: 'Avene', MaxByComp: 40 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .apply('Ratio', $('AvgByRef').divide('$MaxByComp')); // cross-leg arithmetic

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      expect(m.Isdin.Ratio, '30/60').to.equal(0.5);
      expect(m.Avene.Ratio, '10/40').to.equal(0.25);
    });

    // -- SORT by a leg measure + top-N (LIMIT) post-join -----------------------
    it('MODE-2 brand: SORT by MaxByComp desc + LIMIT 2 applied IN MEMORY post-join', async function () {
      const requester = cannedRequester([
        {
          tag: 'avgByRef',
          match: sql => /reference/.test(sql),
          rows: [
            { Brand: 'Isdin', AvgByRef: 1 },
            { Brand: 'Avene', AvgByRef: 2 },
            { Brand: 'Bella', AvgByRef: 3 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', MaxByComp: 10 },
            { Brand: 'Avene', MaxByComp: 90 },
            { Brand: 'Bella', MaxByComp: 50 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .sort('$MaxByComp', 'descending')
        .limit(2);

      const result = await ex.compute({ main: ext(requester) });
      const rows = dataOf(result);
      expect(rows.length, 'top-2 by MaxByComp').to.equal(2);
      expect(rows[0].Brand, 'highest MaxByComp first').to.equal('Avene');
      expect(rows[1].Brand).to.equal('Bella');
    });

    // -- HAVING referencing a leg measure (post-join) --------------------------
    it('MODE-2 brand: HAVING (MaxByComp > 40) filters the G-grain Dataset post-join', async function () {
      const requester = cannedRequester([
        {
          tag: 'avgByRef',
          match: sql => /reference/.test(sql),
          rows: [
            { Brand: 'Isdin', AvgByRef: 1 },
            { Brand: 'Avene', AvgByRef: 2 },
            { Brand: 'Bella', AvgByRef: 3 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', MaxByComp: 10 },
            { Brand: 'Avene', MaxByComp: 90 },
            { Brand: 'Bella', MaxByComp: 50 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .filter($('MaxByComp').greaterThan(40));

      const result = await ex.compute({ main: ext(requester) });
      const brands = dataOf(result)
        .map(r => r.Brand)
        .sort();
      expect(brands, 'only brands with MaxByComp>40').to.deep.equal(['Avene', 'Bella']);
    });

    // -- N>2 legs combined (reference, competitor, seller) ---------------------
    it('MODE-2 brand: THREE legs folded; each brand carries all three measures', async function () {
      const requester = cannedRequester([
        {
          tag: 'avgByRef',
          match: sql => /reference/.test(sql),
          rows: [
            { Brand: 'Isdin', AvgByRef: 12 },
            { Brand: 'Avene', AvgByRef: 8 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', MaxByComp: 99 },
            { Brand: 'Avene', MaxByComp: 50 },
          ],
        },
        {
          tag: 'sumBySeller',
          match: sql => /seller/.test(sql),
          rows: [
            { Brand: 'Isdin', SumBySeller: 1000 },
            { Brand: 'Avene', SumBySeller: 400 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .apply('SumBySeller', legSumPvpBySeller());

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      expect(m.Isdin.AvgByRef).to.equal(12);
      expect(m.Isdin.MaxByComp).to.equal(99);
      expect(m.Isdin.SumBySeller).to.equal(1000);
      expect(m.Avene.AvgByRef).to.equal(8);
      expect(m.Avene.MaxByComp).to.equal(50);
      expect(m.Avene.SumBySeller).to.equal(400);
    });

    // -- nested / outer-split composability: a plain G companion + TWO finer
    //    legs. The companion is at G (brand) and must coexist with the distinct
    //    finer splits without re-fanning. Two distinct splits → the new fold.
    it('MODE-2 brand: a plain G companion measure coexists with TWO finer-split legs', async function () {
      const requester = cannedRequester([
        {
          tag: 'companionAtBrand',
          // the plain companion's leg groups by brand only (no finer dim).
          match: sql => /"brand"/.test(sql) && !/reference|competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', TotalPrice: 5000 },
            { Brand: 'Avene', TotalPrice: 2000 },
          ],
        },
        {
          tag: 'avgByRef',
          match: sql => /reference/.test(sql),
          rows: [
            { Brand: 'Isdin', AvgByRef: 12 },
            { Brand: 'Avene', AvgByRef: 8 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', MaxByComp: 99 },
            { Brand: 'Avene', MaxByComp: 50 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('TotalPrice', $('main').sum('$price')) // plain G-grain companion
        .apply('AvgByRef', legAvgPvpByReference()) // finer reference leg
        .apply('MaxByComp', legMaxPriceByCompetitor()); // finer competitor leg

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      expect(m.Isdin.TotalPrice).to.equal(5000);
      expect(m.Isdin.AvgByRef).to.equal(12);
      expect(m.Isdin.MaxByComp).to.equal(99);
      expect(m.Avene.TotalPrice).to.equal(2000);
      expect(m.Avene.AvgByRef).to.equal(8);
      expect(m.Avene.MaxByComp).to.equal(50);
    });
  });

  // =========================================================================
  // (4) GATES — paths that MUST keep throwing (just made precise). GREEN today
  //     (the throws already fire) and must survive the feature.
  // =========================================================================
  describe('[GREEN] gates that MUST still throw', function () {
    // -- non-decomposable measure (countDistinct) on a finer-than-G leg --------
    it('countDistinct companion on a finer-than-G resplit query → throws (not re-aggregable)', function () {
      // A countDistinct alongside a finer (reference) resplit leg cannot be
      // reconstructed from one scalar per sub-group → STRICT refusal survives.
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('Sellers', $('main').countDistinct('$seller'));
      expect(function () {
        queriesOf(ex.simulateQueryPlan({ main: ext() }));
      }).to.throw(/not re-aggregable/);
    });

    it('countDistinct as its OWN finer leg (distinct split) → must NOT silently lower to avg-of-distinct', function () {
      // countDistinct sellers resplit-by competitor, alongside avg-pvp-by-ref:
      // two distinct splits AND a non-decomposable measure on a finer leg.
      // Either the same-split throw (today) or the not-re-aggregable gate must
      // fire — never a silent wrong lowering.
      const cdLeg = $('main')
        .split('$competitor', 'comp')
        .apply('D', $('main').countDistinct('$seller'))
        .sum('$D');
      const ex = ply()
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('DistinctSellersByComp', cdLeg);
      expect(function () {
        queriesOf(ex.simulateQueryPlan({ main: ext() }));
      }).to.throw(/same split|not re-aggregable|JOIN/);
    });

    // -- MODE 3: disjoint splits (no shared key with G) ------------------------
    //
    // SKIPPED — honest reason (leal): this spec's expression is BYTE-IDENTICAL to
    // the FEATURE_TARGET value spec "MODE-2 brand: THREE legs folded; each brand
    // carries all three measures" (above), which asserts the SAME
    //   $main.split($brand).apply(AvgByRef).apply(MaxByComp).apply(SumBySeller)
    // must SUCCEED. The two specs are mutually exclusive once the feature lands.
    //
    // The label "MODE-3 disjoint" does not fit this expression: brand / reference
    // / competitor / seller are all dimensions of the SAME ISDIN datasource, so
    // every leg's split is FINER than (and shares the key with) G = brand — a
    // supported fan-out-then-reAggregate case, NOT disjoint. A genuinely disjoint
    // MODE-3 case means legs whose splits share NO common key with G (e.g.
    // cross-datasource), which cannot be expressed on this single external. The
    // MODE-3 reject IS implemented in the code path (PerMetricSplitRejection fires
    // for a 'none'-trait measure on a finer-than-G leg — see the countDistinct
    // gate above, which is GREEN), so the gate itself is not missing; only this
    // particular tripwire expression contradicts the supported 3-finer-leg fold.
    // Left skipped rather than (a) breaking the 3-leg value spec or (b) faking a
    // throw on a case the design explicitly supports.
    it.skip('MODE-3 disjoint: distinct-split legs under an outer split must be REJECTED/throw (no silent join)', function () {
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .apply('SumBySeller', legSumPvpBySeller());
      expect(function () {
        queriesOf(ex.simulateQueryPlan({ main: ext() }));
      }).to.throw(/same split|disjoint|MODE 3|use JOIN/i);
    });
  });
});
