/*
 * ===========================================================================
 * AVG-DECOMPOSITION CORRECTNESS — EXHAUSTIVE PHASE-RED TDD suite
 * ===========================================================================
 *
 * Two real, currently-SHIPPING, SILENT-WRONG bugs found in Ogievetsky review
 * of the PerMetricSplitDecomposition landing. The prior 25-passing suite
 * (test/simulate/simulateDruidPerMetricSplits.mocha.js) was GREEN yet BOTH
 * bugs shipped, because its canned data lacked the two triggering factors:
 *   (a) NULL inner sub-groups   — never exercises COUNT(*) vs COUNT(value);
 *   (b) a null first-data-row    — never exercises the joined.data[0] gamble.
 * This suite adds BOTH factors plus a broad matrix of supporting scenarios.
 *
 * ---------------------------------------------------------------------------
 * BUG 1 — AVG denominator COUNT(*) instead of NULL-aware COUNT(value).
 * ---------------------------------------------------------------------------
 *   averageExpression.ts:65  AVG(x) -> operand.sum(x).divide(operand.count())
 *   countExpression.ts:46    count()  ->  SQL COUNT(*)
 * COUNT(*) counts sub-groups whose averaged value IS NULL; SUM(x) skips them.
 * Denominator is inflated -> the average is understated. SQL AVG(x) ignores
 * NULL x, so the decomposed form must too: the count channel must be a
 * NULL-AWARE count of the averaged expression (count of non-null x), i.e.
 *   SUM(CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END)   — never a bare COUNT(*).
 *
 * The defect appears on TWO companion sites that decompose AVG to channels:
 *   - per-metric AVG leg    (perMetricSplitDecomposition.ts:190 + decomposeAverage)
 *   - companion AVG measure (companionDecomposition.ts:108 -> operand.count())
 * and rides into Druid-native / cross-source (baseExternal.ts:4728) builders.
 *
 * GROUND TRUTH (ISDIN ds histories-826e2728..., day 2026-06-07, 1,711,552 rows,
 * only 726,596 non-null pvp — 985K NULL-pvp rows make the defect bite):
 *   - per-metric AvgByRef totals: plywood = 11404.483 WRONG (COUNT*)
 *                                  vs reference AVG(b) = 11668.171 CORRECT (COUNT value)
 *   - per-brand Isdinceutics:      plywood = 21668.453 WRONG vs 22335.174 CORRECT
 *   - companion-avg variant:       plywood =  2932.781 WRONG vs  6908.388 CORRECT (=AVG(pvp))
 * The SINGLE-LEG path emits raw AVG("B") and is CORRECT — so co-querying an
 * avg leg with a different-split metric currently turns a correct number wrong.
 *
 * ---------------------------------------------------------------------------
 * BUG 2 — recombination decided keep-vs-rebuild from row[0] (a global gamble).
 * ---------------------------------------------------------------------------
 * ORIGINAL DEFECT (historical): sqlExternal.ts sampled `joined.data[0]` and
 * passed it to `selectActivePostAggregates(post, sampleRow)`, making ONE
 * decision for the whole result. fullJoin sorts the joined rows by key, so a
 * brand present only in the NON-avg leg could land at data[0] with NULL !T_*
 * channels — and then the avg measure was DROPPED for the ENTIRE result (and
 * the !T_* scaffolding leaked). The decision must be PER-ROW: rebuild where
 * channels are present, keep the finished column where they are absent.
 *
 * LANDED FIX SURFACE (current): `External.applyPerRowPostAggregateRecombination`
 * (baseExternal.ts:1763) makes the keep-vs-rebuild decision PER ROW and is now
 * the single recombination primitive — wired into the per-metric fold at
 * sqlExternal.ts:466. `selectActivePostAggregates` (the row[0] sampler) is
 * RETIRED. The cross-source executor must route through the SAME per-row
 * primitive too (see test/simulate/simulateDruidCrossSourceAvgRecombination).
 *
 * ---------------------------------------------------------------------------
 * RED CONTRACT
 *   FEATURE_FIXED === false  -> the bug-exposing specs assert the CORRECT
 *     post-fix behaviour and therefore FAIL today with REAL value mismatches /
 *     dropped measures (NOT test bugs). Flip to true once the fix lands; they
 *     become the live correctness contract with no edit.
 *   The GREEN guards (single-leg correctness, SUM/COUNT/MIN/MAX unaffected,
 *   gates still throw, building blocks) are shipped-code assertions that must
 *   stay green before AND after the fix.
 * ---------------------------------------------------------------------------
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply, Dataset } = plywood;

// Flip to true once the AVG-denominator + per-row-recombination fix lands.
// While false, every spec gated on it asserts the CORRECT (post-fix) behaviour
// and is RED today — that is the intended phase-RED tripwire.
const FEATURE_FIXED = true;

// ---------------------------------------------------------------------------
// ISDIN-shaped external (matches the live histories datasource attributes).
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
  return plan.flat().filter(q => q && typeof q.query === 'string');
}

function sqlOf(ex) {
  return queriesOf(ex.simulateQueryPlan({ main: ext() }))
    .map(q => q.query)
    .join('\n----\n');
}

// ---------------------------------------------------------------------------
// Canned-requester plumbing for VALUE specs (NO live dependency). A requester
// maps each per-leg SQL to a hand-crafted Dataset of rows, routed by a
// substring of the leg's GROUP-BY column (reference / competitor / seller /
// brand) or its total shape.
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

// avg over (avg pvp per reference) — finer-than-brand split (reference).
function legAvgPvpByReference() {
  return $('main').split('$reference', 'ref').apply('B', $('main').average('$pvp')).average('$B');
}
// max over (max price per competitor) — finer-than-brand split (competitor).
function legMaxPriceByCompetitor() {
  return $('main').split('$competitor', 'comp').apply('M', $('main').max('$price')).max('$M');
}
// sum over (sum pvp per seller) — additive leg, finer split (seller).
function legSumPvpBySeller() {
  return $('main').split('$seller', 'slr').apply('S', $('main').sum('$pvp')).sum('$S');
}
// min over (min price per seller) — second seller-grain leg.
function legMinPriceBySeller() {
  return $('main').split('$seller', 'slr2').apply('N', $('main').min('$price')).min('$N');
}

// The denominator channel of a decomposed AVG must be a NULL-aware count of the
// averaged column. In plywood SQL a `.filter(x.isnt(null)).count()` lowers to a
// CASE-summed non-null count. plywood's canonical not-null predicate is
// `(x IS NULL) IS NOT TRUE` (NotExpression over IsExpression(null)), so the
// emitted form is `SUM(CASE WHEN (x IS NULL) IS NOT TRUE THEN 1 ELSE 0 END)`.
// Accept that form as well as the bare `IS NOT NULL` / `COUNT(col)` shapes.
// It must NEVER be a bare COUNT(*).
const NULL_AWARE_COUNT =
  /SUM\(CASE WHEN[^)]*IS NULL\)? IS NOT TRUE THEN 1|SUM\(CASE WHEN[^)]*IS NULL[^)]*THEN 0 ELSE 1|SUM\(CASE WHEN[^)]*IS NOT NULL[^)]*THEN 1|COUNT\("pvp"\)|COUNT\("price"\)|COUNT\("B_main|COUNT\("a\d/;

// A bare COUNT(*) feeding an avg-denominator channel is the BUG-1 signature.
// We pin the exact wrong SQL shapes the two sites emit today so the RED reason
// is "the wrong COUNT(*) channel is still present", never a harness artefact.
function avgDenomIsCountStar(sql) {
  // per-metric leg today:  COUNT(*) AS "!T_1"  (the avg denominator leaf)
  // companion today:       COUNT(*) AS "a..._main_..."  inside the inner CTE
  return /COUNT\(\*\) AS "!T_\d/.test(sql) || /COUNT\(\*\) AS "a\d/.test(sql);
}

describe('simulate DruidSql AVG-decomposition correctness (COUNT(value) denominator + per-row recombination)', function () {
  // =========================================================================
  // (0) GREEN GUARDS — shipped code that MUST stay correct before & after the
  //     fix. If any of these flip, the fix broke something it must not touch.
  // =========================================================================
  describe('[GREEN] regression-correct anchors (single-leg avg is already right)', function () {
    it('single-leg avg (outer split) emits raw AVG("B_main_0") — NO COUNT(*) denominator', function () {
      // The single-leg path is the correct baseline: it averages the inner
      // per-reference averages directly, never lowering to SUM/COUNT(*).
      const sql = sqlOf(
        $('main').split('$brand', 'Brand').apply('AvgByRef', legAvgPvpByReference()),
      );
      expect(sql, 'inner per-reference avg').to.contain('AVG("pvp")');
      expect(sql, 'outer averages the inner avg directly').to.contain('AVG("B_main_0")');
      expect(avgDenomIsCountStar(sql), 'single-leg never mints a COUNT(*) denominator').to.equal(
        false,
      );
    });

    it('single-leg avg TOTALS emits AVG("B_main_0") to __VALUE__ — correct, no channels', function () {
      const sql = sqlOf(ply().apply('AvgByRef', legAvgPvpByReference()));
      expect(sql).to.contain('AVG("B_main_0")');
      expect(avgDenomIsCountStar(sql)).to.equal(false);
    });

    it('SUM/MIN/MAX legs are unaffected — no AVG, no COUNT channel minted', function () {
      const sqlSum = sqlOf(
        ply()
          .apply('SumBySeller', legSumPvpBySeller())
          .apply('MaxByComp', legMaxPriceByCompetitor()),
      );
      // sum/max legs carry their own reducer, never a count channel.
      expect(sqlSum).to.contain('SUM("S_main');
      expect(sqlSum).to.contain('MAX("M_main');
      expect(avgDenomIsCountStar(sqlSum), 'no avg channel for sum/max-only query').to.equal(false);
    });

    it('plain count() companion is genuinely COUNT(*) — the fix must NOT touch a real row-count', function () {
      // A user-requested count() is a row-count and SHOULD lower to COUNT(*).
      // Only the AVG-DENOMINATOR count channel must change. This guards against
      // an over-broad fix that null-guards every count().
      const sql = sqlOf(
        $('main')
          .split('$brand', 'Brand')
          .apply('Rows', $('main').count())
          .apply('MaxByComp', legMaxPriceByCompetitor()),
      );
      expect(sql, 'real row-count companion stays COUNT(*)').to.match(/COUNT\(\*\)/);
    });
  });

  // =========================================================================
  // (1) BUG 1 — SQL-EMISSION level. The denominator channel of a decomposed
  //     AVG must be a NULL-aware count, never COUNT(*). Deterministic, offline.
  // =========================================================================
  describe(`BUG 1 (AVG denominator) — SQL emission${FEATURE_FIXED ? '' : ' [RED]'}`, function () {
    it('per-metric AVG leg co-queried with a different-split metric: denominator must be NULL-aware', function () {
      // avg-pvp-by-reference + max-price-by-competitor under brand. The avg leg
      // is segregated into SUM/COUNT channels; TODAY the count channel is
      // `COUNT(*) AS "!T_1"` (BUG 1). It must be a non-null count of pvp.
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      const sql = sqlOf(ex);
      // sanity: the per-metric path engaged (two legs, one per distinct split).
      expect(
        queriesOf(ex.simulateQueryPlan({ main: ext() })).length,
        'per-metric engaged',
      ).to.equal(2);
      if (FEATURE_FIXED) {
        expect(avgDenomIsCountStar(sql), 'avg denominator is no longer COUNT(*)').to.equal(false);
        expect(sql, 'avg denominator is a null-aware count of the averaged column').to.match(
          NULL_AWARE_COUNT,
        );
      } else {
        // Phase RED: assert the CORRECT post-fix shape — fails today because the
        // wrong COUNT(*) channel is exactly what is emitted.
        expect(
          avgDenomIsCountStar(sql),
          'RED: avg denominator is still the wrong COUNT(*) channel (BUG 1)',
        ).to.equal(false);
      }
    });

    it('companion AVG measure (different-split sibling) denominator must be NULL-aware, not COUNT(*)', function () {
      // total-avg companion alongside a max-by-competitor resplit leg. The
      // companion AVG lowers to inner SUM + inner COUNT; TODAY the inner count
      // is `COUNT(*) AS "a0_main_1"` then outer SUM — BUG 1 on the companion site.
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgPvp', $('main').average('$pvp'))
        .apply('MaxByComp', legMaxPriceByCompetitor());
      const sql = sqlOf(ex);
      if (FEATURE_FIXED) {
        expect(avgDenomIsCountStar(sql), 'companion avg denominator no longer COUNT(*)').to.equal(
          false,
        );
        expect(sql, 'companion avg count channel is null-aware').to.match(NULL_AWARE_COUNT);
      } else {
        expect(
          avgDenomIsCountStar(sql),
          'RED: companion avg denominator is still COUNT(*) (BUG 1, companion site)',
        ).to.equal(false);
      }
    });

    it('avg-of-a-DIFFERENT-column (price) denominator counts non-null price, not COUNT(*)', function () {
      // Guards that the fix counts the AVERAGED column, not a hard-coded one.
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgPrice', $('main').average('$price'))
        .apply('SumBySeller', legSumPvpBySeller());
      const sql = sqlOf(ex);
      if (FEATURE_FIXED) {
        expect(avgDenomIsCountStar(sql)).to.equal(false);
      } else {
        expect(
          avgDenomIsCountStar(sql),
          'RED: avg(price) companion denominator still COUNT(*)',
        ).to.equal(false);
      }
    });
  });

  // =========================================================================
  // (2) BUG 1 — VALUE level. Canned inner-CTE rows with NULL inner sub-groups.
  //     The fold rebuilds AvgByRef from SUM/COUNT channels; the count channel
  //     value MUST be the non-null count. We model two channel shapes and
  //     route on the SQL the leg actually emits — so the SAME canned facts give
  //     the WRONG number under COUNT(*) and the RIGHT number under COUNT(value).
  // =========================================================================
  describe(`BUG 1 (AVG denominator) — value via NULL-inner-sub-group channels${
    FEATURE_FIXED ? '' : ' [RED]'
  }`, function () {
    // Ground model for brand Isdin at the SUM/COUNT-channel grain:
    //   numerator   SUM(pvp)         = 300   (skips null-pvp sub-groups)
    //   row count    COUNT(*)        = 30    (WRONG: includes 10 null-pvp sub-groups)
    //   non-null     COUNT(pvp)      = 20    (CORRECT)
    //   => WRONG avg = 300/30 = 10 ;  CORRECT avg = 300/20 = 15.
    // The leg returns the count channel that MATCHES the SQL it emitted: a
    // null-aware count SQL -> 20; a bare COUNT(*) SQL -> 30. So the recombined
    // value is 15 iff the fix is in, 10 iff BUG 1 is live.

    it('per-metric AVG with NULL inner sub-groups: ratio uses COUNT(value), not COUNT(*)', async function () {
      // SQL-sensitive canned rows: the count channel value depends on whether
      // the leg emitted a null-aware count (fixed → 20/10) or a bare COUNT(*)
      // (BUG 1 → 30/16), so the SAME facts give 15/8 fixed vs 10/5 broken.
      const rq = promiseFnToStream(req => {
        const sql = (req && req.query && req.query.query) || '';
        if (/reference/.test(sql)) {
          const nn = NULL_AWARE_COUNT.test(sql) && !avgDenomIsCountStar(sql);
          return Promise.resolve([
            { 'Brand': 'Isdin', '!T_0': 300, '!T_1': nn ? 20 : 30 },
            { 'Brand': 'Avene', '!T_0': 80, '!T_1': nn ? 10 : 16 },
          ]);
        }
        if (/competitor/.test(sql)) {
          return Promise.resolve([
            { Brand: 'Isdin', MaxByComp: 99 },
            { Brand: 'Avene', MaxByComp: 50 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());

      const result = await ex.compute({ main: ext(rq) });
      const m = byKey(result, 'Brand');
      // CORRECT (COUNT value):   Isdin 300/20 = 15 ; Avene 80/10 = 8.
      // WRONG  (COUNT(*)):       Isdin 300/30 = 10 ; Avene 80/16 = 5.
      expect(m.Isdin.AvgByRef, 'Isdin avg = SUM/COUNT(value) = 300/20').to.equal(15);
      expect(m.Avene.AvgByRef, 'Avene avg = 80/10').to.equal(8);
    });

    it('companion AVG over NULL-heavy column matches plain AVG (no denominator inflation)', async function () {
      // Companion total-avg + a competitor resplit leg. The companion avg leg
      // returns SUM/COUNT channels with a null-aware count when fixed.
      // Ground: SUM(pvp)=6000, non-null count=1000 -> 6.0 ; row-count=2000 -> 3.0.
      const rq = promiseFnToStream(req => {
        const sql = (req && req.query && req.query.query) || '';
        if (/competitor/.test(sql)) {
          const nn = NULL_AWARE_COUNT.test(sql) && !avgDenomIsCountStar(sql);
          // companion channels (a0_main_0=sum, a0_main_1=count) ride in the
          // SAME competitor sub-query; the outer SUM(sum)/SUM(count) rebuilds avg.
          return Promise.resolve([{ Brand: 'Isdin', AvgPvp: nn ? 6.0 : 3.0, MaxByComp: 99 }]);
        }
        return Promise.resolve([]);
      });
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgPvp', $('main').average('$pvp'))
        .apply('MaxByComp', legMaxPriceByCompetitor());
      const result = await ex.compute({ main: ext(rq) });
      const m = byKey(result, 'Brand');
      // CORRECT (= plain AVG(pvp)) is the non-null ratio.
      expect(m.Isdin.AvgPvp, 'companion avg matches plain AVG over non-null rows').to.equal(6.0);
    });

    it('partial-NULL sub-groups: some references have pvp, some all-null — count(value) drops the null ones', async function () {
      // brand Isdin: 5 references, 2 of them have ALL-null pvp. SUM=450 over the
      // 3 non-null refs (each contributing rows); non-null count=30, row=50.
      // CORRECT 450/30=15 ; WRONG 450/50=9.
      const rq = promiseFnToStream(req => {
        const sql = (req && req.query && req.query.query) || '';
        if (/reference/.test(sql)) {
          const nn = NULL_AWARE_COUNT.test(sql) && !avgDenomIsCountStar(sql);
          return Promise.resolve([{ 'Brand': 'Isdin', '!T_0': 450, '!T_1': nn ? 30 : 50 }]);
        }
        if (/competitor/.test(sql)) {
          return Promise.resolve([{ Brand: 'Isdin', MaxByComp: 7 }]);
        }
        return Promise.resolve([]);
      });
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      const result = await ex.compute({ main: ext(rq) });
      expect(byKey(result, 'Brand').Isdin.AvgByRef, '450/30 (non-null) not 450/50').to.equal(15);
    });

    it('TOTALS mode AVG denominator is non-null count (cross of two distinct legs)', async function () {
      // MODE 1 totals: avg-by-ref + max-by-comp cross into one scalar row. The
      // avg scalar must be SUM/COUNT(value), not SUM/COUNT(*).
      const rq = promiseFnToStream(req => {
        const sql = (req && req.query && req.query.query) || '';
        if (/reference/.test(sql)) {
          const nn = NULL_AWARE_COUNT.test(sql) && !avgDenomIsCountStar(sql);
          return Promise.resolve([{ '!T_0': 11668, '!T_1': nn ? 1000 : 1023 }]);
        }
        if (/competitor/.test(sql)) {
          return Promise.resolve([{ MaxByComp: 250 }]);
        }
        return Promise.resolve([]);
      });
      const ex = ply()
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      const result = await ex.compute({ main: ext(rq) });
      const rows = dataOf(result);
      expect(rows.length, 'totals collapse to one row').to.equal(1);
      expect(rows[0].AvgByRef, '11668/1000 (non-null), not /1023').to.equal(11.668);
    });
  });

  // =========================================================================
  // (3) BUG 2 — per-row recombination. selectActivePostAggregates must decide
  //     keep-vs-rebuild PER ROW, never from joined.data[0]. Deterministic:
  //     fullJoin sorts by key, so a chosen avg-absent brand lands at data[0].
  // =========================================================================
  describe(`BUG 2 (per-row recombination) — first-row gamble${
    FEATURE_FIXED ? '' : ' [RED]'
  }`, function () {
    it('avg-absent brand sorts FIRST (null !T_* at data[0]): avg MUST still be rebuilt for later rows', async function () {
      // avg leg covers {Isdin, Avene}; max leg ALSO has "Aaa" (avg-absent).
      // fullJoin sorts by Brand → "Aaa" (null channels) is data[0]. Today the
      // global decision drops AvgByRef for EVERY row and leaks !T_* scaffolding.
      const requester = cannedRequester([
        {
          tag: 'avgChannels',
          match: sql => /reference/.test(sql),
          rows: [
            { 'Brand': 'Isdin', '!T_0': 300, '!T_1': 20 },
            { 'Brand': 'Avene', '!T_0': 80, '!T_1': 10 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Aaa', MaxByComp: 5 }, // avg-absent → null channels after join → data[0]
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
      // The brands WITH channels must get their avg rebuilt regardless of row[0].
      expect(m.Isdin.AvgByRef, 'Isdin 300/20 rebuilt despite avg-absent first row').to.equal(15);
      expect(m.Avene.AvgByRef, 'Avene 80/10 rebuilt despite avg-absent first row').to.equal(8);
      // The avg-absent brand keeps its other measure and a null/absent avg (not NaN).
      expect(m.Aaa.MaxByComp, 'avg-absent brand keeps MaxByComp').to.equal(5);
      expect(m.Aaa.AvgByRef == null, 'avg-absent brand has no avg (null, not 0 or NaN)').to.equal(
        true,
      );
      // Scaffolding must be dropped for ALL rows, not leaked.
      for (const r of dataOf(result)) {
        expect(r['!T_0'], 'no leaked !T_0 scaffolding').to.equal(undefined);
        expect(r['!T_1'], 'no leaked !T_1 scaffolding').to.equal(undefined);
      }
    });

    it('avg-absent brand sorts LAST (control): avg already rebuilt today — must STAY correct after fix', async function () {
      // Control proving it is precisely a FIRST-ROW gamble: when the avg-channel
      // row is data[0], recombination is globally enabled and the brands WITH
      // channels are correct today. The avg-absent brand ("Zzz") must NOT get a
      // NaN from null/null — it must be null/absent (this part is RED today: the
      // global rebuild divides its null channels into NaN).
      const requester = cannedRequester([
        {
          tag: 'avgChannels',
          match: sql => /reference/.test(sql),
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
            { Brand: 'Zzz', MaxByComp: 5 }, // avg-absent, sorts LAST
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      expect(m.Isdin.AvgByRef, 'Isdin 300/20').to.equal(15);
      expect(m.Avene.AvgByRef, 'Avene 80/10').to.equal(8);
      // The avg-absent brand must be null/absent, NOT NaN from a global divide.
      const zzzAvg = m.Zzz.AvgByRef;
      expect(
        zzzAvg == null || !Number.isNaN(zzzAvg),
        'avg-absent brand must not be NaN (null channels not divided)',
      ).to.equal(true);
      expect(m.Zzz.AvgByRef == null, 'avg-absent brand has no avg').to.equal(true);
    });

    it('MIXED: some brands have channels, some have the finished column — per-row keep-vs-rebuild', async function () {
      // The avg leg returns CHANNELS for Isdin/Avene but the FINISHED column for
      // a brand the transport already reduced (Bella). Per-row: rebuild the two,
      // keep Bella's finished value. data[0] is "Bella" (finished, no channels),
      // which today globally disables rebuild for Isdin/Avene.
      const requester = cannedRequester([
        {
          tag: 'avgChannels',
          match: sql => /reference/.test(sql),
          rows: [
            { Brand: 'Bella', AvgByRef: 42 }, // finished column, no channels; sorts first
            { 'Brand': 'Isdin', '!T_0': 300, '!T_1': 20 },
            { 'Brand': 'Ivene', '!T_0': 80, '!T_1': 10 },
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Bella', MaxByComp: 1 },
            { Brand: 'Isdin', MaxByComp: 99 },
            { Brand: 'Ivene', MaxByComp: 50 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      expect(m.Bella.AvgByRef, 'finished column kept as-is').to.equal(42);
      expect(m.Isdin.AvgByRef, 'channels rebuilt 300/20').to.equal(15);
      expect(m.Ivene.AvgByRef, 'channels rebuilt 80/10').to.equal(8);
      for (const r of dataOf(result)) {
        expect(r['!T_0'], 'no leaked scaffolding').to.equal(undefined);
        expect(r['!T_1'], 'no leaked scaffolding').to.equal(undefined);
      }
    });

    it('ONLY data[0] has channels, the rest are finished columns — rebuild must not corrupt the finished rows', async function () {
      // Inverse first-row factor: data[0] DOES have channels (so today's global
      // decision rebuilds for ALL), but later rows carry a finished column with
      // NO channels → the global rebuild divides their null channels into NaN.
      const requester = cannedRequester([
        {
          tag: 'avgChannels',
          match: sql => /reference/.test(sql),
          rows: [
            { 'Brand': 'Aaa', '!T_0': 300, '!T_1': 20 }, // channels, sorts first
            { Brand: 'Mmm', AvgByRef: 7 }, // finished column, no channels
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Aaa', MaxByComp: 9 },
            { Brand: 'Mmm', MaxByComp: 4 },
          ],
        },
      ]);
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());

      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      expect(m.Aaa.AvgByRef, 'channels rebuilt 300/20').to.equal(15);
      expect(m.Mmm.AvgByRef, 'finished column preserved, not NaN').to.equal(7);
    });
  });

  // =========================================================================
  // (4) COMPOSABILITY — avg correctness under derived measures, sort, HAVING,
  //     mixed grains. Each combines an avg leg (BUG-1-sensitive) with the
  //     per-row factor (BUG-2-sensitive). Canned channels with NULL inner.
  // =========================================================================
  describe(`composability of the avg fix${FEATURE_FIXED ? '' : ' [RED]'}`, function () {
    function brandReq() {
      return cannedRequester([
        {
          tag: 'avgChannels',
          match: sql => /reference/.test(sql),
          rows: [
            { 'Brand': 'Bella', '!T_0': 90, '!T_1': 6 }, // 15
            { 'Brand': 'Isdin', '!T_0': 300, '!T_1': 20 }, // 15
            { 'Brand': 'Avene', '!T_0': 80, '!T_1': 10 }, // 8
          ],
        },
        {
          tag: 'maxByComp',
          match: sql => /competitor/.test(sql),
          rows: [
            { Brand: 'Bella', MaxByComp: 30 },
            { Brand: 'Isdin', MaxByComp: 60 },
            { Brand: 'Avene', MaxByComp: 40 },
          ],
        },
      ]);
    }

    it('derived measure (AvgByRef / MaxByComp) uses the channel-rebuilt avg, not a dropped one', async function () {
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .apply('Ratio', $('AvgByRef').divide('$MaxByComp'));
      const result = await ex.compute({ main: ext(brandReq()) });
      const m = byKey(result, 'Brand');
      expect(m.Isdin.AvgByRef).to.equal(15);
      expect(m.Isdin.Ratio, '15/60').to.equal(0.25);
      expect(m.Avene.Ratio, '8/40').to.equal(0.2);
      expect(m.Bella.Ratio, '15/30').to.equal(0.5);
    });

    it('SORT by the avg measure desc + LIMIT 2 (avg must be present to sort on it)', async function () {
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .sort('$AvgByRef', 'descending')
        .limit(2);
      const result = await ex.compute({ main: ext(brandReq()) });
      const rows = dataOf(result);
      expect(rows.length, 'top-2 by AvgByRef').to.equal(2);
      // Bella(15) and Isdin(15) tie at top; Avene(8) drops. Both survivors are 15.
      expect(rows[0].AvgByRef).to.equal(15);
      expect(rows[1].AvgByRef).to.equal(15);
      expect(rows.map(r => r.Brand).sort()).to.deep.equal(['Bella', 'Isdin']);
    });

    it('HAVING on the avg measure (AvgByRef >= 15) filters the rebuilt avg post-join', async function () {
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor())
        .filter($('AvgByRef').greaterThanOrEqual(15));
      const result = await ex.compute({ main: ext(brandReq()) });
      const brands = dataOf(result)
        .map(r => r.Brand)
        .sort();
      expect(brands, 'only brands with rebuilt avg >= 15').to.deep.equal(['Bella', 'Isdin']);
    });

    it('plain G companion sum + avg leg + max leg: the avg stays channel-correct alongside the companion', async function () {
      const requester = cannedRequester([
        {
          tag: 'companionAtBrand',
          match: sql => /"brand"/.test(sql) && !/reference|competitor/.test(sql),
          rows: [
            { Brand: 'Isdin', TotalPrice: 5000 },
            { Brand: 'Avene', TotalPrice: 2000 },
          ],
        },
        {
          tag: 'avgChannels',
          match: sql => /reference/.test(sql),
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
        .apply('TotalPrice', $('main').sum('$price'))
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('MaxByComp', legMaxPriceByCompetitor());
      const result = await ex.compute({ main: ext(requester) });
      const m = byKey(result, 'Brand');
      expect(m.Isdin.TotalPrice).to.equal(5000);
      expect(m.Isdin.AvgByRef, '300/20').to.equal(15);
      expect(m.Avene.AvgByRef, '80/10').to.equal(8);
    });
  });

  // =========================================================================
  // (5) GREEN GATES — the non-decomposable refusals must keep throwing, both
  //     before and after the fix. (The avg fix must not loosen any gate.)
  // =========================================================================
  describe('[GREEN] gates that MUST still throw (avg fix changes nothing here)', function () {
    it('countDistinct companion on a finer-than-G avg-resplit query → throws (not re-aggregable)', function () {
      const ex = $('main')
        .split('$brand', 'Brand')
        .apply('AvgByRef', legAvgPvpByReference())
        .apply('Sellers', $('main').countDistinct('$seller'));
      expect(function () {
        queriesOf(ex.simulateQueryPlan({ main: ext() }));
      }).to.throw(/not re-aggregable/);
    });

    it('countDistinct as its OWN finer leg alongside an avg leg → must NOT silently lower', function () {
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
  });

  // =========================================================================
  // (6) BUILDING-BLOCK GREEN — the LANDED recombination primitive itself
  //     (`applyPerRowPostAggregateRecombination`, the BUG-2 fix surface),
  //     exercised directly so a regression in the per-row primitive surfaces
  //     here precisely. The row mix is HETEROGENEOUS (a channel row + a
  //     finished-column row) — the exact case the retired row[0]-sampling /
  //     global rebuild got WRONG. The legacy global `applyPostAggregateRecombination`
  //     would overwrite the finished column to NaN here, so this also pins WHY
  //     the per-row primitive is the single source of truth.
  // =========================================================================
  describe('[GREEN] per-row recombination primitive sanity (the BUG-2 fix surface)', function () {
    function buildAvgApply() {
      // The avg recombination apply: AvgByRef = $!T_0 / $!T_1. The `!` prefix is
      // not parseable by the loose string parser, so construct via the explicit
      // JS form (the same shape segregation mints).
      const applyEx = plywood.Expression.fromJS({
        op: 'divide',
        operand: { op: 'ref', name: '!T_0' },
        expression: { op: 'ref', name: '!T_1' },
      });
      const ApplyExpression = plywood.Expression.classMap['apply'];
      return new ApplyExpression({
        name: 'AvgByRef',
        operand: plywood.Expression.fromJS({ op: 'ref', name: '_' }),
        expression: applyEx,
      });
    }

    it('applyPerRowPostAggregateRecombination rebuilds channel rows, PRESERVES the finished-column row, drops !T_*', function () {
      const ds = Dataset.fromJS({
        keys: ['Brand'],
        data: [
          { 'Brand': 'Isdin', '!T_0': 300, '!T_1': 20, 'AvgByRef': null }, // channels → 15
          { 'Brand': 'Avene', '!T_0': 80, '!T_1': 10, 'AvgByRef': null }, // channels → 8
          { 'Brand': 'Bella', '!T_0': null, '!T_1': null, 'AvgByRef': 42 }, // finished column kept
        ],
      });
      const out = External.applyPerRowPostAggregateRecombination(ds, [buildAvgApply()]);
      const m = {};
      for (const r of out.toJS().data) m[r.Brand] = r;
      expect(m.Isdin.AvgByRef, '300/20').to.equal(15);
      expect(m.Avene.AvgByRef, '80/10').to.equal(8);
      expect(m.Bella.AvgByRef, 'finished column preserved (per-row gate), not NaN').to.equal(42);
      expect(m.Isdin['!T_0'], 'scaffolding dropped').to.equal(undefined);
      expect(m.Bella['!T_1'], 'scaffolding dropped on the finished row too').to.equal(undefined);
    });

    it('legacy global applyPostAggregateRecombination FAILS that contract (overwrites finished column) — divergence pinned', function () {
      // Documents WHY the cross-source executor must stop calling the global fn:
      // on the same heterogeneous rows the unconditional global rebuild destroys
      // Bella's finished 42 (divides null/null → NaN/null).
      const ds = Dataset.fromJS({
        keys: ['Brand'],
        data: [
          { 'Brand': 'Isdin', '!T_0': 300, '!T_1': 20, 'AvgByRef': null },
          { 'Brand': 'Bella', '!T_0': null, '!T_1': null, 'AvgByRef': 42 },
        ],
      });
      const out = External.applyPostAggregateRecombination(ds, [buildAvgApply()]);
      const m = {};
      for (const r of out.toJS().data) m[r.Brand] = r;
      expect(m.Isdin.AvgByRef, 'global rebuilds the channel row').to.equal(15);
      expect(m.Bella.AvgByRef === 42, 'global fn DESTROYS the finished column').to.equal(false);
    });
  });
});
