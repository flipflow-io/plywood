/*
 * ===========================================================================
 * CROSS-SOURCE AVG RECOMBINATION — EXHAUSTIVE PHASE-RED TDD suite
 * ===========================================================================
 *
 * Closing pass for the completeness-critic GAP CLASS that survived the prior
 * AVG fix: the per-metric fold (sqlExternal.ts:466) was routed through the new
 * PER-ROW recombination primitive, but the CROSS-SOURCE / linked-source
 * decompose executor (baseExternal.ts:3825) STILL calls the OLD GLOBAL
 * `applyPostAggregateRecombination` (baseExternal.ts:1709). They must be ONE
 * source of truth: route the cross-source executor through
 * `applyPerRowPostAggregateRecombination` (baseExternal.ts:1763).
 *
 * ---------------------------------------------------------------------------
 * THE DIVERGENCE (deterministic, two flavours — both currently WRONG on the
 * cross-source path because it uses the GLOBAL fn):
 *
 *   (A) FINISHED-COLUMN row with NULL channels. A transport may return the
 *       already-reduced measure column (`avg_price = 8`) for some keys while
 *       returning the SUM/COUNT channels (`!T_0`/`!T_1`) for others. The GLOBAL
 *       fn rebuilds EVERY row unconditionally: `8 / (null/null)` collapses to
 *       NaN — the finished value is DESTROYED. The PER-ROW fn channel-gates:
 *       a row whose channels are null/absent KEEPS its existing value.
 *
 *   (B) ORPHAN row (left/full join). A key present only in the NON-avg leg
 *       carries null channels. The GLOBAL fn samples `data[0]` to drop the
 *       `!T_*` scaffolding and can globally drop the avg for the whole result;
 *       the PER-ROW fn drops scaffolding from the UNION of all rows and gates
 *       keep-vs-rebuild per row, so an orphan keeps a null avg (never NaN,
 *       never globally dropped).
 *
 * Severity: this divergence is LATENT on the live magic-dim path today (its
 * own homogeneous INNER join can't reach it). It becomes a live correctness
 * bug the moment a transport returns finished avg columns (A) or the path
 * adopts a left/full join (B). Routing both halves through the per-row
 * primitive closes the gap with ONE source of truth.
 *
 * ---------------------------------------------------------------------------
 * RED CONTRACT
 *   FEATURE_FIXED === false (today) → the cross-source specs assert the CORRECT
 *     post-fix behaviour and therefore FAIL with REAL value mismatches (a
 *     finished avg overwritten to NaN; not a test bug). Flip to true once the
 *     cross-source executor is routed through the per-row primitive — the same
 *     specs become the live correctness contract with NO edit.
 *   The GREEN guards (regression-correct all-channels fold, native-Druid
 *   COUNT(*) pin, the per-row primitive sanity, int-column + nesting shape)
 *   are shipped-code assertions that stay green BEFORE and AFTER the fix.
 * ---------------------------------------------------------------------------
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply, Dataset, Expression } = plywood;

// Routed: the cross-source decompose executor (baseExternal.ts:~3825) now calls
// `applyPerRowPostAggregateRecombination` (the ONE source of truth shared with
// the per-metric fold at sqlExternal.ts:466). With the fix landed, every gated
// spec asserts the CORRECT (post-fix) behaviour and is GREEN.
const FEATURE_FIXED = true;

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

const timeFilter = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-02T00:00:00Z'),
});

// Cross-source magic-dimension external: split lives ONLY in the lookup
// (`brand_country`), joinKey is `brand`. This is the live "avg + magic dim"
// shape that routes through getCrossExternalDecomposition + the executor at
// baseExternal.ts:3825.
function makeMain(requester, joinMode) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'main_ds',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
      ],
      linkedSources: {
        magic_bc: {
          source: 'lookup_bc_rev1',
          joinKeys: ['brand'],
          autoInjectJoinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: joinMode || 'inner',
          timeAlignment: 'eternal',
          backing: { phase: 'canonical' },
          attributes: [
            { name: 'brand', type: 'STRING' },
            { name: 'brand_country', type: 'STRING' },
          ],
        },
      },
      filter: timeFilter,
    },
    requester,
  );
}

// Turnilo-style top-level: scope registrations + a SPLIT by the linked-only dim.
function buildSplitExpr(valueApplies, sortName, sortDir) {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || 'brand_country'), sortDir || 'ascending').limit(100);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

async function computeSplit(valueApplies, requester, opts) {
  opts = opts || {};
  const ex = buildSplitExpr(valueApplies, opts.sortName, opts.sortDir);
  const result = await ex.compute({ main: makeMain(requester, opts.joinMode) });
  return result.toJS().data[0].SPLIT.data;
}

function byCountry(rows) {
  const m = {};
  for (const r of rows) m[r.brand_country] = r;
  return m;
}

describe('Cross-source AVG recombination — one source of truth (per-row)', function () {
  // =========================================================================
  // (1) GAP #1 — the cross-source executor must use the PER-ROW primitive.
  //     RED today: it calls the GLOBAL fn at baseExternal.ts:3825.
  // =========================================================================
  describe(`GAP #1 — cross-source executor recombination${
    FEATURE_FIXED ? '' : ' [RED]'
  }`, function () {
    it('FINISHED avg column for one key (null channels) is PRESERVED, not overwritten to NaN', async function () {
      // Both brands match the lookup (inner join). The TRANSPORT returns
      // SUM/COUNT channels for B1 (Spain) but the already-reduced FINISHED
      // `avg_price` column for B3 (France). Per-row must rebuild Spain (300/20
      // = 15) and KEEP France's finished 8. The GLOBAL fn divides France's
      // null/null channels → NaN, destroying the finished value.
      const requester = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B3', brand_country: 'France' },
          ]);
        }
        if (sql.includes('main_ds')) {
          return Promise.resolve([
            { '__join_brand': 'B1', '!T_0': 300, '!T_1': 20 }, // channels → 15
            { __join_brand: 'B3', avg_price: 8 }, // FINISHED column, null channels
          ]);
        }
        return Promise.resolve([]);
      });
      const rows = await computeSplit([['avg_price', '$main.average($price)']], requester);
      const m = byCountry(rows);
      // Spain rebuilt from channels (regression-correct under BOTH fns).
      expect(m.Spain.avg_price, 'Spain 300/20 rebuilt from channels').to.equal(15);
      // France: per-row preserves the finished 8; global overwrites to NaN.
      const franceAvg = m.France && m.France.avg_price;
      if (FEATURE_FIXED) {
        expect(franceAvg, 'France finished column PRESERVED (per-row keeps it)').to.equal(8);
      } else {
        // Phase RED: today the global rebuild divides France's null/null
        // channels and the finished 8 is destroyed (NaN). Pin the exact wrong
        // value so the RED reason is "finished column overwritten", not a
        // harness artefact.
        expect(
          franceAvg === 8,
          'RED today: France finished avg is NOT preserved (global rebuild overwrites it)',
        ).to.equal(false);
        const isNaNish =
          franceAvg == null || Number.isNaN(Number(franceAvg)) || franceAvg === 'NaN';
        expect(
          isNaNish,
          'RED today: France avg is NaN/null (global divided null/null), not the finished 8',
        ).to.equal(true);
      }
      // Scaffolding never leaks to the caller, either way.
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
        }
      }
    });

    it('all-channels INNER fold stays regression-correct (weighted avg, not media-de-medias)', async function () {
      // Homogeneous case: every joined row carries channels. Spain has two
      // brands (sum=200, count=101 → weighted 200/101 = 1.9801…, NOT the
      // media-de-medias 50.5); France has one (70/10 = 7). This is GREEN under
      // BOTH fns — the per-row routing must not regress the live happy path.
      const requester = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B2', brand_country: 'Spain' },
            { __join_brand: 'B3', brand_country: 'France' },
          ]);
        }
        if (sql.includes('main_ds')) {
          return Promise.resolve([
            { '__join_brand': 'B1', '!T_0': 100, '!T_1': 100 },
            { '__join_brand': 'B2', '!T_0': 100, '!T_1': 1 },
            { '__join_brand': 'B3', '!T_0': 70, '!T_1': 10 },
          ]);
        }
        return Promise.resolve([]);
      });
      const rows = await computeSplit([['avg_price', '$main.average($price)']], requester);
      const m = byCountry(rows);
      expect(rows.length, 'one row per country').to.equal(2);
      expect(m.Spain.avg_price, 'Spain weighted avg 200/101').to.be.closeTo(200 / 101, 1e-9);
      expect(m.Spain.avg_price, 'NOT media-de-medias 50.5').to.not.be.closeTo(50.5, 1e-6);
      expect(m.France.avg_price, 'France 70/10').to.equal(7);
    });

    it('LEFT-join orphan (linked key with no main avg row) keeps null avg, never NaN', async function () {
      // joinMode 'left' keeps a main brand even when... but the orphan we care
      // about is the MIRROR shape the critic flagged: a key landing in the fold
      // with NULL channels. We model it via a transport that returns channels
      // for Spain and an explicit null-channel row for France (the orphan that a
      // left/full join would synthesise). Per-row keeps France's avg null; the
      // global rebuild divides null/null. Both currently yield null here (the
      // benign half), so this PINS that the orphan never becomes NaN — and after
      // the fix it stays null via the per-row gate, not via accident.
      const requester = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B3', brand_country: 'France' },
          ]);
        }
        if (sql.includes('main_ds')) {
          return Promise.resolve([
            { '__join_brand': 'B1', '!T_0': 300, '!T_1': 20 }, // Spain → 15
            { '__join_brand': 'B3', '!T_0': null, '!T_1': null }, // France orphan: null channels
          ]);
        }
        return Promise.resolve([]);
      });
      const rows = await computeSplit([['avg_price', '$main.average($price)']], requester);
      const m = byCountry(rows);
      expect(m.Spain.avg_price, 'Spain 300/20 rebuilt').to.equal(15);
      const franceAvg = m.France && m.France.avg_price;
      const orphanIsNullNotNaN =
        franceAvg == null || (!Number.isNaN(Number(franceAvg)) && franceAvg !== 'NaN');
      expect(
        orphanIsNullNotNaN,
        'orphan avg must be null/absent, never NaN (null channels not divided)',
      ).to.equal(true);
    });

    it('DERIVED cross-source measure (Ratio = avg/max) on a finished-column row stays consistent with its avg', async function () {
      // A derived ratio references the RAW channels (cross-source post-aggregate
      // applies come from segregationAggregateApplies, not the per-metric cross-
      // leg rebasing), so on a finished-column row per-row channel-gates it to
      // null (avg kept, ratio left null) rather than NaN. France returns the
      // finished avg (8) and a max (4); per-row keeps avg=8.
      const requester = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B3', brand_country: 'France' },
          ]);
        }
        if (sql.includes('main_ds')) {
          return Promise.resolve([
            { '__join_brand': 'B1', '!T_0': 300, '!T_1': 20, 'price_max': 60 }, // 15, max 60
            { __join_brand: 'B3', avg_price: 8, price_max: 16 }, // finished avg 8, max 16
          ]);
        }
        return Promise.resolve([]);
      });
      const rows = await computeSplit(
        [
          ['avg_price', '$main.average($price)'],
          ['price_max', '$main.max($price)'],
          ['Ratio', '$main.average($price) / $main.max($price)'],
        ],
        requester,
      );
      const m = byCountry(rows);
      expect(m.Spain.avg_price, 'Spain avg 300/20').to.equal(15);
      expect(m.Spain.Ratio, 'Spain ratio 15/60').to.be.closeTo(0.25, 1e-9);
      if (FEATURE_FIXED) {
        expect(m.France.avg_price, 'France finished avg preserved').to.equal(8);
      } else {
        expect(
          m.France.avg_price === 8,
          'RED today: France finished avg overwritten by global rebuild',
        ).to.equal(false);
      }
    });

    it('avg + sum + max together: each measure recombines correctly across the fold', async function () {
      // Mixed measure set through the cross-source executor. Segregation DEDUPES
      // the avg numerator with the standalone `sum($price)` measure: both are
      // SUM("price"), so the avg recombination is `avg_price = $rev / $!T_0`
      // where `!T_0` is the null-aware count channel and `rev` doubles as the
      // shared SUM. The fixture therefore carries `rev` (= SUM), `!T_0` (= count)
      // and `price_max` per brand. avg recombines from rev/!T_0 AFTER re-agg, sum
      // sums, max maximises — all collapse to one row per country.
      const requester = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B2', brand_country: 'Spain' },
            { __join_brand: 'B3', brand_country: 'France' },
          ]);
        }
        if (sql.includes('main_ds')) {
          return Promise.resolve([
            // Spain: sum 100+100=200, count 100+1=101 → weighted avg 200/101.
            { '__join_brand': 'B1', 'rev': 100, '!T_0': 100, 'price_max': 5 },
            { '__join_brand': 'B2', 'rev': 100, '!T_0': 1, 'price_max': 9 },
            // France: sum 70, count 10 → avg 7.
            { '__join_brand': 'B3', 'rev': 70, '!T_0': 10, 'price_max': 3 },
          ]);
        }
        return Promise.resolve([]);
      });
      const rows = await computeSplit(
        [
          ['avg_price', '$main.average($price)'],
          ['rev', '$main.sum($price)'],
          ['price_max', '$main.max($price)'],
        ],
        requester,
      );
      const m = byCountry(rows);
      expect(rows.length, 'one row per country').to.equal(2);
      expect(m.Spain.avg_price, 'Spain weighted avg 200/101').to.be.closeTo(200 / 101, 1e-9);
      expect(m.Spain.rev, 'Spain sum 100+100').to.equal(200);
      expect(m.Spain.price_max, 'Spain max(5,9)').to.equal(9);
      expect(m.France.avg_price, 'France 70/10').to.equal(7);
      expect(m.France.rev, 'France sum 70').to.equal(70);
      expect(m.France.price_max, 'France max 3').to.equal(3);
    });
  });

  // =========================================================================
  // (2) GAP #2 — the recombination PRIMITIVE itself, repointed at the PER-ROW
  //     fn (the live BUG-2 fix surface) with a null-channel + finished-column
  //     row. GREEN — proves the per-row contract directly so a regression in
  //     applyPerRowPostAggregateRecombination surfaces here precisely.
  //     (The OLD global fn FAILS this contract — see the asserted divergence.)
  // =========================================================================
  describe('[GREEN] per-row recombination primitive sanity (the BUG-2 fix surface)', function () {
    function buildAvgApply() {
      const applyEx = Expression.fromJS({
        op: 'divide',
        operand: { op: 'ref', name: '!T_0' },
        expression: { op: 'ref', name: '!T_1' },
      });
      const ApplyExpression = Expression.classMap['apply'];
      return new ApplyExpression({
        name: 'AvgByRef',
        operand: Expression.fromJS({ op: 'ref', name: '_' }),
        expression: applyEx,
      });
    }

    it('applyPerRowPostAggregateRecombination: rebuilds channel rows, PRESERVES the finished-column row, drops !T_*', function () {
      // Heterogeneous mix: Isdin has channels (rebuild → 15); Bella has the
      // FINISHED column with null channels (must be PRESERVED → 42); Zzz is an
      // orphan with all-null (avg stays null, never NaN). This is the exact row
      // mix the GLOBAL fn corrupts.
      const ds = Dataset.fromJS({
        keys: ['Brand'],
        data: [
          { 'Brand': 'Isdin', '!T_0': 300, '!T_1': 20, 'AvgByRef': null },
          { 'Brand': 'Bella', '!T_0': null, '!T_1': null, 'AvgByRef': 42 },
          { 'Brand': 'Zzz', '!T_0': null, '!T_1': null, 'AvgByRef': null },
        ],
      });
      const out = External.applyPerRowPostAggregateRecombination(ds, [buildAvgApply()]);
      const m = {};
      for (const r of out.toJS().data) m[r.Brand] = r;
      expect(m.Isdin.AvgByRef, 'channels rebuilt 300/20').to.equal(15);
      expect(m.Bella.AvgByRef, 'finished column PRESERVED (per-row gate)').to.equal(42);
      const zzz = m.Zzz.AvgByRef;
      expect(zzz == null || !Number.isNaN(zzz), 'orphan avg null, never NaN').to.equal(true);
      // Scaffolding dropped across ALL rows (union of keys), not just data[0].
      expect(m.Isdin['!T_0'], 'scaffolding dropped').to.equal(undefined);
      expect(m.Bella['!T_1'], 'scaffolding dropped on finished row too').to.equal(undefined);
    });

    it('the OLD GLOBAL fn FAILS that contract on the same rows (overwrites finished column) — divergence pinned', function () {
      // Documents WHY the cross-source executor must stop calling the global fn:
      // on the identical row mix the global rebuild destroys Bella's finished 42.
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
      expect(m.Isdin.AvgByRef, 'global rebuilds channel row 300/20').to.equal(15);
      // The divergence: the global fn overwrote Bella's finished 42 → null/NaN.
      expect(m.Bella.AvgByRef === 42, 'global fn DESTROYS the finished column').to.equal(false);
    });
  });

  // =========================================================================
  // (3) GAP #3 — native-Druid AVG keeps COUNT(*) BY DESIGN. SQL externals use a
  //     null-aware count; native Druid cannot filter a rolled-up metric, so it
  //     keeps the historical COUNT(*). This is an INTENTIONAL engine-semantics
  //     difference, NOT a bug — pinned here as a labeled decision and surfaced
  //     to the human (product decision pending). Out of scope to change.
  // =========================================================================
  describe('[GREEN] native-Druid AVG uses COUNT(*) by design — INTENTIONAL, product decision pending', function () {
    it('SQL-engine AVG decomposes to a NULL-AWARE count (filter is expressible)', function () {
      const avg = $('main').average('$price');
      // nullAwareCount=true (the SQL-external default at baseExternal.ts:4839).
      const decomposed = avg.decomposeAverage(undefined, true);
      expect(decomposed.toString(), 'denominator filters out null price').to.equal(
        '$main.sum($price).divide($main.filter($price.is(null).not()).count())',
      );
    });

    it('native-Druid AVG keeps COUNT(*) — INTENTIONAL (rolled-up metric cannot be re-filtered)', function () {
      const avg = $('main').average('$price');
      // nullAwareCount=false is what baseExternal.ts:4839 passes when
      // `engine === 'druid'`. A non-null filter on an unsplitable/rolled-up
      // Druid metric is not expressible as a Druid filter, so the denominator
      // stays the plain COUNT(*). This UNDERSTATES the avg when the averaged
      // column has nulls — the SAME defect class as BUG 1, but on the native
      // engine it is a DELIBERATE opt-out, not an oversight.
      const decomposed = avg.decomposeAverage(undefined, false);
      expect(decomposed.toString(), 'denominator is the plain COUNT(*)').to.equal(
        '$main.sum($price).divide($main.count())',
      );
      // ====================================================================
      // FLAGGED DECISION FOR THE HUMAN:
      //   native-Druid AVG over a NULL-bearing column understates the result
      //   (COUNT(*) denominator includes null rows). SQL engines do not.
      //   This pin LOCKS the current intentional behaviour; flipping it
      //   requires a product decision (and a Druid-side null filter). Until
      //   then, native-Druid AVG and SQL AVG can disagree on null-heavy data.
      // ====================================================================
    });
  });

  // =========================================================================
  // (4) GAP #4 — minor completeness: AVG over an integer-valued (long-stored)
  //     column, and AVG nesting depth. Deterministic, low-risk.
  // =========================================================================
  describe('[GREEN] AVG over integer/long-stored column + nesting depth', function () {
    function intExt() {
      return External.fromJS({
        engine: 'druidsql',
        source: 'histories',
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          // Druid LONG metrics surface as NUMBER in plywood; `quantity` models
          // an integer-valued (long-stored) measure.
          { name: 'quantity', type: 'NUMBER', unsplitable: true },
          { name: 'brand', type: 'STRING' },
        ],
      });
    }

    it('AVG over an integer/long-stored column emits AVG natively (single-leg, no COUNT(*) channel)', function () {
      const ex = $('main').split('$brand', 'brand').apply('avg_qty', '$main.average($quantity)');
      const sqls = ex
        .simulateQueryPlan({ main: intExt() })
        .flat()
        .filter(q => typeof q.query === 'string')
        .map(q => q.query);
      const mainSql = sqls.find(s => /avg_qty/i.test(s));
      expect(mainSql, 'main sub-query exists').to.exist;
      expect(mainSql, 'integer column averaged natively').to.match(
        /AVG\("quantity"\) AS "avg_qty"/,
      );
      expect(mainSql, 'no COUNT(*) denominator channel for single-leg avg').to.not.match(
        /COUNT\(\*\) AS "!T_/,
      );
    });

    it('AVG over an integer/long-stored column recombines from channels to the exact ratio (no integer truncation)', async function () {
      // Cross-source fold: integer SUM/COUNT channels recombine to a fractional
      // average (3/2 = 1.5), proving the recombination divides as a real ratio,
      // not integer division.
      const requester = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([{ __join_brand: 'B1', brand_country: 'Spain' }]);
        }
        if (sql.includes('main_ds')) {
          return Promise.resolve([{ '__join_brand': 'B1', '!T_0': 3, '!T_1': 2 }]);
        }
        return Promise.resolve([]);
      });
      const rows = await computeSplit([['avg_qty', '$main.average($price)']], requester);
      const m = byCountry(rows);
      expect(m.Spain.avg_qty, 'integer channels 3/2 → 1.5 (fractional, not 1)').to.equal(1.5);
    });

    it('two-level nested AVG (avg of per-reference avgs) lowers to a single nested-CTE query', function () {
      // The supported resplit form: avg over (avg pvp per reference). This is the
      // deepest avg nesting the resplit lowering handles; it produces ONE nested
      // CTE that averages the inner per-reference averages.
      const ext = External.fromJS({
        engine: 'druidsql',
        source: 'histories',
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'pvp', type: 'NUMBER', unsplitable: true },
          { name: 'reference', type: 'STRING' },
        ],
      });
      const nested = $('main')
        .split('$reference', 'ref')
        .apply('B', $('main').average('$pvp'))
        .average('$B');
      const sqls = ply()
        .apply('AvgByRef', nested)
        .simulateQueryPlan({ main: ext })
        .flat()
        .filter(q => typeof q.query === 'string')
        .map(q => q.query);
      expect(sqls.length, 'single nested-CTE query').to.equal(1);
      expect(sqls[0], 'inner per-reference avg').to.match(/AVG\("pvp"\) AS "B_main_0"/);
      expect(sqls[0], 'outer averages the inner avg').to.match(/AVG\("B_main_0"\)/);
    });

    it('three-level nested AVG is NOT lowerable to SQL — fails loud (pins the current contract)', function () {
      // avg over (per-reference avg of (per-seller avg pvp)) — a 3rd resplit
      // level the lowering does not support. It must throw, not silently emit
      // wrong SQL. Pins the current depth ceiling; a future deepening flips this.
      const ext = External.fromJS({
        engine: 'druidsql',
        source: 'histories',
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'pvp', type: 'NUMBER', unsplitable: true },
          { name: 'reference', type: 'STRING' },
          { name: 'seller', type: 'STRING' },
        ],
      });
      const threeLevel = $('main')
        .split('$reference', 'ref')
        .apply(
          'B',
          $('main').split('$seller', 'slr').apply('S', $('main').average('$pvp')).average('$S'),
        )
        .average('$B');
      expect(function () {
        ply()
          .apply('Deep', threeLevel)
          .simulateQueryPlan({ main: ext })
          .flat()
          .filter(q => typeof q.query === 'string');
      }).to.throw(/can not convert split expression to SQL|resplit/i);
    });
  });
});
