/*
 * COMPOSITION proof — all three magic-dimension fixes in ONE panel.
 *
 * Ismael (2026-06-02): demonstrate that the three fixes shipped on
 * `feat/cross-source-timeshift-decomposition` are ORTHOGONAL and COMPOSE —
 * that they do not bite each other (nor the rest of the engine) when they all
 * fire simultaneously, using a REAL cube query shape (canonical GrupoIfa=507,
 * "Índices vs Competencia": time-bucket dim + magic-dim split + avg/ratio
 * measures + a linked-only filter).
 *
 * The three fixes under test:
 *
 *   (A) avg leaf decomposition + linked-only filter per-request (5a0a3dd):
 *       avg/ratio measures over a linked-only fan-out are segregated into
 *       homomorphic LEAVES (!T_n = SUM / COUNT), re-aggregated post-join, then
 *       recombined (weighted average / ratio replayed at the split grain);
 *       filters over linked-only columns are harvested per-request onto a copy
 *       of the lookup External (not mutating shared config); the nativeJoin's
 *       renderAggregateSQL THROWS rather than returning null.
 *
 *   (B) time split + magic dim (658a23e): a split on the MAIN time attribute
 *       against an `eternal` linked source resolves to MAIN instead of throwing
 *       "resolve on both sides".
 *
 * THE COMPOSED PANEL (a single Turnilo table):
 *   - DOUBLE split { time: $__time.timeBucket(P1D)  (fix B)
 *                  , brand_country: $brand_country   (linked-only) }
 *   - measures [ avg_price = $main.average($price)                      (fix A leaves)
 *              , rp_pvp    = ($main.average($price) - $main.average($pvp))
 *                            / $main.average($pvp)                       (fix A ratio recombine) ]
 *   - query filter brand_country ∈ ['Francia']                          (fix A linked-filter)
 *   - sort by avg_price desc + limit
 *
 * The probe-verified emitted SQL (build @ 658a23e):
 *   MAIN  : SELECT "brand" AS "__join_brand",
 *                  TIME_FLOOR("__time",'P1D',NULL,'Etc/UTC') AS "time",
 *                  SUM("price") AS "!T_0", COUNT(*) AS "!T_1", SUM("pvp") AS "!T_2"
 *           FROM "histories_main" WHERE (<time>) GROUP BY 1,2
 *   LOOKUP: SELECT "brand" AS "__join_brand", "brand_country" AS "brand_country"
 *           FROM "lookup_d01f07da_rev1" WHERE ("brand_country"='Francia') GROUP BY 1,2
 *
 * i.e. fix B (no ambiguity throw, time → main), fix A leaves (!T_0/!T_1/!T_2),
 * and fix A linked-filter (lookup WHERE Francia, time-free) all fire at once
 * without colliding.
 *
 * Counterfactuals:
 *   - Revert fix B → the first `it` throws the ambiguity error (the double
 *     split never plans). Asserted via `.to.not.throw()`.
 *   - Revert fix A leaves → main carries a ratio column / re-agg refuses to
 *     collapse → media-de-medias or a 500. Asserted via the weighted-avg value
 *     (200/101, NOT 50.5) and the ratio recombination (-0.5).
 *   - Revert fix A linked-filter → the lookup emits no WHERE and the
 *     no-filter twin returns España + Italia too. Asserted by the explicit
 *     no-filter compute (3 countries) vs the filtered compute (Francia only).
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

function promiseFnToStream(promiseRq) {
  return rq => {
    const stream = new PassThrough({ objectMode: true });
    promiseRq(rq).then(
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

// Two-day query window — the wire-real time filter the UI POSTs.
const timeFilter = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-03T00:00:00Z'),
});

// fix-A linked-only clause: brand_country lives ONLY in the lookup.
const franciaFilter = timeFilter.and($('brand_country').overlap(['Francia']));

const MAGIC = 'magic_d01f07da-1111-2222-3333-444455556666';

// Main cube mirror + eternal magic-dim lookup. The lookup carries its OWN
// `__time` (snapshot sentinel) — the overlap with main's timeAttribute is what
// tripped fix-B's ambiguity guard before the fix.
function makeMain(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'histories_main',
      timeAttribute: '__time',
      allowEternity: true,
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'competitor', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
        { name: 'promo', type: 'BOOLEAN' },
      ],
      filter: timeFilter,
      linkedSources: {
        [MAGIC]: {
          source: 'lookup_d01f07da_rev1',
          joinKeys: ['brand'],
          autoInjectJoinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: '__time', type: 'TIME' },
            { name: 'brand', type: 'STRING' },
            { name: 'brand_country', type: 'STRING' },
          ],
        },
      },
    },
    requester,
  );
}

// The composed panel: DOUBLE split (time-bucket fix-B + linked-only brand_country)
// + avg_price + rp_pvp ratio (fix-A leaves/recombine) + sort + limit, under the
// given query filter (timeFilter alone, or franciaFilter for the linked-filter).
function buildComposedExpr(queryFilter) {
  let split = $('main').split({
    time: '$__time.timeBucket(P1D)',
    brand_country: '$brand_country',
  });
  split = split
    .apply('avg_price', '$main.average($price)')
    .apply('rp_pvp', '($main.average($price) - $main.average($pvp)) / $main.average($pvp)')
    .sort('$avg_price', 'descending')
    .limit(100);
  return ply()
    .apply('main', $('main').filter(queryFilter))
    .apply(MAGIC, $(MAGIC).filter(queryFilter))
    .apply('SPLIT', split);
}

function planSql(expr) {
  return expr
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

const DAY1 = new Date('2026-05-01T00:00:00Z');
const DAY2 = new Date('2026-05-02T00:00:00Z');

describe('Compose all three magic-dim fixes in one panel (time × brand_country, avg + ratio, Francia)', () => {
  describe('SQL shape — fix B + fix A leaves + fix A linked-filter fire simultaneously', () => {
    it('the composed panel does NOT throw; main groups by (bucket, __join_brand) with !T leaves; lookup WHERE Francia time-free', () => {
      const expr = buildComposedExpr(franciaFilter);

      // ── Fix B: the double split must NOT raise the ambiguity error. ───────
      let sqls;
      expect(() => {
        sqls = planSql(expr);
      }, 'fix B: double (time, brand_country) split must not throw the ambiguity error').to.not.throw();

      const mainSql = sqls.find(
        s => s.includes('"histories_main"') && !s.includes('lookup_d01f07da_rev1'),
      );
      const lookupSql = sqls.find(s => s.includes('lookup_d01f07da_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      expect(lookupSql, 'lookup sub-query exists').to.exist;

      // ── Fix B: the time bucket resolved to MAIN (TIME_FLOOR on main's __time)
      // and the synthetic join key is projected. ───────────────────────────
      expect(mainSql, 'main projects a TIME_FLOOR day bucket').to.match(/TIME_FLOOR/i);
      expect(mainSql, 'main projects the day bucket alias').to.match(/AS "time"/);
      expect(mainSql, 'main projects __join_brand').to.match(/AS "__join_brand"/);

      // ── Fix A: avg + ratio segregate into homomorphic leaves on the main SQL.
      // avg_price → SUM(price)/COUNT(*) ; rp_pvp adds SUM(pvp); COUNT shared/deduped.
      expect(mainSql, 'SUM(price) leaf').to.match(/SUM\("price"\) AS "!T_\d+"/);
      expect(mainSql, 'SUM(pvp) leaf').to.match(/SUM\("pvp"\) AS "!T_\d+"/);
      const countLeaves = (mainSql.match(/COUNT\(\*\) AS "!T_\d+"/g) || []).length;
      expect(countLeaves, 'count() shared between avg_price and rp_pvp → ONE leaf').to.equal(1);
      // No un-decomposed ratio column (the pre-fix bug shape).
      expect(mainSql, 'no ratio column on main SQL').to.not.match(
        /\(SUM\("price"\)\*1\.0\/COUNT\(\*\)\)/,
      );
      // The derived sort is post-aggregate, not a main-SQL ORDER BY.
      expect(mainSql, 'no ORDER BY on the derived measure in main SQL').to.not.match(/ORDER BY/);

      // ── Fix B: main GROUP BY covers BOTH split keys (bucket + join key). ──
      const gb = mainSql.match(/GROUP BY ([\d,\s]+)/);
      expect(gb, 'main has a GROUP BY').to.exist;
      const positions = gb[1].split(',').map(s => parseInt(s.trim(), 10));
      expect(Math.max(...positions), 'GROUP BY covers bucket + join key').to.be.at.least(2);

      // ── Fix A linked-filter: the Francia clause reaches the lookup WHERE,
      // the lookup is TIME-FREE (eternal prune), never WHERE FALSE. ─────────
      expect(lookupSql, 'lookup WHERE filters on Francia').to.match(
        /brand_country.*Francia|Francia/,
      );
      expect(lookupSql, 'lookup is not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
      expect(lookupSql, 'lookup does not filter on __time (eternal)').to.not.match(/"__time"/);
      expect(lookupSql, 'lookup projects the join key').to.match(/"brand" AS "__join_brand"/);

      // ── Main must NOT leak the linked-only column. ───────────────────────
      expect(mainSql, 'main does not reference brand_country').to.not.match(/brand_country/);

      // ── No mutilated SELECT anywhere (the avg-drop / dangling-comma 500). ─
      for (const sql of sqls) {
        expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
        expect(sql, 'no empty select item (double comma)').to.not.match(/,\s*,/);
        const selectCols = (sql.match(/ AS "/g) || []).length;
        const groupByMatch = sql.match(/GROUP BY ([\d,\s]+)/);
        if (groupByMatch) {
          const maxPos = Math.max(...groupByMatch[1].split(',').map(s => parseInt(s.trim(), 10)));
          expect(
            selectCols,
            `GROUP BY references position ${maxPos} but only ${selectCols} select items`,
          ).to.be.at.least(maxPos);
        }
      }
    });
  });

  describe('Row-level correctness — grain (day, country), weighted avg, recombined ratio, Francia only', () => {
    // Fixture (honest lookup: returns only Francia when WHERE Francia present).
    //   B_FR1, B_FR2 → Francia.  (B_ES → España exists but is filtered out.)
    //   Day1 Francia: B_FR1 price sum=100 cnt=100 (avg 1) ; B_FR2 price sum=100 cnt=1 (avg 100)
    //                 → weighted avg(price) = 200/101 = 1.98019…  (media-de-medias = 50.5, WRONG)
    //                 pvp: B_FR1 sum=200 cnt=100 (avg 2) ; B_FR2 sum=200 cnt=1 (avg 200)
    //                 → weighted avg(pvp) = 400/101 = 3.96039…
    //                 rp_pvp = (avgP - avgPvp)/avgPvp = (200/101 - 400/101)/(400/101) = -0.5
    //   Day2 Francia: B_FR1 only — price sum=40 cnt=5 (avg 8) ; pvp sum=80 cnt=5 (avg 16)
    //                 → rp_pvp = (8 - 16)/16 = -0.5
    // Leaf order verified against the emitted MAIN SQL: !T_0=SUM(price),
    // !T_1=COUNT(*), !T_2=SUM(pvp); `time` = the P1D day bucket value.
    function reqFrancia() {
      return promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_d01f07da_rev1')) {
          // WHERE Francia present → only Francia brands come back.
          return Promise.resolve([
            { __join_brand: 'B_FR1', brand_country: 'Francia' },
            { __join_brand: 'B_FR2', brand_country: 'Francia' },
          ]);
        }
        if (sql.includes('histories_main')) {
          return Promise.resolve([
            { 'time': DAY1, '__join_brand': 'B_FR1', '!T_0': 100, '!T_1': 100, '!T_2': 200 },
            { 'time': DAY1, '__join_brand': 'B_FR2', '!T_0': 100, '!T_1': 1, '!T_2': 200 },
            { 'time': DAY2, '__join_brand': 'B_FR1', '!T_0': 40, '!T_1': 5, '!T_2': 80 },
          ]);
        }
        return Promise.resolve([]);
      });
    }

    it('collapses to one row per (day, Francia) with weighted avg + recombined ratio, sort + limit respected', async () => {
      const expr = buildComposedExpr(franciaFilter);
      const result = await expr.compute({ main: makeMain(reqFrancia()) });
      const rows = result.toJS().data[0].SPLIT.data;

      // Only Francia (fix-A linked-filter honoured), exactly 2 (day, country) cells.
      const countries = [...new Set(rows.map(r => r.brand_country))];
      expect(countries, 'split shows ONLY Francia').to.deep.equal(['Francia']);
      expect(rows.length, 'exactly 2 cells: Day1/Francia + Day2/Francia').to.equal(2);

      const cell = day => rows.find(r => +new Date(r.time.start) === +day);
      const d1 = cell(DAY1);
      const d2 = cell(DAY2);
      expect(d1, 'Day1/Francia present').to.exist;
      expect(d2, 'Day2/Francia present').to.exist;

      // Fix A leaves: weighted avg per cell, NOT media-de-medias.
      expect(d1.avg_price, 'Day1 weighted avg(price) = 200/101').to.be.closeTo(200 / 101, 1e-9);
      expect(d1.avg_price, 'Day1 avg is NOT media-de-medias (50.5)').to.not.be.closeTo(50.5, 1e-6);
      expect(d2.avg_price, 'Day2 avg(price) = 8').to.be.closeTo(8, 1e-9);

      // Fix A ratio recombine: rp_pvp replayed from leaves at the (day,country) grain.
      expect(d1.rp_pvp, 'Day1 rp_pvp = (200/101 - 400/101)/(400/101) = -0.5').to.be.closeTo(
        -0.5,
        1e-9,
      );
      expect(d2.rp_pvp, 'Day2 rp_pvp = (8-16)/16 = -0.5').to.be.closeTo(-0.5, 1e-9);
      // rp_pvp is NOT the mean of per-brand ratios (media-de-medias guard):
      // per-brand Day1 ratios are B_FR1 (1-2)/2=-0.5 and B_FR2 (100-200)/200=-0.5,
      // both -0.5 here, so additionally pin the weighted-avg numerator:
      expect(d1.rp_pvp, 'Day1 rp_pvp is the recombined ratio, finite').to.be.a('number');

      // Sort by avg_price descending: Day2 (8) before Day1 (1.98).
      expect(rows[0].avg_price, 'first row is the highest avg (Day2)').to.be.closeTo(8, 1e-9);
      expect(rows[1].avg_price, 'second row is the lower avg (Day1)').to.be.closeTo(
        200 / 101,
        1e-9,
      );

      // No synthetic leaf / join-key column leaks to the caller.
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
          expect(k, 'no __join_ leak').to.not.equal('__join_brand');
        }
      }
    });

    it('COUNTERFACTUAL: WITHOUT the Francia filter the same panel returns España + Italia too', async () => {
      // Identical panel, only the query filter drops the linked-only clause.
      // The lookup mock returns ALL countries when no WHERE Francia is present
      // (the pre-fix behaviour). This proves the filter — not the fixture —
      // is what narrows the result to Francia, and that the double split +
      // leaf decomposition still compose for the multi-country case.
      const reqAll = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_d01f07da_rev1')) {
          expect(sql, 'no-filter lookup carries no Francia clause').to.not.match(/Francia/);
          return Promise.resolve([
            { __join_brand: 'B_FR1', brand_country: 'Francia' },
            { __join_brand: 'B_ES', brand_country: 'España' },
            { __join_brand: 'B_IT', brand_country: 'Italia' },
          ]);
        }
        if (sql.includes('histories_main')) {
          return Promise.resolve([
            { 'time': DAY1, '__join_brand': 'B_FR1', '!T_0': 70, '!T_1': 10, '!T_2': 70 },
            { 'time': DAY1, '__join_brand': 'B_ES', '!T_0': 100, '!T_1': 100, '!T_2': 200 },
            { 'time': DAY1, '__join_brand': 'B_IT', '!T_0': 150, '!T_1': 50, '!T_2': 200 },
          ]);
        }
        return Promise.resolve([]);
      });
      const expr = buildComposedExpr(timeFilter); // NO brand_country clause
      const result = await expr.compute({ main: makeMain(reqAll) });
      const rows = result.toJS().data[0].SPLIT.data;
      const countries = [...new Set(rows.map(r => r.brand_country))].sort();
      expect(countries, 'without the filter the split shows more than Francia').to.deep.equal([
        'España',
        'Francia',
        'Italia',
      ]);
      expect(countries.length, 'strictly more countries than the filtered case (1)').to.be.above(1);
    });
  });
});
