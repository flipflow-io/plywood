/*
 * COMPOSITION suite — subquery-split / countDistinct-family measure × magic-dim
 * (linked-only) split. Asks the orthogonality question Ismael posed:
 *
 *   Do the three shipped fixes compose with the engine's pre-existing
 *   subquery-split measure family without biting each other?
 *
 *     (A) avg leaf decomposition + linked-only filter per-request (5a0a3dd)
 *     (B) time split + magic dim — eternal linked __time → main (658a23e)
 *     (·) the SUBQUERY-SPLIT measure family that already lives in 23 cubes
 *         (countDistinct via split, "nº de sellers", shares) plus the
 *         universal `countDistinct($productId.concat($competitor))` present
 *         in ALL 349 cubes.
 *
 * The measures in this family all resolve `decomposable: 'none'` (a
 * countDistinct, a quantile, or a raw `PIVOT_NESTED_AGG` SqlAggregate). When
 * any 'none' leaf appears under a linked-only split, the INV-2 gate flips the
 * whole panel to the NATIVE-JOIN path (single Druid SQL, in-engine JOIN). So
 * the real composition question is: does the native-JOIN renderer handle these
 * shapes — concat keys, mixed avg+countDistinct, time×country double split, raw
 * PIVOT_NESTED_AGG — or does the composition emit malformed SQL / wrong
 * cardinality / a 500?
 *
 * Three answers, all pinned below:
 *
 *   1. countDistinct over a concat key (`$productId.concat($competitor)`,
 *      `$seller.concat($competitor)`) COMPOSES: one well-formed native-JOIN SQL,
 *      INNER JOIN on brand, GROUP BY 1, COUNT(DISTINCT (a||b)), ORDER BY
 *      projected. avg (fix A) mixed in renders as AVG(...) (original form, not
 *      the buggy divide). Row-level correct: one row per country. ORTHOGONAL.
 *
 *   2. A raw `PIVOT_NESTED_AGG(...)` SqlAggregate (the literal subquery-split
 *      measure, op `sqlAggregate`) under a linked-only split FAILS LOUD with
 *      `PlywoodUnsupportedNativeJoinShape` — renderAggregateSQL throws on the
 *      unrenderable root op rather than dropping the SELECT item. This is a
 *      documented gap (subquery-split measures are not yet native-JOIN
 *      emittable), NOT a 500/garbage. Acceptable composition: it refuses
 *      cleanly.
 *
 *   3. ★ COMPOSITION BUG (fix B × countDistinct family) — see the dedicated
 *      `describe` at the bottom. A `[Time(Day) × brand_country]` double split
 *      with a countDistinct measure routes to native-JOIN, but the v1
 *      native-JOIN renderer projects ONLY the single linked-only split alias
 *      and emits `GROUP BY 1` — the Time(Day) bucket is SILENTLY DROPPED from
 *      both SELECT and GROUP BY. The result collapses every day into one
 *      per-country row and `uniq` becomes the ALL-PERIOD distinct count, not
 *      the per-day count the user asked for. No throw, no malformed SQL — a
 *      silently-wrong answer. Fix B (avg path) and the countDistinct family are
 *      each individually correct; their COMPOSITION over a double split is the
 *      defect. Pinned with the current (wrong) behavior asserted explicitly +
 *      a counterfactual `it` asserting the correct behavior (RED today).
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply, Expression } = plywood;

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

// Main cube espejo + eternal magic-dim lookup (joinKey brand, brand_country
// linked-only). Carries the attrs the catalog's subquery-split family needs:
// productId, seller, competitor (for concat keys), price (avg), promo (filter).
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
        { name: 'productId', type: 'STRING' },
        { name: 'seller', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
        { name: 'promo', type: 'BOOLEAN' },
      ],
      filter: timeFilter,
      linkedSources: {
        magic_bc: {
          source: 'lookup_bc_rev1',
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

// Single linked-only split by brand_country + the given value applies.
function buildSplitExpr(valueApplies, sortName) {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) {
    split = split.apply(name, typeof formula === 'string' ? formula : formula);
  }
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

function planSql(expr) {
  return expr
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('Compose subquery-split / countDistinct-family measure × magic-dim split', () => {
  describe('1. countDistinct over a concat key composes with a linked-only split (native-JOIN)', () => {
    it('nº URLs únicas — countDistinct($productId.concat($competitor)) → one well-formed native-JOIN SQL', () => {
      // The universal shape (all 349 cubes). countDistinct is a 'none' leaf →
      // the panel routes to native-JOIN. The concat key must render inside the
      // COUNT(DISTINCT ...), not collapse the SELECT.
      const sqls = planSql(
        buildSplitExpr([['uniq', '$main.countDistinct($productId.concat($competitor))']]),
      );
      expect(sqls.length, 'single combined native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN against the lookup').to.match(/INNER JOIN .*lookup_bc_rev1/i);
      expect(sql, 'linked-only split projected').to.match(/AS "brand_country"/);
      // The concat key renders inside COUNT(DISTINCT (a||b)) — not dropped.
      expect(sql, 'countDistinct over the concat key').to.match(
        /COUNT\(DISTINCT \("main"\."productId"\|\|"main"\."competitor"\)\) AS "uniq"/,
      );
      // No mutilated SELECT, ORDER BY column projected, single-key GROUP BY.
      expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
      expect(sql, 'no empty select item').to.not.match(/,\s*,/);
      expect(sql, 'ORDER BY uniq is projected').to.include('AS "uniq"');
      expect(sql, 'GROUP BY on the single linked-only key').to.match(/GROUP BY 1\b/);
      // Counterfactual: a renderInnerRef that only handled bare refs would emit
      // COUNT(DISTINCT main."productId.concat(...)") or drop the column — the
      // concat-match assertion above would fail.
    });

    it('nº de sellers — countDistinct($seller.concat($competitor)) composes identically', () => {
      const sqls = planSql(
        buildSplitExpr([['nSellers', '$main.countDistinct($seller.concat($competitor))']]),
      );
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      expect(sqls[0], 'countDistinct over seller||competitor').to.match(
        /COUNT\(DISTINCT \("main"\."seller"\|\|"main"\."competitor"\)\) AS "nSellers"/,
      );
      expect(sqls[0], 'ORDER BY nSellers projected').to.include('AS "nSellers"');
    });

    it('compute: countDistinct(concat) collapses to one row per country', async () => {
      // Native-JOIN is a single SQL grouped by country in-engine, so the
      // requester returns the already-grouped rows. The decomposition must NOT
      // re-fan or duplicate them.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('histories_main') && sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { brand_country: 'Spain', uniq: 7 },
            { brand_country: 'France', uniq: 3 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildSplitExpr([['uniq', '$main.countDistinct($productId.concat($competitor))']]);
      const result = await ex.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      const countries = rows.map(r => r.brand_country).sort();
      expect(countries, 'one row per country').to.deep.equal(['France', 'Spain']);
      expect(rows.find(r => r.brand_country === 'Spain').uniq, 'Spain distinct count').to.equal(7);
      expect(rows.find(r => r.brand_country === 'France').uniq, 'France distinct count').to.equal(
        3,
      );
    });
  });

  describe('2. fix A (avg) × countDistinct family compose in a single native-JOIN', () => {
    it('avg + countDistinct(concat) — avg renders as AVG (original form, not the buggy divide)', () => {
      // The 'none' countDistinct leaf forces native-JOIN. avg must render as
      // AVG(...) directly (fix A passes ORIGINAL applies to native-JOIN), NOT
      // the avg-rewritten divide(sum,count) that renderAggregateSQL cannot
      // render (that was the original avg+magic-dim 500).
      const sqls = planSql(
        buildSplitExpr(
          [
            ['avg_price', '$main.average($price)'],
            ['uniq', '$main.countDistinct($productId.concat($competitor))'],
          ],
          'uniq',
        ),
      );
      expect(sqls.length, 'single native-JOIN SQL (the none-leaf gates the whole panel)').to.equal(
        1,
      );
      const sql = sqls[0];
      expect(sql, 'avg rendered as AVG, not dropped').to.match(
        /AVG\(main\."price"\) AS "avg_price"/,
      );
      expect(sql, 'no leaked divide/ratio column').to.not.match(/SUM\("price"\).*COUNT\(\*\)/);
      expect(sql, 'countDistinct(concat) projected').to.match(
        /COUNT\(DISTINCT \("main"\."productId"\|\|"main"\."competitor"\)\) AS "uniq"/,
      );
      // Every value apply contributes a SELECT column → no orphan ORDER BY.
      const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
      if (orderMatch) {
        expect(sql, `ORDER BY ${orderMatch[1]} projected`).to.include(`AS "${orderMatch[1]}"`);
      }
    });

    it('compute: avg + countDistinct(concat) → weighted-avg-equivalent per-country row from the native join', async () => {
      // Native-JOIN computes both measures in one in-engine GROUP BY, so the
      // requester returns the final per-country values directly. We assert the
      // decomposition does not mangle / re-fan them.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('histories_main') && sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { brand_country: 'Spain', avg_price: 1.98019, uniq: 9 },
            { brand_country: 'France', avg_price: 7, uniq: 4 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildSplitExpr(
        [
          ['avg_price', '$main.average($price)'],
          ['uniq', '$main.countDistinct($productId.concat($competitor))'],
        ],
        'uniq',
      );
      const result = await ex.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      expect(rows.length, 'two countries').to.equal(2);
      const spain = rows.find(r => r.brand_country === 'Spain');
      expect(spain.avg_price, 'Spain avg').to.be.closeTo(1.98019, 1e-4);
      expect(spain.uniq, 'Spain distinct').to.equal(9);
      // No synthetic leaf column leaks (native-JOIN mints no `!T_` leaves).
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
        }
      }
    });
  });

  describe('3. raw PIVOT_NESTED_AGG subquery-split measure × linked-only split FAILS LOUD (documented gap)', () => {
    it('a literal PIVOT_NESTED_AGG SqlAggregate throws PlywoodUnsupportedNativeJoinShape (no silent drop / 500)', () => {
      // The literal subquery-split shape (op `sqlAggregate`). Under a linked-only
      // split it routes to native-JOIN where renderAggregateSQL meets root
      // op='sqlAggregate' — no case → THROWS rather than returning null. The
      // composition refuses cleanly; it never emits a half-built SQL.
      const pna = 'PIVOT_NESTED_AGG(seller, COUNT(*) AS "sellerCount", COUNT(t.sub_key))';
      const sqlAgg = Expression.fromJS({
        op: 'sqlAggregate',
        operand: { op: 'ref', name: 'main' },
        sql: pna,
      });
      let split = $('main')
        .split('$brand_country', 'brand_country')
        .apply('nSellers', sqlAgg)
        .sort('$nSellers', 'descending')
        .limit(100);
      const expr = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('SPLIT', split);
      expect(() => planSql(expr), 'subquery-split measure refuses native-JOIN loudly').to.throw(
        /native-JOIN|sqlAggregate|PIVOT_NESTED_AGG|single native-JOIN aggregate/i,
      );
      // Counterfactual: if renderAggregateSQL returned null on the unknown op
      // (the pre-fix behavior), the SELECT item would be dropped and the SQL
      // would still ORDER BY "nSellers" → malformed SQL reaching the engine
      // (silent 500). The throw is the orthogonal, fail-loud composition.
    });
  });

  describe('4. orthogonality — countDistinct(concat) over a SHARED-dim split needs no cross-source path', () => {
    it('split by the shared join key (brand) keeps countDistinct(concat) a plain main-side aggregate', () => {
      // brand is shared → no linked-only fan-out → the cross-source gate is
      // inert. countDistinct(concat) must stay a single main-side query (no
      // native-JOIN minted, no synthetic join key).
      let split = $('main')
        .split('$brand', 'brand')
        .apply('uniq', '$main.countDistinct($productId.concat($competitor))')
        .sort('$uniq', 'descending')
        .limit(100);
      const expr = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('SPLIT', split);
      const sqls = planSql(expr);
      const mainSql = sqls.find(
        s => s.includes('"histories_main"') && !s.includes('lookup_bc_rev1'),
      );
      expect(mainSql, 'plain main sub-query exists').to.exist;
      expect(mainSql, 'countDistinct(concat) projected on main').to.match(/COUNT\(DISTINCT/i);
      expect(mainSql, 'no synthetic join key (no fan-out)').to.not.match(/__join_/);
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // ★ COMPOSITION BUG (FIXED) — fix B (Time × magic-dim double split) × the
  //   countDistinct family (any 'none'-trait measure → native-JOIN). The v1
  //   native-JOIN renderer dropped the main-side Time(Day) bucket from SELECT +
  //   GROUP BY, silently collapsing every day into one per-country row. The
  //   matrix flagged this; the fix carries EVERY split key (main-side buckets +
  //   linked key) into the combined SQL and the result Dataset. These tests now
  //   pin the CORRECT 2-D behavior. Counterfactual: revert the fix and the SQL
  //   reverts to `GROUP BY 1` (no TIME_FLOOR) and the compute test sees one
  //   all-period row with no `time` field — exactly the bug they were written to
  //   catch (verified RED on 658a23e before the fix).
  // ───────────────────────────────────────────────────────────────────────────
  describe('5. ★ FIXED: [Time(Day) × brand_country] + countDistinct keeps the time dimension', () => {
    function buildDoubleSplitExpr(valueApplies, sortName) {
      let split = $('main').split({
        time: '$__time.timeBucket(P1D)',
        brand_country: '$brand_country',
      });
      for (const [name, formula] of valueApplies) split = split.apply(name, formula);
      split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
      return ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('SPLIT', split);
    }

    it('emitted native-JOIN SQL projects BOTH the Time(Day) bucket and brand_country, GROUP BY 1, 2', () => {
      // Fix B makes `time` (the main timeAttribute) classify main-side against
      // the eternal lookup, so it is collected into mainSideSplitAliases and the
      // native-JOIN renderer projects TIME_FLOOR(...) AS "time" alongside
      // lookup.brand_country, grouping by BOTH positions. The Time(Day) bucket
      // is no longer dropped.
      const sqls = planSql(
        buildDoubleSplitExpr([['uniq', '$main.countDistinct($productId.concat($competitor))']]),
      );
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      // Both split keys projected.
      expect(sql, 'Time(Day) bucket projected via TIME_FLOOR').to.match(/TIME_FLOOR/i);
      expect(sql, '"time" alias projected').to.match(/AS "time"/);
      expect(sql, 'brand_country projected').to.match(/AS "brand_country"/);
      // The countDistinct(concat) measure still renders correctly.
      expect(sql, 'countDistinct over the concat key').to.match(
        /COUNT\(DISTINCT \("main"\."productId"\|\|"main"\."competitor"\)\) AS "uniq"/,
      );
      // GROUP BY covers BOTH the time bucket and the country key (≥2 positions),
      // never the mutilated `GROUP BY 1`.
      const gb = sql.match(/GROUP BY ([\d,\s]+)/);
      expect(gb, 'a positional GROUP BY exists').to.exist;
      const positions = gb[1].split(',').map(s => parseInt(s.trim(), 10));
      expect(
        Math.max(...positions),
        'GROUP BY must cover BOTH the time bucket and the country key',
      ).to.be.at.least(2);
      expect(positions.length, 'exactly two split-key positions').to.equal(2);
      // No mutilated SELECT.
      expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
      expect(sql, 'no empty select item').to.not.match(/,\s*,/);
    });

    it('compute: rows carry the `time` bucket AND brand_country — one row per (day, country)', async () => {
      // The engine now groups by (time, country), so the requester returns one
      // row per cell. The decomposition must inflate the `time` column to a
      // TimeRange and key the dataset on BOTH dimensions.
      const DAY1 = new Date('2026-05-01T00:00:00Z');
      const DAY2 = new Date('2026-05-02T00:00:00Z');
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('histories_main') && sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { time: DAY1, brand_country: 'Spain', uniq: 5 },
            { time: DAY2, brand_country: 'Spain', uniq: 4 },
            { time: DAY1, brand_country: 'France', uniq: 2 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildDoubleSplitExpr([
        ['uniq', '$main.countDistinct($productId.concat($competitor))'],
      ]);
      const result = await ex.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      // CORRECT cardinality: 3 distinct (day, country) cells — days NOT collapsed.
      expect(rows.length, 'three (day,country) cells').to.equal(3);
      // The `time` cell is a P1D TimeRange ({ start, end }); the Time dimension
      // is present on every row.
      for (const r of rows) {
        expect(
          Object.prototype.hasOwnProperty.call(r, 'time'),
          'the requested Time dimension is present on the row',
        ).to.equal(true);
        expect(r.time && r.time.start, 'time is an inflated TimeRange').to.exist;
      }
      const cell = (day, country) =>
        rows.find(r => +new Date(r.time.start) === +day && r.brand_country === country);
      expect(cell(DAY1, 'Spain').uniq, 'Day1/Spain per-day distinct count').to.equal(5);
      expect(cell(DAY2, 'Spain').uniq, 'Day2/Spain per-day distinct count').to.equal(4);
      expect(cell(DAY1, 'France').uniq, 'Day1/France per-day distinct count').to.equal(2);
    });

    it('avg (fix A) + countDistinct over [Time(Day) × country] composes — both measures, both split keys', () => {
      // The full composition: fix A (avg → AVG in native-JOIN) × fix B (time
      // bucket main-side) × the countDistinct family ('none' leaf → native-JOIN)
      // over a double split. All three orthogonal pieces in one SQL.
      const sqls = planSql(
        buildDoubleSplitExpr(
          [
            ['avg_price', '$main.average($price)'],
            ['uniq', '$main.countDistinct($productId.concat($competitor))'],
          ],
          'uniq',
        ),
      );
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'Time(Day) bucket projected').to.match(/TIME_FLOOR/i);
      expect(sql, 'avg rendered as AVG (not the buggy divide)').to.match(
        /AVG\(main\."price"\) AS "avg_price"/,
      );
      expect(sql, 'countDistinct(concat) projected').to.match(
        /COUNT\(DISTINCT \("main"\."productId"\|\|"main"\."competitor"\)\) AS "uniq"/,
      );
      const gb = sql.match(/GROUP BY ([\d,\s]+)/);
      const positions = gb[1].split(',').map(s => parseInt(s.trim(), 10));
      expect(positions.length, 'two split-key positions (time + country)').to.equal(2);
    });
  });
});
