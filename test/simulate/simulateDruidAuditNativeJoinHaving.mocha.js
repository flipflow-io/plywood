/*
 * CONTRACT — native-JOIN route × HAVING (filter on the VALUE of an aggregate
 * measure) over a linked-only magic-dimension split + countDistinct.
 *
 * SHAPE under test (the live-app shape): the user splits by a linked-only magic
 * dim (`brand_country`, joinKey `brand`, timeAlignment 'eternal'), asks for a
 * NON-decomposable measure (`countDistinct(concat(product_id, competitor))` —
 * trait 'none') which forces the panel onto the native-JOIN route, AND adds a
 * value filter on that measure: "only countries whose uniq > 100". In plywood,
 * a `.filter()` applied to a split External BEFORE `.limit()` folds into the
 * External's `havingFilter` (baseExternal `_addFilterExpression`, mode 'split':
 * `value.havingFilter = value.havingFilter.and(expression)`).
 *
 * CORRECT BEHAVIOR (commit A): the native-JOIN path applies the HAVING
 * POST-JOIN — an exact mirror of the jsJoin branch — NOT as an in-SQL Druid
 * HAVING. Rationale: the native-JOIN applies never pass through
 * `External.addExpression`, so `$uniq` is NOT absorbed as a column; emitting
 * `HAVING COUNT(DISTINCT …) > 137` would reference an alias the engine can't
 * resolve. Instead the executor keys the returned Dataset by `applyNames`, so
 * `Dataset.filter($uniq > 137)` runs by alias — identical to the jsJoin path's
 * `joined.filter(crossExt.postJoinHavingFilter)`.
 *
 *   - `getNativeJoinDecomposition` splits `this.havingFilter` by scope over the
 *     projectable names (applyNames + split keys) via `External.splitFilterByScope`.
 *     The post-projectable subset surfaces as `postJoinHavingFilter` in the
 *     return shape. A clause referencing a NON-projected ref fails loud
 *     (`PlywoodUnsupportedNativeJoinShape`) rather than silently dropping.
 *
 *   - LIMIT correctness: when a `postJoinHavingFilter` is present the combined
 *     SQL must NOT carry its inline `ORDER BY` / `LIMIT`. If the engine cut to
 *     LIMIT 100 BEFORE the post-join filter ran, surviving rows would starve.
 *     So sort/limit are STRIPPED from the SQL and surfaced as
 *     `postJoinSort` / `postJoinLimit`, applied (filter → sort → limit) in the
 *     executor. This mirrors the jsJoin branch (baseExternal ~5015-5028).
 *
 *   - The native-JOIN EXECUTE branch applies, in order:
 *       1. ds = ds.filter(postJoinHavingFilter)   (only buckets above threshold)
 *       2. ds = ds.sort(postJoinSort)
 *       3. ds = ds.limit(postJoinLimit)
 *     before `assertDatasetShape`. Mirror of the jsJoin branch (~3341-3349).
 *
 * Net effect: the user asks "countries with uniq > 100" and gets back ONLY the
 * countries whose uniq exceeds the threshold (Spain), not every country.
 *
 * CONTRAST (proves the fix is native-JOIN-local and orthogonal): the SAME
 * HAVING over a DECOMPOSABLE measure (sum) still takes the jsJoin path and the
 * HAVING is emitted as a Druid HAVING on the main sub-query — unchanged. And a
 * native-JOIN panel WITHOUT any HAVING emits SQL byte-identical to before
 * (inline ORDER BY / LIMIT retained) — the strip only fires when a having is
 * actually present.
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

const timeFilter = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-02T00:00:00Z'),
});

function makeMain(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'main_ds',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'competitor', type: 'STRING' },
        { name: 'product_id', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
      ],
      linkedSources: {
        magic_bc: {
          source: 'lookup_bc_rev1',
          joinKeys: ['brand'],
          autoInjectJoinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
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

// The canonical non-decomposable measure that forces the native-JOIN route.
const UNIQ = '$main.countDistinct($product_id.concat($competitor))';

// Build a split that carries a HAVING. The `.filter(predicate)` is applied
// BEFORE `.limit()` so it folds into the External's havingFilter (mode 'split'
// refuses to add a filter once a limit is present — _addFilterExpression).
function buildHavingSplitExpr(valueApplies, havingPredicate, sortName) {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.filter(havingPredicate); // <- becomes havingFilter
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

// A native-JOIN split WITHOUT any HAVING — for the orthogonality counterfactual.
function buildNoHavingSplitExpr(valueApplies, sortName) {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
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

describe('native-JOIN × HAVING (filter on aggregate value) — magic-dim + countDistinct', () => {
  describe('SQL emit — HAVING is applied POST-JOIN, inline sort/limit stripped', () => {
    it('countDistinct + HAVING(uniq > 137): ONE native-JOIN SQL, NO inline HAVING/ORDER BY/LIMIT', () => {
      // Threshold 137 chosen so it can't collide with the LIMIT (100).
      const expr = buildHavingSplitExpr([['uniq', UNIQ]], $('uniq').greaterThan(137), 'uniq');
      const sqls = planSql(expr);
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];

      // This IS the native-JOIN route (the precondition).
      expect(sql, 'INNER JOIN against the lookup').to.match(/INNER JOIN/i);
      expect(sql, 'COUNT(DISTINCT …) present').to.match(/COUNT\(DISTINCT/i);
      expect(sql, 'GROUP BY 1').to.match(/GROUP BY 1\b/);

      // The HAVING is applied POST-JOIN (Dataset.filter by alias), NOT in SQL:
      // $uniq is never absorbed as a column, so an in-SQL HAVING would be invalid.
      expect(sql, 'HAVING applied post-join, never emitted in SQL').to.not.match(/HAVING/i);
      expect(sql, 'the threshold literal 137 is not in the SQL').to.not.match(/\b137\b/);

      // LIMIT correctness: when a having moves post-join, the inline ORDER BY /
      // LIMIT MUST be stripped from the SQL (else the engine cuts to LIMIT 100
      // BEFORE the post-join filter and surviving rows starve). They are
      // surfaced as postJoinSort / postJoinLimit instead.
      expect(sql, 'inline ORDER BY stripped when HAVING present').to.not.match(/ORDER BY/i);
      expect(sql, 'inline LIMIT stripped when HAVING present').to.not.match(/LIMIT/i);
    });

    it('CONTRAST — the SAME HAVING over a DECOMPOSABLE measure (sum) IS honored in-SQL (jsJoin route)', () => {
      // sum is trait 'sum' → no native-JOIN; the jsJoin path pushes HAVING to the
      // main sub-query. Proves the native-JOIN fix did not regress the jsJoin route.
      const expr = buildHavingSplitExpr([['s', '$main.sum($price)']], $('s').greaterThan(100), 's');
      const sqls = planSql(expr);
      expect(sqls.length, 'jsJoin emits 2 sub-queries').to.equal(2);
      const anyHaving = sqls.some(s => /HAVING/i.test(s));
      expect(anyHaving, 'jsJoin route DOES emit a HAVING for the sum measure').to.equal(true);
      // No INNER JOIN on the main sub-query — confirms this is jsJoin, not native.
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      expect(mainSql, 'sum route is jsJoin (no native INNER JOIN)').to.not.match(/INNER JOIN/i);
    });

    it('mixed panel (avg decomposable + countDistinct none) + HAVING(uniq>100): native-JOIN, HAVING post-join', () => {
      // The 'none' leaf dominates → whole panel native-JOIN. HAVING on uniq is
      // applied post-join; inline sort/limit stripped; avg renders as AVG.
      const expr = buildHavingSplitExpr(
        [
          ['avg_price', '$main.average($price)'],
          ['uniq', UNIQ],
        ],
        $('uniq').greaterThan(100),
        'uniq',
      );
      const sqls = planSql(expr);
      expect(sqls.length, 'one native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'native-JOIN route (avg + countDistinct in one SQL)').to.match(/INNER JOIN/i);
      expect(sql, 'AVG present').to.match(/AVG\(main\."price"\)/);
      expect(sql, 'COUNT(DISTINCT) present').to.match(/COUNT\(DISTINCT/i);
      expect(sql, 'HAVING(uniq>100) applied post-join, not in SQL').to.not.match(/HAVING/i);
      expect(sql, 'inline ORDER BY stripped when HAVING present').to.not.match(/ORDER BY/i);
      expect(sql, 'inline LIMIT stripped when HAVING present').to.not.match(/LIMIT/i);
    });

    it('ORTHOGONALITY — native-JOIN WITHOUT any HAVING keeps its inline ORDER BY / LIMIT', () => {
      // The strip fires ONLY when a having is present. A no-having native-JOIN
      // panel emits exactly the SQL it did before commit A — inline sort/limit
      // retained, no behavior change.
      const expr = buildNoHavingSplitExpr([['uniq', UNIQ]], 'uniq');
      const sqls = planSql(expr);
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN against the lookup').to.match(/INNER JOIN/i);
      expect(sql, 'COUNT(DISTINCT …) present').to.match(/COUNT\(DISTINCT/i);
      // No having → inline sort + limit retained (Druid topN), no post-join strip.
      expect(sql, 'inline ORDER BY retained (no having)').to.match(/ORDER BY "uniq" DESC/i);
      expect(sql, 'inline LIMIT retained (no having)').to.match(/LIMIT 100/i);
      expect(sql, 'still no HAVING (none requested)').to.not.match(/HAVING/i);
    });
  });

  describe('Row-level compute — the HAVING IS applied post-join (only surviving buckets returned)', () => {
    it('requester returns France=5, Italy=80, Spain=150; HAVING(uniq>100) keeps ONLY Spain', async () => {
      // The engine ran the JOIN + GROUP BY and returned per-country counts.
      // With HAVING(uniq > 100), the correct user-visible result is exactly
      // [Spain]. The native-JOIN execute branch filters the rows post-join.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('INNER JOIN') && sql.includes('main_ds')) {
          return Promise.resolve([
            { brand_country: 'Spain', uniq: 150 },
            { brand_country: 'Italy', uniq: 80 },
            { brand_country: 'France', uniq: 5 },
          ]);
        }
        return Promise.resolve([]);
      });

      const expr = buildHavingSplitExpr([['uniq', UNIQ]], $('uniq').greaterThan(100), 'uniq');
      const result = await expr.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      const countries = rows.map(r => r.brand_country).sort();

      // CORRECT (Class A): only Spain (uniq=150) survives the HAVING. Italy (80)
      // and France (5) both violate uniq > 100 and are dropped post-join.
      expect(countries, 'HAVING applied — only Spain survives').to.deep.equal(['Spain']);
      expect(rows.length, '1 row returned (only Spain)').to.equal(1);

      // The surviving row carries the correct value.
      const spain = rows.find(r => r.brand_country === 'Spain');
      expect(spain, 'Spain present').to.exist;
      expect(spain.uniq, 'Spain uniq passes >100').to.equal(150);
      // The violating rows are gone.
      expect(
        rows.find(r => r.brand_country === 'Italy'),
        'Italy (uniq=80) dropped',
      ).to.not.exist;
      expect(
        rows.find(r => r.brand_country === 'France'),
        'France (uniq=5) dropped',
      ).to.not.exist;
    });

    it('compute does NOT throw — post-join HAVING is applied cleanly (no fail-loud)', async () => {
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('INNER JOIN') && sql.includes('main_ds')) {
          return Promise.resolve([
            { brand_country: 'Spain', uniq: 150 },
            { brand_country: 'France', uniq: 5 },
          ]);
        }
        return Promise.resolve([]);
      });
      const expr = buildHavingSplitExpr([['uniq', UNIQ]], $('uniq').greaterThan(100), 'uniq');
      let threw = null;
      let rows = null;
      try {
        const result = await expr.compute({ main: makeMain(req) });
        rows = result.toJS().data[0].SPLIT.data;
      } catch (e) {
        threw = e;
      }
      expect(threw, 'no PlywoodUnsupportedNativeJoinShape — the HAVING is supported').to.equal(
        null,
      );
      expect(rows && rows.length, 'France (uniq=5) dropped; only Spain survives').to.equal(1);
      expect(rows[0].brand_country, 'survivor is Spain').to.equal('Spain');
    });
  });
});
