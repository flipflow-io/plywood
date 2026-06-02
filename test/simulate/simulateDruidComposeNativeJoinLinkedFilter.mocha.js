/*
 * COMPOSITION test — LINKED-ONLY FILTER × native-JOIN route (countDistinct).
 *
 * Bug (reported by Ismael, reproduced by wire 2026-06-02): a panel that
 *   (a) splits by a LINKED-ONLY magic dimension (`brand_country`, joinKey
 *       `brand`, joinMode inner, timeAlignment eternal),
 *   (b) FILTERS on that same linked-only column (`brand_country = 'Francia'`),
 *   (c) measures a NON-decomposable aggregate (`countDistinct(productId)`),
 * silently DROPS the linked-only filter. The split returns EVERY country
 * (España, Francia, Reino Unido, …) with the same numbers as the no-filter
 * query — Francia is not isolated.
 *
 * Why it slips through: countDistinct has `decomposable: 'none'`, so the INV-2
 * gate diverts the whole panel to the NATIVE-JOIN path (`getNativeJoinDecomposition`),
 * a single SQL with an in-engine INNER JOIN. The v2 linked-filter fix
 * (`pruneLinkedFilterRefsInTree`) harvests the linked-only clause onto a
 * PER-REQUEST copy of the External (`this.linkedSources[lsName].filter`) and the
 * JS-join LEAF path (`getCrossExternalDecomposition`, `templateFilter`) reads it.
 * But the native-JOIN path builds its WHERE from `this.getQueryFilter()` (MAIN's
 * filter only) and never consults the harvested clause → the lookup side of the
 * JOIN carries no `brand_country` predicate → the inner join restricts nothing.
 *
 * Correct semantics: the harvested clause, already pruned to the lookup schema
 * (`brand_country = 'Francia'`), must reach the WHERE on the `lookup` side of the
 * JOIN. With joinMode inner the join then keeps only the main rows whose brand
 * maps to Francia → the split shows ONLY Francia, the countDistinct is over
 * Francia products only, and the datum-root total is Francia only. A clause that
 * cannot be evaluated by either side is NOT touched (existing behaviour).
 *
 * Orthogonality: a panel with NO linked-only filter must emit the SAME SQL as
 * before (no extra WHERE clause). Pinned by the no-filter counterfactual below
 * and by the existing simulateDruidComposeCountDistinctMagicDim suite.
 *
 * Canonical-cube mirror (same shape as simulateDruidComposeCountDistinctMagicDim):
 * druidsql `main_ds` histories + an eternal `brand`-keyed lookup carrying
 * `brand_country`. `product_id` feeds countDistinct; price/pvp feed the
 * derived RP/PVP ratio in the combined variant.
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

// Same External as the existing native-JOIN countDistinct suite.
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

// Turnilo stamps the FULL cube filter (time AND the linked-only clause) on the
// main `.filter()` AND on the magic linked-source apply — exactly as the front
// does. `extraLinkedClause` is the linked-only predicate; pass `null` for the
// no-filter counterfactual.
function buildSplitExpr(valueApplies, sortName, extraLinkedClause) {
  const cubeFilter = extraLinkedClause ? timeFilter.and(extraLinkedClause) : timeFilter;
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  return ply()
    .apply('main', $('main').filter(cubeFilter))
    .apply('magic_bc', $('magic_bc').filter(cubeFilter))
    .apply('SPLIT', split);
}

function planSql(valueApplies, sortName, extraLinkedClause) {
  return buildSplitExpr(valueApplies, sortName, extraLinkedClause)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

const FRANCIA = $('brand_country').overlap(['Francia']);
const UNIQ = '$main.countDistinct($product_id)';

// The dialect escapes the table alias when it renders the harvested clause
// (`setTable('lookup')` → `"lookup"."brand_country"`), while the native-JOIN
// code projects the split column with a hand-built unquoted alias
// (`lookup."brand_country"`). Both reference the same `lookup` join alias —
// allow the optional quotes so the assertion pins the SEMANTICS (Francia on the
// lookup side), not the incidental quoting of one render path.
const FRANCIA_ON_LOOKUP = /"?lookup"?\."brand_country"\s*=\s*'Francia'/;

describe('Native-JOIN linked-only FILTER (countDistinct) reaches the lookup side', () => {
  describe('(1) SIMULATE — the Francia predicate lands on the JOIN, not silently dropped', () => {
    it('the native-JOIN SQL applies brand_country=Francia on the LOOKUP side', () => {
      const sqls = planSql([['uniq', UNIQ]], 'uniq', FRANCIA);
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN against the lookup').to.match(/INNER JOIN/i);
      expect(sql, 'COUNT(DISTINCT …) rendered').to.match(/COUNT\(DISTINCT/i);
      // THE BUG: the Francia predicate must be present in the SQL.
      expect(sql, 'Francia predicate present in the JOIN SQL').to.match(
        /brand_country.*Francia|Francia/,
      );
      // And it must be on the LOOKUP side (the column lives only on the lookup).
      expect(sql, 'Francia qualified to the lookup alias').to.match(FRANCIA_ON_LOOKUP);
      // The split key is still projected from the lookup.
      expect(sql, 'split column projected from lookup').to.match(
        /lookup\."brand_country" AS "brand_country"/,
      );
      // Still GROUP BY 1, ORDER BY a projected alias, no mutilation.
      expect(sql, 'GROUP BY 1').to.match(/GROUP BY 1\b/);
      expect(sql, 'ORDER BY uniq projected').to.include('AS "uniq"');
      expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
      expect(sql, 'no WHERE FALSE (clause not collapsed)').to.not.match(/WHERE\s+FALSE/i);
    });

    it('no main sub-query carries the linked-only brand_country column', () => {
      // The native-JOIN is one SQL; the only `brand_country` reference is on the
      // lookup side (split + WHERE). The MAIN table (`main."..."`) must never
      // reference brand_country — it does not have that column.
      const sqls = planSql([['uniq', UNIQ]], 'uniq', FRANCIA);
      const sql = sqls[0];
      expect(sql, 'no main-side brand_country').to.not.match(/main\."brand_country"/);
    });

    it('ORTHOGONALITY — NO linked-only filter emits the SAME SQL with no Francia WHERE', () => {
      // Counterfactual at the SQL level: drop the linked-only clause and the SQL
      // must NOT gain a brand_country WHERE. The fix is inert without a harvested
      // clause (a no-filter panel does not change shape).
      const filtered = planSql([['uniq', UNIQ]], 'uniq', FRANCIA)[0];
      const unfiltered = planSql([['uniq', UNIQ]], 'uniq', null)[0];
      expect(unfiltered, 'no-filter SQL has no Francia clause').to.not.match(/Francia/);
      expect(unfiltered, 'no-filter SQL has no lookup brand_country WHERE').to.not.match(
        /"?lookup"?\."brand_country"\s*=/,
      );
      // The two SQLs differ ONLY by the added lookup-side predicate.
      expect(filtered, 'filtered SQL adds exactly the Francia lookup predicate').to.match(
        FRANCIA_ON_LOOKUP,
      );
      expect(
        filtered.replace(/\s*AND\s+\("?lookup"?\."brand_country"\s*=\s*'Francia'\)/, ''),
        'stripping the Francia predicate recovers the unfiltered SQL',
      ).to.equal(unfiltered);
    });
  });

  describe('(2) COMPUTE — the split returns ONLY Francia, countDistinct over Francia only', () => {
    // The engine does the JOIN + GROUP BY and returns the final per-bucket rows.
    // An HONEST requester returns the rows the SQL's WHERE would actually yield:
    //   - with Francia in the WHERE → only the Francia bucket;
    //   - without it → all countries (the BUG behaviour).
    // This makes the test fail RED on the unfixed code (filter dropped → all
    // countries) and pass GREEN once the predicate reaches the lookup WHERE.
    function honestRequester() {
      return promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (!sql.includes('INNER JOIN')) return Promise.resolve([]);
        if (FRANCIA_ON_LOOKUP.test(sql)) {
          // Inner join restricted to Francia brands → only Francia products.
          return Promise.resolve([{ brand_country: 'Francia', uniq: 864 }]);
        }
        // Filter dropped → all countries, unrestricted counts (the bug).
        return Promise.resolve([
          { brand_country: 'España', uniq: 3329 },
          { brand_country: 'Francia', uniq: 864 },
          { brand_country: 'Reino Unido', uniq: 800 },
          { brand_country: 'Italia', uniq: 555 },
          { brand_country: 'Alemania', uniq: 444 },
          { brand_country: 'Portugal', uniq: 333 },
          { brand_country: 'Bélgica', uniq: 222 },
        ]);
      });
    }

    it('split shows ONLY Francia; uniq is the Francia-only distinct count', async () => {
      const ex = buildSplitExpr([['uniq', UNIQ]], 'uniq', FRANCIA);
      const result = await ex.compute({ main: makeMain(honestRequester()) });
      const rows = result.toJS().data[0].SPLIT.data;

      const countries = [...new Set(rows.map(r => r.brand_country))].sort();
      expect(countries, 'split shows ONLY Francia').to.deep.equal(['Francia']);
      const fr = rows.find(r => r.brand_country === 'Francia');
      expect(fr, 'Francia bucket present').to.exist;
      expect(fr.uniq, 'Francia distinct count = 864').to.equal(864);

      // Counterfactual: the no-filter answer is 7 countries with España=3329.
      const unfiltered = await buildSplitExpr([['uniq', UNIQ]], 'uniq', null).compute({
        main: makeMain(honestRequester()),
      });
      const ufRows = unfiltered.toJS().data[0].SPLIT.data;
      expect(ufRows.length, 'no-filter returns the full 7 countries (the bug shape)').to.equal(7);
      expect(
        ufRows.map(r => r.brand_country).sort(),
        'no-filter includes España and others',
      ).to.include('España');
    });
  });

  describe('(3) EXCLUSION variant — NOT(brand_country IN [Francia]) + countDistinct', () => {
    it('the exclusion reaches the lookup side (NOT, not WHERE FALSE)', () => {
      const sqls = planSql([['uniq', UNIQ]], 'uniq', FRANCIA.not());
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN').to.match(/INNER JOIN/i);
      // The negated predicate must reference brand_country on the lookup side …
      expect(sql, 'exclusion references lookup brand_country').to.match(
        /"?lookup"?\."brand_country"/,
      );
      expect(sql, 'exclusion still names Francia').to.match(/Francia/);
      // … negated, not affirmed: a NOT/IS NOT TRUE wrapper around the predicate.
      expect(sql, 'negation rendered (IS NOT TRUE / NOT)').to.match(/IS NOT TRUE|NOT/i);
      // … and must NOT collapse to WHERE FALSE / drop the clause.
      expect(sql, 'not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
    });

    it('compute: exclusion returns every country EXCEPT Francia', async () => {
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (!sql.includes('INNER JOIN')) return Promise.resolve([]);
        // Honest engine: the negated predicate is on the lookup → Francia excluded.
        if (/"?lookup"?\."brand_country"/.test(sql) && /Francia/.test(sql)) {
          return Promise.resolve([
            { brand_country: 'España', uniq: 3329 },
            { brand_country: 'Reino Unido', uniq: 800 },
          ]);
        }
        // Predicate dropped → Francia leaks back in (the bug).
        return Promise.resolve([
          { brand_country: 'España', uniq: 3329 },
          { brand_country: 'Francia', uniq: 864 },
          { brand_country: 'Reino Unido', uniq: 800 },
        ]);
      });
      const result = await buildSplitExpr([['uniq', UNIQ]], 'uniq', FRANCIA.not()).compute({
        main: makeMain(req),
      });
      const countries = result.toJS().data[0].SPLIT.data.map(r => r.brand_country);
      expect(countries, 'Francia excluded').to.not.include('Francia');
      expect(countries, 'other countries present').to.include('España');
    });
  });

  describe('(4) COMBINED — Francia filter + derived RP/PVP (divide) + countDistinct', () => {
    // All three at once: linked-only filter + a derived measure (root op divide,
    // rendered by the arithmetic-recursing renderAggregateSQL) + countDistinct.
    // The derived measure must still render, the countDistinct must render, AND
    // the Francia predicate must reach the lookup side.
    it('SIMULATE — one SQL with ratio + COUNT(DISTINCT) + Francia on the lookup', () => {
      const sqls = planSql(
        [
          ['rp', '$main.average($price) / $main.average($pvp)'],
          ['uniq', UNIQ],
        ],
        'uniq',
        FRANCIA,
      );
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN').to.match(/INNER JOIN/i);
      expect(sql, 'ratio rendered as floatDivision of AVGs').to.match(
        /\(AVG\(main\."price"\)\*1\.0\/AVG\(main\."pvp"\)\) AS "rp"/,
      );
      expect(sql, 'countDistinct co-present').to.match(/COUNT\(DISTINCT/i);
      expect(sql, 'Francia on the lookup side').to.match(FRANCIA_ON_LOOKUP);
      expect(sql, 'no synthetic leaf columns (native JOIN)').to.not.match(/!T_\d+/);
    });

    it('COMPUTE — Francia only; ratio and uniq are the Francia-only values', async () => {
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (!sql.includes('INNER JOIN')) return Promise.resolve([]);
        if (FRANCIA_ON_LOOKUP.test(sql)) {
          // Engine: Francia-only rows. RP = avg(price)/avg(pvp) = 7/10 = 0.7.
          return Promise.resolve([{ brand_country: 'Francia', rp: 0.7, uniq: 864 }]);
        }
        // Bug: all countries with their own ratios.
        return Promise.resolve([
          { brand_country: 'España', rp: 1.2, uniq: 3329 },
          { brand_country: 'Francia', rp: 0.7, uniq: 864 },
          { brand_country: 'Italia', rp: 0.9, uniq: 555 },
        ]);
      });
      const result = await buildSplitExpr(
        [
          ['rp', '$main.average($price) / $main.average($pvp)'],
          ['uniq', UNIQ],
        ],
        'uniq',
        FRANCIA,
      ).compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      expect(
        rows.map(r => r.brand_country),
        'only Francia',
      ).to.deep.equal(['Francia']);
      const fr = rows[0];
      expect(fr.uniq, 'Francia uniq').to.equal(864);
      expect(fr.rp, 'Francia ratio = 0.7').to.be.closeTo(0.7, 1e-9);
      // Counterfactual: España's ratio 1.2 must NOT appear.
      expect(
        rows.some(r => r.rp === 1.2),
        'no España ratio leaked',
      ).to.equal(false);
    });
  });
});
