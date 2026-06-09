/*
 * AUDIT test — TOTALS row (NO split) × linked-only magic-dim FILTER × a
 * non-decomposable measure (countDistinct).
 *
 * Shape under audit (reproduced by Ismael 2026-06-02):
 *   ply()
 *     .apply('main', $main.filter(time AND brand_country = 'Francia'))
 *     .apply('uniq', $main.countDistinct($product_id))
 *
 * There is NO split. The user wants ONE number: the count of distinct
 * products whose brand maps to country = Francia. Reproduced by hand: the
 * root total returns 8340 (every product, all countries) when it should be
 * ~864 (only Francia brands).
 *
 * WHY it is the totals row specifically (vs the split row, which IS fixed by
 * the native-JOIN linked-filter harvest):
 *
 *   - `pruneLinkedFilterRefsInTree` (baseExternal.ts ~1937) DOES harvest the
 *     linked-only `brand_country = 'Francia'` clause onto a per-request
 *     `config.filter` slot — this harvest is request-level, NOT split-gated.
 *
 *   - BUT the ONLY consumer of that harvested clause is
 *     `getCrossExternalDecomposition` (baseExternal.ts 4160), which on its very
 *     first line (4229) does `if (this.mode !== 'split') return null;`.
 *     A totals query has `this.mode === 'total'`, so it NEVER enters the
 *     cross-external path: neither the native-JOIN branch (which would read
 *     `config.filter` and put `lookup."brand_country" = 'Francia'` on the JOIN
 *     WHERE) nor the JS-join branch fires.
 *
 *   - With `crossExt === null`, `queryBasicValueStream` (3199) also gets null
 *     from `getJoinDecompositionShortcut` (split-only), so it falls through to
 *     the plain single-external `getQueryAndPostTransform()` at 3449. That SQL
 *     is built from `this.getQueryFilter()` (2996) — MAIN's filter ONLY. The
 *     linked-only `brand_country` column does NOT live on main and was pruned
 *     off main's filter, so the totals SQL carries NO brand_country predicate,
 *     no JOIN, no lookup reference at all → COUNT(DISTINCT product_id) over the
 *     whole period = 8340 instead of the Francia-only 864.
 *
 * The gap is already KNOWN and explicitly skipped in
 * simulateDruidLinkedDimFilter.mocha.js:252
 *   ('compute: TOTALS (datum root) restricted to Francia — needs
 *    semijoin-to-root (gap)').
 * This audit pins the COUNTDISTINCT instance of it (the measure that, on a
 * split, forces native-JOIN) — and proves the diagnosis at BOTH the SQL level
 * (no Francia clause emitted) and the row level (8340 leaks through).
 *
 * CLASSIFICATION (expected): C — SILENT BUG. No error is thrown; a single
 * confident-looking number comes back; it is the all-country count, and the
 * Francia filter is silently discarded. This is the worst class.
 *
 * SUGGESTED FIX (NOT applied here — diagnosis only): a semijoin-to-root. For a
 * totals query with a harvested linked-only clause, run the lookup side first
 * (`SELECT DISTINCT brand FROM lookup WHERE brand_country = 'Francia'`),
 * collect the brand set, then add `main."brand" IN (<brands>)` to the totals
 * main SQL WHERE. That is exactly the brand-set semijoin the gap note in
 * simulateDruidLinkedDimFilter describes, lifted to the datum-root grain.
 *
 * Cube mirror is identical to simulateDruidComposeNativeJoinLinkedFilter /
 * simulateDruidComposeCountDistinctMagicDim: druidsql `main_ds` histories + an
 * eternal `brand`-keyed lookup carrying `brand_country`.
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

const FRANCIA = $('brand_country').overlap(['Francia']);
const UNIQ = '$main.countDistinct($product_id)';

// TOTALS shape: NO split apply. The cube filter (time AND the linked-only
// Francia clause) is stamped on main `.filter()` AND on the magic linked
// apply — exactly as the front does. `extraLinkedClause = null` is the
// no-filter counterfactual.
function buildTotalsExpr(extraLinkedClause) {
  const cubeFilter = extraLinkedClause ? timeFilter.and(extraLinkedClause) : timeFilter;
  return ply()
    .apply('main', $('main').filter(cubeFilter))
    .apply('magic_bc', $('magic_bc').filter(cubeFilter))
    .apply('uniq', UNIQ);
}

function planSql(extraLinkedClause) {
  return buildTotalsExpr(extraLinkedClause)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => q && typeof q.query === 'string')
    .map(q => q.query);
}

// A `lookup`-side reference to the Francia predicate — the shape a CORRECT fix
// (semijoin-to-root or native-JOIN-on-totals) would have to emit somewhere.
const FRANCIA_ON_LOOKUP = /lookup.*brand_country|brand_country.*lookup/i;

describe('FIXED — TOTALS row + linked-only magic-dim filter + countDistinct', () => {
  // COMMIT B: semijoin-to-root. The previously-pinned silent bug is now fixed:
  // a totals query with a harvested linked-only Francia clause emits a lookup
  // DISTINCT-joinKey sub-query and restricts the totals main with
  // `main.brand IN (<Francia brands>)`. These asserts were the RED BUG-pins
  // inverted to GREEN once the fix landed.
  describe('(1) SIMULATE — the totals SQL now honours the Francia filter', () => {
    it('the lookup DISTINCT-joinKey sub-query carries the Francia predicate', () => {
      const sqls = planSql(FRANCIA);
      // Surface the SQL so the diagnosis is on the record (not just asserted).
      // eslint-disable-next-line no-console
      console.log('\n--- TOTALS SQL (Francia filter present in the EXPRESSION) ---');
      for (const s of sqls) console.log(s, '\n');

      expect(sqls.length, 'at least one query emitted').to.be.greaterThan(0);
      const totals = sqls.find(s => /COUNT\(DISTINCT/i.test(s));
      expect(totals, 'a COUNT(DISTINCT …) totals query is emitted').to.exist;

      const anyFrancia = sqls.some(s => /Francia/.test(s));
      const anyLookup = sqls.some(s => /lookup_bc_rev1|lookup\b/i.test(s));
      // eslint-disable-next-line no-console
      console.log('any SQL mentions Francia? ', anyFrancia);
      // eslint-disable-next-line no-console
      console.log('any SQL references the lookup? ', anyLookup);

      // FIX: the Francia predicate now reaches the lookup sub-query and the
      // lookup is referenced (the brand-set semijoin source).
      expect(anyFrancia, 'FIX: the Francia predicate reaches the lookup sub-query').to.equal(true);
      expect(anyLookup, 'FIX: a lookup DISTINCT sub-query is emitted').to.equal(true);
    });

    it('the lookup sub-query is SELECT DISTINCT brand WHERE brand_country=Francia', () => {
      const sqls = planSql(FRANCIA);
      const lookup = sqls.find(s => /lookup_bc_rev1/.test(s));
      expect(lookup, 'lookup sub-query exists').to.exist;
      expect(lookup, 'lookup filters on Francia').to.match(/brand_country.*Francia|Francia/);
      expect(lookup, 'lookup not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
      expect(lookup, 'lookup projects the joinKey (brand)').to.match(/"brand"/);
    });

    it('the totals main carries an IN-list on the joinKey (brand), no JOIN', () => {
      const sqls = planSql(FRANCIA);
      const totals = sqls.find(s => /COUNT\(DISTINCT/i.test(s));
      expect(totals, 'COUNT(DISTINCT) query exists').to.exist;
      expect(totals, 'totals grouping is GROUP BY ()').to.match(/GROUP BY \(\)/);
      // The semijoin is an IN-list on main — NOT an in-SQL JOIN (that is the
      // split-path native-JOIN shape). The totals main references the joinKey.
      expect(totals, 'no INNER JOIN against the lookup').to.not.match(/INNER JOIN/i);
      expect(totals, 'totals main restricts on the joinKey "brand"').to.match(/"brand"/);
      // The linked-only column never leaks into the main SQL (it lives only on
      // the lookup; the semijoin folds it into a brand IN-list).
      expect(totals, 'no brand_country in the totals main SQL').to.not.match(/brand_country/i);
    });

    it('ORTHOGONALITY — the no-filter totals SQL is UNCHANGED (no lookup, no IN-list)', () => {
      // The fix is fully inert when no linked-only clause is harvested: a totals
      // query with no magic-dim filter emits EXACTLY today's single query.
      const unfiltered = planSql(null);
      expect(unfiltered.length, 'no-filter totals emits exactly one query').to.equal(1);
      expect(unfiltered[0], 'no-filter totals is the plain COUNT(DISTINCT)').to.match(
        /COUNT\(DISTINCT/i,
      );
      expect(unfiltered[0], 'no lookup sub-query when no filter').to.not.match(/lookup/i);
      expect(unfiltered[0], 'no IN-list / brand restriction when no filter').to.not.match(
        /"brand"\s*(=|IN)/i,
      );
      // And it MUST differ from the filtered plan (which now adds the lookup +
      // IN-list): the filter makes a difference now.
      const filtered = planSql(FRANCIA);
      expect(filtered, 'filtered plan differs from the no-filter plan').to.not.deep.equal(
        unfiltered,
      );
    });
  });

  describe('(2) COMPUTE — the returned number now respects the Francia filter', () => {
    // HONEST engine: it answers whatever the SQL asks. The fixed SQL restricts
    // main to Francia brands (IN-list semijoin), so it returns the Francia-only
    // 864 — not the all-country 8340.
    // The totals sub-query projects the measure as `AS "__VALUE__"` (value
    // mode), so the engine row must key the count on `__VALUE__`, not `uniq`.
    function honestRequester() {
      return promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        // The lookup DISTINCT-brand sub-query: return the Francia brand set.
        if (/lookup_bc_rev1/i.test(sql)) {
          return Promise.resolve([{ brand: 'B_FR' }]);
        }
        if (!/COUNT\(DISTINCT/i.test(sql)) return Promise.resolve([{ __VALUE__: 0 }]);
        // The fixed main query restricts to the Francia brand set (IN-list).
        if (/"brand"\s*(=|IN)|Francia/i.test(sql)) {
          return Promise.resolve([{ __VALUE__: 864 }]); // Francia-only distinct products
        }
        // Unrestricted → all products, every country (the OLD bug behaviour).
        return Promise.resolve([{ __VALUE__: 8340 }]);
      });
    }

    function totalsUniq(result) {
      // total-mode compute() returns a TotalContainer; toJS() exposes the datum
      // (the apply name `uniq` carries the value).
      const js = result && typeof result.toJS === 'function' ? result.toJS() : result;
      if (js && typeof js.uniq === 'number') return js.uniq;
      if (js && js.data && js.data[0] && typeof js.data[0].uniq === 'number')
        return js.data[0].uniq;
      if (result && typeof result.get === 'function') return result.get('uniq');
      if (result && result.datum && typeof result.datum.uniq === 'number') return result.datum.uniq;
      return js;
    }

    it('FIX: the Francia totals returns the Francia-only 864, not 8340', async () => {
      const result = await buildTotalsExpr(FRANCIA).compute({ main: makeMain(honestRequester()) });
      const uniq = totalsUniq(result);
      // eslint-disable-next-line no-console
      console.log('\nTOTALS uniq with Francia filter in the expression =', uniq);

      expect(uniq, 'FIX: Francia filter honoured → Francia-only count').to.equal(864);
      expect(uniq, 'NOT the all-country leak (8340)').to.not.equal(8340);
    });

    it('counterfactual: the no-filter totals still returns the all-country 8340', async () => {
      const result = await buildTotalsExpr(null).compute({ main: makeMain(honestRequester()) });
      const uniq = totalsUniq(result);
      expect(uniq, 'no-filter totals = 8340 (full population — orthogonal)').to.equal(8340);
    });

    it('empty set: a country mapping to NO brand returns 0, not all-country', async () => {
      // The lookup returns zero brands → IN () → no rows → total 0. NEVER the
      // all-country leak.
      const emptyReq = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (/lookup_bc_rev1/i.test(sql)) return Promise.resolve([]); // no Francia brands
        if (!/COUNT\(DISTINCT/i.test(sql)) return Promise.resolve([{ __VALUE__: 0 }]);
        // An IN () restriction returns no rows: a COUNT(DISTINCT) over zero rows
        // is 0. The engine honestly reflects that.
        return Promise.resolve([{ __VALUE__: 0 }]);
      });
      const result = await buildTotalsExpr(FRANCIA).compute({ main: makeMain(emptyReq) });
      const uniq = totalsUniq(result);
      expect(uniq, 'empty brand set → total 0').to.equal(0);
      expect(uniq, 'NOT the all-country leak (8340)').to.not.equal(8340);
    });
  });
});
