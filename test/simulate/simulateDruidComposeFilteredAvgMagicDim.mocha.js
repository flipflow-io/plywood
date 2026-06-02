/*
 * Composition test — FILTERED average + magic-dimension (linked-only) split.
 *
 * Family: the "Índices vs Competencia" shape — `$main.filter(X).agg(...)`, the
 * single most common measure form in the real cube catalog (524 distinct
 * formulas / 274 cubes; e.g. `$main.filter($reviewsRating > 0).average(...)`,
 * `$main.filter($hasBuybox == "1").countDistinct(...)`,
 * `$main.filter($competitiveness.contains('Vendes más barato')).count()`).
 *
 * Goal (asked by Ismael): prove the THREE shipped fixes compose orthogonally on
 * a REAL query shape, not a synthetic one:
 *   (A) avg leaf decomposition  — `average($price)` over a linked-only split
 *       segregates into homomorphic SUM/COUNT leaves, re-aggregates post-join,
 *       recombines as a WEIGHTED average (not media-de-medias).
 *   (A') linked-only filter per-request — a query filter over `brand_country`
 *       (a column living only in the lookup) is harvested onto the lookup
 *       sub-query without mutating the shared External config.
 *   (B) eternal-time resolution — (covered by the sibling time-split test; here
 *       the lookup is `timeAlignment: 'eternal'` so its sub-query is time-free).
 *
 * THE COMPOSITION UNDER TEST: a measure that is itself a CONDITIONAL average —
 * `$main.filter(<promo>).average($price)`. The per-measure filter must survive
 * INTO the segregated leaves (so each leaf is
 * `SUM(CASE WHEN promo THEN price)` / `SUM(CASE WHEN promo THEN 1)`), and must
 * compose with a linked-only query filter (only one country, only promo rows).
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * HONEST FINDING (composition gap, NOT hidden) — see the `documents the gap`
 * block at the bottom. The brief literally asks for `$main.filter($promo)`
 * with `promo: BOOLEAN`. A BARE boolean ref used directly as the filter
 * predicate (`$main.filter($promo)`) THROWS `could not resolve $promo` the
 * moment the linked-only split forces leaf segregation — while the SAME
 * measure-filter works fine on a main-only split (no fan-out), and the explicit
 * comparison form `$main.filter($promo.is(true))` works on BOTH paths and
 * renders the desired `CASE WHEN ("promo"=TRUE)` leaves. The leaf External's
 * type context drops the bare boolean dimension during segregation; a
 * comparison expression resolves because its operands are projected/pruned
 * differently. Every real catalog filter is a comparison or a `.contains(...)`
 * (`== "1"`, `> 0`, `.contains('…')`), so production shapes are SAFE — but the
 * bare-boolean form is a genuine narrow gap, pinned below as a counterfactual.
 *
 * The primary tests therefore use `$main.filter($promo.is(true))` — semantically
 * identical to "filter by promo" and faithful to the real `$x == "v"` catalog
 * shape — and the gap is asserted explicitly so it is visible, not silently
 * routed around.
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

// Main cube + eternal magic-dim lookup (brand → brand_country). `promo` is a
// real BOOLEAN dimension; `price`/`pvp` are unsplitable measures.
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
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
        { name: 'promo', type: 'BOOLEAN' },
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
            { name: '__time', type: 'TIME' },
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

// The canonical conditional-average measure: average($price) over promo rows.
// Uses the explicit comparison form (the real catalog shape); the bare-ref form
// is pinned separately as the documented gap.
const avgPromoPrice = $('main').filter($('promo').is(true)).average('$price');
const avgAllPrice = $('main').average('$price');

// Turnilo-style top-level expression: scope registrations + a SPLIT apply by the
// linked-only dim, with an optional linked-only query filter on the main scope.
function buildSplitExpr(valueApplies, queryFilter) {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + valueApplies[0][0], 'descending').limit(100);
  const mainScope = queryFilter
    ? $('main').filter(timeFilter.and(queryFilter))
    : $('main').filter(timeFilter);
  return ply()
    .apply('main', mainScope)
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

function planSql(valueApplies, queryFilter) {
  return buildSplitExpr(valueApplies, queryFilter)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('Compose: FILTERED avg ($main.filter(promo).average) + magic-dim split', () => {
  describe('SQL shape — the measure filter enters BOTH leaves (not lost)', () => {
    it('conditional avg over a linked-only split → SUM(CASE..) + COUNT(CASE..) leaves', () => {
      const sqls = planSql([['avg_promo_price', avgPromoPrice]]);
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;

      // The conditional NUMERATOR leaf: SUM only over promo rows.
      expect(mainSql, 'filtered SUM leaf').to.match(
        /SUM\(CASE WHEN \("promo"=TRUE\) THEN "price" ELSE 0 END\) AS "!T_\d+"/,
      );
      // The conditional DENOMINATOR leaf: COUNT only over promo rows — NOT a
      // bare COUNT(*). If the filter were lost, this would be COUNT(*) and the
      // average would be wrong (numerator over promo rows / count over all rows).
      expect(mainSql, 'filtered COUNT leaf').to.match(
        /SUM\(CASE WHEN \("promo"=TRUE\) THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      // The filter MUST be in the leaves, never silently dropped to a plain
      // SUM("price")/COUNT(*) pair.
      expect(mainSql, 'no unconditional SUM leaf').to.not.match(/SUM\("price"\) AS "!T_\d+"/);
      expect(mainSql, 'no unconditional COUNT leaf').to.not.match(/COUNT\(\*\) AS "!T_\d+"/);

      // The un-decomposed ratio column must NOT appear (that was the avg-500 shape).
      expect(mainSql, 'no ratio column on main SQL').to.not.match(/\(SUM\([^)]*\)\*1\.0\/COUNT/);
      // The lookup is the time-free eternal sub-query; the main carries the
      // synthetic join key. No ORDER BY on the main (sort moves post-join).
      expect(mainSql, 'main projects __join_brand').to.match(/AS "__join_brand"/);
      expect(mainSql, 'no ORDER BY on the main leaf SQL').to.not.match(/ORDER BY/);
    });

    it('lookup sub-query is time-free (eternal) and well-formed; no mutilated SELECT anywhere', () => {
      const sqls = planSql([['avg_promo_price', avgPromoPrice]]);
      const lookupSql = sqls.find(s => s.includes('lookup_bc_rev1'));
      expect(lookupSql, 'lookup sub-query exists').to.exist;
      expect(lookupSql, 'lookup does not filter on __time (eternal)').to.not.match(/"__time"/);
      expect(lookupSql, 'lookup not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
      expect(lookupSql, 'lookup projects the join key').to.match(/"brand" AS "__join_brand"/);

      // Structural well-formedness across every emitted query.
      for (const sql of sqls) {
        expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
        expect(sql, 'no empty select item (double comma)').to.not.match(/,\s*,/);
        const selectCols = (sql.match(/ AS "/g) || []).length;
        const gb = sql.match(/GROUP BY ([\d,\s]+)/);
        if (gb) {
          const positions = gb[1].split(',').map(s => parseInt(s.trim(), 10));
          expect(
            selectCols,
            `GROUP BY references position ${Math.max(
              ...positions,
            )} but only ${selectCols} select items`,
          ).to.be.at.least(Math.max(...positions));
        }
      }
    });

    it('filtered avg + unfiltered avg in one panel → distinct conditional vs unconditional leaves', () => {
      // Both measures share the split; the segregator mints FOUR leaves:
      // conditional SUM/COUNT (promo) for avg_promo_price, and unconditional
      // SUM/COUNT for avg_all_price. Proof the filter is per-measure, not global.
      const sqls = planSql([
        ['avg_promo_price', avgPromoPrice],
        ['avg_all_price', avgAllPrice],
      ]);
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      expect(mainSql, 'conditional SUM leaf').to.match(
        /SUM\(CASE WHEN \("promo"=TRUE\) THEN "price" ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(mainSql, 'conditional COUNT leaf').to.match(
        /SUM\(CASE WHEN \("promo"=TRUE\) THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(mainSql, 'unconditional SUM leaf for the all-rows avg').to.match(
        /SUM\("price"\) AS "!T_\d+"/,
      );
      expect(mainSql, 'unconditional COUNT leaf for the all-rows avg').to.match(
        /COUNT\(\*\) AS "!T_\d+"/,
      );
    });
  });

  describe('Row-level correctness — weighted avg over PROMO rows only', () => {
    // Fixture engineered so three answers diverge for Spain:
    //   conditional weighted avg (correct)  ≠  media-de-medias  ≠  unfiltered avg.
    // Spain B1: promo SUM=100 COUNT=100 (avg 1) ; all SUM=300 COUNT=110
    // Spain B2: promo SUM=100 COUNT=1   (avg 100); all SUM=100 COUNT=1
    //   filtered weighted = (100+100)/(100+1) = 200/101 = 1.98019…
    //   media-de-medias   = (1 + 100)/2       = 50.5            (WRONG)
    //   unfiltered weighted = (300+100)/(110+1) = 400/111 = 3.6036…
    // France B3: promo SUM=70 COUNT=10 (avg 7) ; all SUM=70 COUNT=10
    function reqWithLeaves() {
      return promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B2', brand_country: 'Spain' },
            { __join_brand: 'B3', brand_country: 'France' },
          ]);
        }
        if (sql.includes('main_ds')) {
          // Leaf order matches the emitted SQL: avg_promo_price → !T_0=condSUM,
          // !T_1=condCOUNT; avg_all_price → !T_2=SUM, !T_3=COUNT(*).
          return Promise.resolve([
            { '__join_brand': 'B1', '!T_0': 100, '!T_1': 100, '!T_2': 300, '!T_3': 110 },
            { '__join_brand': 'B2', '!T_0': 100, '!T_1': 1, '!T_2': 100, '!T_3': 1 },
            { '__join_brand': 'B3', '!T_0': 70, '!T_1': 10, '!T_2': 70, '!T_3': 10 },
          ]);
        }
        return Promise.resolve([]);
      });
    }

    it('collapses to one row per country with the weighted PROMO avg (≠ media-de-medias, ≠ unfiltered)', async () => {
      const result = await buildSplitExpr([
        ['avg_promo_price', avgPromoPrice],
        ['avg_all_price', avgAllPrice],
      ]).compute({ main: makeMain(reqWithLeaves()) });
      const rows = result.toJS().data[0].SPLIT.data;

      const countries = rows.map(r => r.brand_country).sort();
      expect(countries, 'one row per country').to.deep.equal(['France', 'Spain']);

      const spain = rows.find(r => r.brand_country === 'Spain');
      const france = rows.find(r => r.brand_country === 'France');

      // The conditional weighted average over promo rows only.
      expect(spain.avg_promo_price, 'Spain promo weighted avg = 200/101').to.be.closeTo(
        200 / 101,
        1e-9,
      );
      // Counterfactual #1: NOT media-de-medias.
      expect(spain.avg_promo_price, 'NOT media-de-medias (50.5)').to.not.be.closeTo(50.5, 1e-6);
      // Counterfactual #2: the FILTER changes the answer — promo avg ≠ all avg,
      // computed side-by-side over the very same buckets.
      expect(spain.avg_all_price, 'Spain unfiltered weighted avg = 400/111').to.be.closeTo(
        400 / 111,
        1e-9,
      );
      expect(
        spain.avg_promo_price,
        'filtered avg ≠ unfiltered avg (the measure filter is load-bearing)',
      ).to.not.be.closeTo(spain.avg_all_price, 1e-6);

      // France: a single brand, promo and all coincide (avg 7).
      expect(france.avg_promo_price, 'France promo avg = 7').to.be.closeTo(7, 1e-9);

      // No synthetic leaf column leaks to the caller.
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
          expect(k, 'no __join_ leak').to.not.equal('__join_brand');
        }
      }
    });
  });

  describe('Triple composition — measure filter + linked-only query filter + decomposition', () => {
    it('filter brand_country=Spain reaches the lookup WHERE; measure stays conditional', () => {
      const sqls = planSql(
        [['avg_promo_price', avgPromoPrice]],
        $('brand_country').overlap(['Spain']),
      );
      const lookupSql = sqls.find(s => s.includes('lookup_bc_rev1'));
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(lookupSql, 'lookup sub-query exists').to.exist;
      expect(mainSql, 'main sub-query exists').to.exist;

      // Fix A' — the linked-only clause is harvested onto the lookup sub-query.
      expect(lookupSql, 'lookup WHERE filters on Spain').to.match(/brand_country.*Spain|Spain/);
      expect(lookupSql, 'lookup not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
      // The main leaf SQL must NOT leak the linked-only column…
      expect(mainSql, 'main does not reference brand_country').to.not.match(/brand_country/);
      // …and the measure filter is STILL conditional on the leaves (fixes A + A'
      // do not stomp each other).
      expect(mainSql, 'measure filter still in leaf').to.match(
        /SUM\(CASE WHEN \("promo"=TRUE\) THEN "price" ELSE 0 END\)/,
      );
    });

    it('compute: ONLY Spain, ONLY promo rows — the two filters AND together exactly', async () => {
      // Honest lookup: with WHERE Spain only Spain brands come back; without it,
      // all countries (the would-be bug behaviour) — so an unhonoured filter
      // would visibly leak France into the result.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          if (/Spain/.test(sql)) {
            return Promise.resolve([
              { __join_brand: 'B1', brand_country: 'Spain' },
              { __join_brand: 'B2', brand_country: 'Spain' },
            ]);
          }
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

      const result = await buildSplitExpr(
        [['avg_promo_price', avgPromoPrice]],
        $('brand_country').overlap(['Spain']),
      ).compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;

      // Only Spain survives the linked filter; France is excluded.
      const countries = [...new Set(rows.map(r => r.brand_country))].sort();
      expect(countries, 'split shows ONLY Spain').to.deep.equal(['Spain']);

      const spain = rows.find(r => r.brand_country === 'Spain');
      // …and the surviving value is the conditional weighted avg over promo rows.
      expect(spain.avg_promo_price, 'Spain promo weighted avg = 200/101').to.be.closeTo(
        200 / 101,
        1e-9,
      );
      // Counterfactual: had the linked filter been dropped, France's bucket (avg
      // 7) would also appear; had the measure filter been dropped, the value
      // would not be 200/101 (it would mix in non-promo rows).
      expect(rows.length, 'exactly one bucket').to.equal(1);
    });
  });

  describe('Orthogonality — main-dim split keeps the conditional avg native (no fan-out)', () => {
    it('split by the shared join key (brand) emits AVG(CASE..) natively, no leaf decomposition', () => {
      let split = $('main')
        .split('$brand', 'brand')
        .apply('avg_promo_price', avgPromoPrice)
        .apply('cnt_country', $('magic_bc').count());
      const sqls = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('SPLIT', split)
        .simulateQueryPlan({ main: makeMain() })
        .flat()
        .filter(q => typeof q.query === 'string')
        .map(q => q.query);
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      // No fan-out → the fix is inert: avg stays the pre-existing F2 rewrite (a
      // single ratio column), still carrying its CASE-WHEN filter in BOTH the
      // numerator and the denominator; no synthetic leaf columns minted.
      expect(mainSql, 'no synthetic leaf columns').to.not.match(/!T_\d+/);
      expect(mainSql, 'conditional avg rendered as a single native ratio column').to.match(
        /\(SUM\(CASE WHEN \("promo"=TRUE\) THEN "price" ELSE 0 END\)\*1\.0\/SUM\(CASE WHEN \("promo"=TRUE\) THEN 1 ELSE 0 END\)\) AS "avg_promo_price"/,
      );
    });
  });

  describe('HONEST FINDING — bare boolean ref in a measure filter is a narrow composition gap', () => {
    // The brief literally asks for `$main.filter($promo)` with promo:BOOLEAN.
    // That BARE-ref form throws the moment the linked-only split forces leaf
    // segregation — but works on a main-only split and is fully avoided by the
    // comparison form used above. Pinned as a counterfactual so the gap is
    // VISIBLE, not silently routed around. The fix (carry bare boolean dims into
    // the leaf type context) belongs to the next phase.
    const bareFiltered = $('main').filter('$promo').average('$price');

    it('bare $promo + linked-only split THROWS "could not resolve $promo" (the gap)', () => {
      expect(() => planSql([['m', bareFiltered]])).to.throw(/could not resolve \$promo/);
    });

    it('the SAME bare-$promo measure works on a MAIN-only split (gap is fan-out specific)', () => {
      const split = $('main').split('$brand', 'brand').apply('m', bareFiltered);
      const expr = ply().apply('main', $('main').filter(timeFilter)).apply('SPLIT', split);
      let sqls;
      expect(() => {
        sqls = expr
          .simulateQueryPlan({ main: makeMain() })
          .flat()
          .filter(q => typeof q.query === 'string')
          .map(q => q.query);
      }, 'no throw without fan-out').to.not.throw();
      const mainSql = sqls.find(s => s.includes('"main_ds"'));
      expect(mainSql, 'bare boolean renders as a CASE-WHEN avg natively').to.match(
        /AVG\(CASE WHEN \("promo" = TRUE\) THEN "price" END\)/,
      );
    });

    it('the comparison form $promo.is(true) is the safe equivalent on BOTH paths', () => {
      // Linked-only split: no throw, conditional leaves (asserted above).
      expect(() => planSql([['avg_promo_price', avgPromoPrice]])).to.not.throw();
    });
  });
});
