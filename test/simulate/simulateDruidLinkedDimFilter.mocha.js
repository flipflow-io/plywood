/*
 * Wire-real reproduction of the LINKED-DIMENSION FILTER drop bug.
 *
 * Bug (reproduced in UI local + by wire 2026-06-02): a query whose filter has
 * a clause over a column that exists ONLY in a linked-source lookup — e.g.
 * `$brand_country.overlap(['Francia'])`, where `brand_country` lives only in
 * the `lookup_d01f07da…_rev1` lookup (joinKeys `['brand']`) — drops that
 * clause SILENTLY. The split, the totals (GROUP BY ()), and the pinboard all
 * come back with EVERY country instead of just Francia.
 *
 * Captured fixture: /tmp/replay-francia.json — the verbatim body the Turnilo
 * front POSTs to /plywood for team histories-0604cd31…, replayed through
 * `Expression.fromJS(body.expression)` exactly as the server route does. The
 * filter (`__time AND brand_country∈['Francia']`) is stamped on the main
 * `.filter()` AND on both magic linked-source applies (a14b8bf6, d01f07da).
 *
 * Root cause (confirmed by trace, see PR body):
 *   - `pruneLinkedFilterRefsInTree` correctly keeps the Francia clause only in
 *     the `magic_d01f07da` apply (the lookup that owns `brand_country`).
 *   - That apply is a sibling DATASET apply no output measure references, so
 *     `.simplify()` deletes it as dead code — taking the Francia clause.
 *   - At decomposition, `getCrossExternalDecomposition` seeds the lookup's
 *     `templateFilter` from `this.filter` (main's filter), which NEVER carried
 *     `brand_country` → the lookup sub-query emits NO WHERE → returns all
 *     countries → the inner join restricts nothing.
 *   - The totals query (GROUP BY ()) never decomposes at all → one SQL on
 *     histories with WHERE only on __time → all-country totals.
 *
 * Correct semantics: the linked-only clause must reach the lookup sub-query
 * (`WHERE brand_country = 'Francia'`); with `joinMode: inner` the join then
 * restricts main rows to Francia brands, so the split shows ONLY Francia and
 * the totals are computed over Francia rows only. A clause that cannot be
 * honoured is NEVER dropped silently — it is honoured or it throws.
 *
 * The External mirror reproduces the real cube: druidsql histories source +
 * an inner `brand`-keyed lookup carrying `brand_country` (the d01f07da one,
 * which drives the split) + a second `competitor`-keyed lookup (a14b8bf6) so
 * the reference-check of the fixture's second magic apply passes.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');
const fs = require('fs');

const plywood = require('../plywood');
const { External, Expression, $, r, ply } = plywood;

const WIRE = JSON.parse(fs.readFileSync('/tmp/replay-francia.json', 'utf8'));

// The two magic linked-source apply names in the wire fixture.
const D01 = 'magic_d01f07da-6a6f-41bc-9bf0-13ddb3bdc422'; // owns brand_country, joinKey brand
const A14 = 'magic_a14b8bf6-4f82-442f-bdc6-c0152c9eaf73'; // sibling, joinKey competitor

function makeMain(requester, mainFilter, d01JoinMode) {
  const value = {
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: '__time',
    allowEternity: true,
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'brand', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
      { name: 'pvp', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      [D01]: {
        source: 'lookup_d01f07da_rev1',
        joinKeys: ['brand'],
        autoInjectJoinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: d01JoinMode || 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'brand_country', type: 'STRING' },
        ],
      },
      [A14]: {
        source: 'lookup_a14b8bf6_rev1',
        joinKeys: ['competitor'],
        autoInjectJoinKeys: ['competitor'],
        sharedDimensions: ['competitor'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'competitor', type: 'STRING' },
          { name: 'competitor_group', type: 'STRING' },
        ],
      },
    },
  };
  if (mainFilter) value.filter = mainFilter;
  return External.fromJS(value, requester);
}

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

function planSqls(expression, mainFilter, d01JoinMode) {
  return expression
    .simulateQueryPlan({ main: makeMain(undefined, mainFilter, d01JoinMode) })
    .flat()
    .map(q => (typeof q === 'string' ? q : q && q.query))
    .filter(q => typeof q === 'string');
}

function lookupSubQuery(sqls) {
  return sqls.find(s => s.includes('lookup_d01f07da_rev1'));
}
function mainGroupBy1(sqls) {
  return sqls.find(
    s => s.includes('"histories"') && /GROUP BY 1\b/.test(s) && !s.includes('lookup_'),
  );
}
function totalsSubQuery(sqls) {
  return sqls.find(s => s.includes('"histories"') && /GROUP BY \(\)/.test(s));
}

// ── Split variant of the wire fixture: drop the totals applies and keep only
// the SPLIT apply, so a "totals-only" shape can also be exercised by stripping
// the SPLIT apply. We build them from the captured expression.
const FULL_EXPR = () => Expression.fromJS(WIRE.expression);

describe('Linked-only dimension filter (Francia wire fixture) must reach the lookup', () => {
  it('parses the captured wire expression: the linked-only Francia clause is present', () => {
    const ex = FULL_EXPR();
    const s = ex.toString();
    expect(s, 'd01 magic apply present').to.include(D01);
    expect(s, 'split on brand_country').to.include('brand_country: $brand_country');
    expect(s, 'Francia literal present').to.include('Francia');
  });

  describe('SPLIT path (brand_country + competitor)', () => {
    it('the lookup sub-query carries the Francia predicate (NOT silently dropped)', () => {
      const sqls = planSqls(FULL_EXPR());
      const lookup = lookupSubQuery(sqls);
      expect(lookup, 'lookup sub-query exists').to.exist;
      // The whole point: the Francia clause must reach the lookup WHERE.
      expect(lookup, 'lookup WHERE filters on Francia').to.match(/brand_country.*Francia|Francia/);
      expect(lookup, 'lookup not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
    });

    it('no main sub-query leaks the linked-only brand_country column', () => {
      const sqls = planSqls(FULL_EXPR());
      const main = mainGroupBy1(sqls);
      expect(main, 'main GROUP BY 1 sub-query exists').to.exist;
      expect(main, 'main does not reference brand_country').to.not.match(/brand_country/);
    });

    it('compute: split returns ONLY Francia with weighted avg over Francia rows', async () => {
      // Fixture engineered so the no-filter answer differs from the filtered one.
      // brands: B_FR (Francia), B_ES (España), B_IT (Italia).
      // main rows are per (brand, competitor); the lookup maps brand→country.
      // If Francia is honoured, only B_FR's rows survive the inner join.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_d01f07da_rev1')) {
          // Honest lookup: only Francia rows come back when WHERE Francia is present.
          if (/Francia/.test(sql)) {
            return Promise.resolve([{ __join_brand: 'B_FR', brand_country: 'Francia' }]);
          }
          // No filter → all brands/countries (the BUG behaviour).
          return Promise.resolve([
            { __join_brand: 'B_FR', brand_country: 'Francia' },
            { __join_brand: 'B_ES', brand_country: 'España' },
            { __join_brand: 'B_IT', brand_country: 'Italia' },
          ]);
        }
        if (sql.includes('"histories"') && /GROUP BY 1\b/.test(sql)) {
          // main leaves per (brand, competitor). The simulate plan projects, in
          // order: MIN(price)=min_price, SUM(price)=!T_0, COUNT(*)=!T_1,
          // SUM(pvp)=!T_2 (verified against the emitted SQL).
          //   B_FR: price sum 70 / count 10 → avg(price)=7 ; pvp sum 70 → avg=7
          //   B_ES: price sum 100 / count 100 → avg=1 ; pvp sum 200 → avg=2
          //   B_IT: price sum 150 / count 50 → avg=3 ; pvp sum 200 → avg=4
          return Promise.resolve([
            {
              '__join_brand': 'B_FR',
              'competitor': 'CA',
              'min_price': 7,
              '!T_0': 70,
              '!T_1': 10,
              '!T_2': 70,
            },
            {
              '__join_brand': 'B_ES',
              'competitor': 'CB',
              'min_price': 1,
              '!T_0': 100,
              '!T_1': 100,
              '!T_2': 200,
            },
            {
              '__join_brand': 'B_IT',
              'competitor': 'CC',
              'min_price': 3,
              '!T_0': 150,
              '!T_1': 50,
              '!T_2': 200,
            },
          ]);
        }
        // totals
        return Promise.resolve([{ avg_price: 0, avg_pvp: 0, min_price: 0, diff_with_pvp: 0 }]);
      });

      const result = await FULL_EXPR().compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;

      const countries = [...new Set(rows.map(r => r.brand_country))].sort();
      expect(countries, 'split shows ONLY Francia').to.deep.equal(['Francia']);

      const fr = rows.find(r => r.brand_country === 'Francia');
      expect(fr, 'Francia bucket present').to.exist;
      expect(fr.avg_price, 'Francia weighted avg(price) = 70/10').to.be.closeTo(7, 1e-9);
      expect(fr.avg_pvp, 'Francia weighted avg(pvp) = 70/10').to.be.closeTo(7, 1e-9);
      expect(fr.diff_with_pvp, 'Francia diff_with_pvp = (7-7)/7').to.be.closeTo(0, 1e-9);
      expect(fr.min_price, 'Francia min_price').to.equal(7);
      // Counterfactual: the unfiltered weighted avg(price) over all 3 brands is
      // (70+100+150)/(10+100+50) = 320/160 = 2 ≠ 7.
      expect(fr.avg_price, 'NOT the all-country avg (2)').to.not.be.closeTo(2, 1e-6);
    });

    // FIXED by COMMIT B (semijoin-to-root). The totals GROUP BY () query — no
    // linked split, no linked measure — used to drop the Francia clause. The
    // fix runs the lookup DISTINCT-brand sub-query first and restricts the
    // totals main with `main.brand IN (<Francia brands>)`.
    it('compute: TOTALS (datum root) restricted to Francia — semijoin-to-root brand IN-list', async () => {
      // A pure totals expression on the same main external: filter on the
      // linked-only `brand_country` clause + a main aggregate, NO split.
      const time = $('__time').overlap({
        start: new Date('2026-05-02T11:50:00.000Z'),
        end: new Date('2026-06-02T11:50:00.000Z'),
      });
      const francia = $('brand_country').overlap(['Francia']);
      const buildTotals = extra => {
        const cf = extra ? time.and(extra) : time;
        return ply()
          .apply('main', $('main').filter(cf))
          .apply(D01, $(D01).filter(cf))
          .apply('avg_price', '$main.average($price)');
      };

      // ── SIMULATE: the lookup carries Francia; the totals main carries the
      // brand restriction; no JOIN; the no-filter plan is unchanged.
      const sqls = buildTotals(francia)
        .simulateQueryPlan({ main: makeMain() })
        .flat()
        .map(q => (typeof q === 'string' ? q : q && q.query))
        .filter(q => typeof q === 'string');
      const lookup = sqls.find(s => s.includes('lookup_d01f07da_rev1'));
      expect(lookup, 'lookup DISTINCT sub-query emitted').to.exist;
      expect(lookup, 'lookup filters on Francia').to.match(/brand_country.*Francia|Francia/);
      expect(lookup, 'lookup projects the joinKey brand').to.match(/"brand"/);
      const totals = sqls.find(s => /AVG\("price"\)/.test(s) && /GROUP BY \(\)/.test(s));
      expect(totals, 'totals main GROUP BY () exists').to.exist;
      expect(totals, 'totals main restricts on brand').to.match(/"brand"/);
      expect(totals, 'no INNER JOIN on the totals main').to.not.match(/INNER JOIN/i);
      expect(totals, 'no brand_country leak in the totals main').to.not.match(/brand_country/i);

      // ── COMPUTE: honest engine returns Francia-only when the brand restriction
      // is present, all-country otherwise. The fix must yield the Francia value.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_d01f07da_rev1')) return Promise.resolve([{ brand: 'B_FR' }]);
        if (!/AVG\("price"\)/.test(sql)) return Promise.resolve([{ __VALUE__: 0 }]);
        if (/"brand"\s*(=|IN)/i.test(sql)) return Promise.resolve([{ __VALUE__: 7 }]); // Francia avg
        return Promise.resolve([{ __VALUE__: 2 }]); // all-country avg
      });
      const result = await buildTotals(francia).compute({ main: makeMain(req) });
      const js = result && typeof result.toJS === 'function' ? result.toJS() : result;
      const avg = typeof js.avg_price === 'number' ? js.avg_price : js.data && js.data[0].avg_price;
      expect(avg, 'FIX: totals restricted to Francia (7), not all-country (2)').to.equal(7);
    });
  });

  describe('exclusion variant — NOT overlap over the linked-only column', () => {
    it('a NOT(brand_country IN [Francia]) reaches the lookup (excludes Francia, not WHERE FALSE)', () => {
      // Build a clean split expression filtered with an EXCLUSION on the
      // linked-only column. The lookup that owns brand_country sees the negated
      // predicate; with the prune-to-identity rule for negations over
      // out-of-schema columns (0.51.1), the clause that survives onto the lookup
      // must reference brand_country and must NOT collapse to WHERE FALSE.
      const ex = $('main')
        .filter(
          $('__time')
            .overlap({
              start: new Date('2026-05-02T11:50:00.000Z'),
              end: new Date('2026-06-02T11:50:00.000Z'),
            })
            .and($('brand_country').overlap(['Francia']).not()),
        )
        .split({ brand_country: '$brand_country' }, 'main')
        .apply('avg_price', '$main.average($price)');
      const sqls = planSqls(ex);
      const lookup = lookupSubQuery(sqls);
      expect(lookup, 'lookup sub-query exists').to.exist;
      expect(lookup, 'exclusion reaches lookup (references brand_country)').to.match(
        /brand_country/,
      );
      expect(lookup, 'lookup not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
    });
  });

  describe('main-split-only variant — linked-only filter, split on the MAIN dim (brand)', () => {
    // FIXED by COMMIT B (semijoin-to-root). A linked-only filter with a split on
    // a MAIN dimension (and no linked split/measure) is the same orphaned-clause
    // shape as the totals gap: getCrossExternalDecomposition returns null (no
    // foreign apply/split to seed a sub-plan). The semijoin-to-root now runs the
    // lookup DISTINCT-brand sub-query and restricts the main split with
    // `main.brand IN (<Francia brands>)`, so the user-observable bug (linked-only
    // filter dropped on a main-dim split) is fixed here too.
    it('lookup carries Francia when split is a main dim — main split gets brand IN-list', () => {
      const ex = $('main')
        .filter(
          $('__time')
            .overlap({
              start: new Date('2026-05-02T11:50:00.000Z'),
              end: new Date('2026-06-02T11:50:00.000Z'),
            })
            .and($('brand_country').overlap(['Francia'])),
        )
        .split({ brand: '$brand' }, 'main')
        .apply('avg_price', '$main.average($price)');
      const sqls = planSqls(ex);
      const lookup = lookupSubQuery(sqls);
      expect(lookup, 'lookup sub-query exists').to.exist;
      expect(lookup, 'lookup carries Francia').to.match(/brand_country.*Francia|Francia/);
      expect(lookup, 'lookup projects the joinKey brand').to.match(/"brand"/);
      expect(lookup, 'lookup not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);

      const main = sqls.find(s => /AVG\("price"\)/.test(s) && /GROUP BY 1\b/.test(s));
      expect(main, 'main GROUP BY 1 sub-query exists').to.exist;
      expect(main, 'main split restricts on brand').to.match(/"brand"/);
      expect(main, 'main does not leak brand_country').to.not.match(/brand_country/);
      expect(main, 'no INNER JOIN on the main split').to.not.match(/INNER JOIN/i);
    });
  });

  describe('joinMode left — the linked-only filter must be honoured or throw clearly', () => {
    it('left join with a linked-only filter does not silently drop the predicate', () => {
      // With joinMode:left the unmatched main rows would survive the join, so an
      // inner-style WHERE on the lookup cannot restrict main. Pin the chosen
      // behaviour: EITHER the predicate reaches the lookup (honoured) OR a clear
      // PlywoodError fires. It must NOT silently emit a lookup with no WHERE.
      let threw = null;
      let sqls = null;
      try {
        sqls = planSqls(FULL_EXPR(), undefined, 'left');
      } catch (e) {
        threw = e;
      }
      if (threw) {
        expect(threw.message, 'clear plywood error mentions the linked filter').to.match(
          /linked|filter|brand_country|join/i,
        );
      } else {
        const lookup = lookupSubQuery(sqls);
        expect(lookup, 'lookup sub-query exists').to.exist;
        expect(lookup, 'left-join lookup still carries Francia (not dropped)').to.match(/Francia/);
      }
    });

    it('TOTALS semijoin-to-root with joinMode:left FAILS LOUD (IN-list is inner-only)', () => {
      // The semijoin-to-root models an INNER join as a main-side IN-list. A LEFT
      // join keeps orphan main rows, so an IN-list would silently DISCARD rows
      // the user asked to keep — it cannot express left semantics. The rescue
      // must throw a clear PlywoodUnsupportedNativeJoinShape, never silently
      // emit the all-country totals. Parity with the split left-join pin above.
      const time = $('__time').overlap({
        start: new Date('2026-05-02T11:50:00.000Z'),
        end: new Date('2026-06-02T11:50:00.000Z'),
      });
      const cf = time.and($('brand_country').overlap(['Francia']));
      const totals = ply()
        .apply('main', $('main').filter(cf))
        .apply(D01, $(D01).filter(cf))
        .apply('avg_price', '$main.average($price)');
      expect(() => {
        totals
          .simulateQueryPlan({ main: makeMain(undefined, undefined, 'left') })
          .flat()
          .map(q => (typeof q === 'string' ? q : q && q.query));
      }, 'left-join totals semijoin throws clearly').to.throw(/linked|filter|join|inner/i);
    });
  });
});
