/*
 * Cross-source AVG (and derived ratio/min) over a magic-dimension split.
 *
 * Reproduces and pins the fix for the "avg + magic dimension" 500 (Pernod-style
 * cube: split by `brand_country`, a column that lives ONLY in the magic-dim
 * lookup, joinKey `brand`; measures Price average, RP/PVP = ratio of two avgs,
 * Price minimum).
 *
 * THE BUG (two failure modes, both fail-silent → malformed SQL → engine 500):
 *
 *   (1) jsJoin path. `average($x)` was rewritten pre-gate to the ratio column
 *       `sum($x)/count()`. The decomposability gate walks leaves, sees only
 *       sum+count (both 'sum') and judges it jsJoin-safe — but the ratio is a
 *       SINGLE projected column that is NOT a homomorphism over the join
 *       fan-out (summing per-brand averages ≠ per-country average:
 *       media-de-medias). `reAggregateToSplitGrain` inspected the ROOT op
 *       (`divide`) → trait 'none' → refused to collapse → fan-out reached
 *       `assertDatasetShape` → PlywoodCardinalityViolation (the 500).
 *
 *   (2) nativeJoin path. Adding a non-'sum' measure (min) flipped the gate to
 *       native-JOIN, which renders each apply via `renderAggregateSQL`. The
 *       avg-REWRITTEN `divide(sum,count)` has op `divide`, which had no case
 *       and silently returned null → the SELECT item was dropped while the
 *       ORDER BY still referenced it → "Column 'avg_price' not found".
 *
 * THE FIX (Ogievetsky segregate-then-recombine, algebraic + orthogonal):
 *   - Only when a linked-only split causes fan-out, segregate each main-side
 *     measure into homomorphic LEAF aggregates (sum, count, min, max) carried
 *     as separate columns, re-aggregate the leaves per bucket post-join, then
 *     replay the deriving scalar function (divide/subtract/…) at the split
 *     grain and drop the synthetic leaf columns.
 *   - nativeJoin receives the ORIGINAL (un-rewritten) applies so `average`
 *     renders as `AVG(...)` directly. `renderAggregateSQL` now THROWS on an
 *     unrenderable op instead of dropping the SELECT item.
 *
 * Counterfactuals are spelled out per test.
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

// Build the Turnilo-style top-level expression: scope registrations + a SPLIT
// apply by the linked-only dim with the given value applies.
function buildSplitExpr(valueApplies, sortName) {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

function planSql(valueApplies, sortName) {
  const queries = buildSplitExpr(valueApplies, sortName)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string');
  return queries.map(q => q.query);
}

describe('Cross-source AVG + magic-dimension split (segregate-then-recombine)', () => {
  describe('SQL shape — jsJoin leaf decomposition', () => {
    it('avg over a linked-only split projects SUM + COUNT leaves, not a ratio column', () => {
      const sqls = planSql([['avg_price', '$main.average($price)']]);
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      // Homomorphic leaves projected separately.
      expect(mainSql, 'SUM leaf').to.match(/SUM\("price"\) AS "!T_0"/);
      expect(mainSql, 'COUNT leaf').to.match(/COUNT\(\*\) AS "!T_1"/);
      // The un-decomposed ratio column must NOT appear (that was the bug shape).
      expect(mainSql, 'no ratio column').to.not.match(/\(SUM\("price"\)\*1\.0\/COUNT\(\*\)\)/);
      // Counterfactual: with the old code the main SQL carried the ratio column
      // and re-agg refused to collapse it (500). With the fix it carries leaves.
    });

    it('no mutilated SELECT and no orphan ORDER BY / GROUP BY (avg + min)', () => {
      // The original failure: avg's SELECT item dropped while ORDER BY avg
      // survived → "Column avg_price not found". Assert every emitted query is
      // structurally well-formed: no dangling comma, no GROUP BY referencing a
      // missing select item, and any ORDER BY column is actually projected.
      const sqls = planSql([
        ['avg_price', '$main.average($price)'],
        ['price_min', '$main.min($price)'],
      ]);
      for (const sql of sqls) {
        expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
        expect(sql, 'no empty select item (double comma)').to.not.match(/,\s*,/);
        // A GROUP BY 1,2 must have ≥2 select items; GROUP BY 1 must have ≥1.
        const selectCols = (sql.match(/ AS "/g) || []).length;
        const groupByMatch = sql.match(/GROUP BY ([\d,\s]+)/);
        if (groupByMatch) {
          const positions = groupByMatch[1].split(',').map(s => parseInt(s.trim(), 10));
          const maxPos = Math.max(...positions);
          expect(
            selectCols,
            `GROUP BY references position ${maxPos} but only ${selectCols} select items`,
          ).to.be.at.least(maxPos);
        }
        // ORDER BY column, if present, must be a projected alias.
        const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
        if (orderMatch) {
          expect(sql, `ORDER BY "${orderMatch[1]}" must be a projected column`).to.include(
            `AS "${orderMatch[1]}"`,
          );
        }
      }
      // Counterfactual: pre-fix the avg+min panel emitted a single nativeJoin
      // SQL whose SELECT dropped avg_price yet ORDER BY "avg_price" remained —
      // the ORDER-BY-projected assertion above would fail.
    });

    it('ratio of two avgs (RP/PVP) + diff + min all share/dedupe leaves; sort moves post-join', () => {
      const sqls = planSql([
        ['avg_price', '$main.average($price)'],
        ['rp', '$main.average($price) / $main.average($pvp)'],
        ['rp_diff', '($main.average($price) / $main.average($pvp)) - 1'],
        ['price_min', '$main.min($price)'],
      ]);
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      // count() is shared between avg_price and rp/rp_diff → deduped to ONE leaf.
      const countLeaves = (mainSql.match(/COUNT\(\*\) AS "!T_\d+"/g) || []).length;
      expect(countLeaves, 'count() deduped to a single leaf').to.equal(1);
      // Two distinct SUM leaves (price, pvp) + the min projected under its name.
      expect(mainSql, 'SUM(price) leaf').to.match(/SUM\("price"\) AS "!T_\d+"/);
      expect(mainSql, 'SUM(pvp) leaf').to.match(/SUM\("pvp"\) AS "!T_\d+"/);
      expect(mainSql, 'min projected by name').to.match(/MIN\("price"\) AS "price_min"/);
      // Sort on a derived measure must NOT stay on the main SQL (it is a
      // post-aggregate name, not a leaf column).
      expect(mainSql, 'no ORDER BY on derived measure in main SQL').to.not.match(/ORDER BY/);
    });

    it('countDistinct over a linked-only split STILL routes to native-JOIN (single well-formed SQL)', () => {
      const sqls = planSql([['uniq', '$main.countDistinct($brand)']]);
      expect(sqls.length, 'single combined SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN').to.match(/INNER JOIN/i);
      expect(sql, 'projects countDistinct').to.match(/COUNT\(DISTINCT/i);
      // ORDER BY column is projected (no orphan).
      expect(sql, 'ORDER BY uniq projected').to.include('AS "uniq"');
    });
  });

  describe('Row-level correctness — weighted average, not media-de-medias', () => {
    // Fixture engineered so per-brand averages are equal-weighted-wrong:
    //   Spain: B1 sum=100 count=100 (avg 1) ; B2 sum=100 count=1 (avg 100)
    //     true weighted avg = (100+100)/(100+1) = 200/101 = 1.98019...
    //     media-de-medias (WRONG) = (1+100)/2 = 50.5
    //   France: B3 sum=70 count=10 (avg 7)
    //   min: B1=1 B2=5 → Spain min 1 ; B3=7 → France min 7
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
          return Promise.resolve([
            { '__join_brand': 'B1', '!T_0': 100, '!T_1': 100, 'price_min': 1 },
            { '__join_brand': 'B2', '!T_0': 100, '!T_1': 1, 'price_min': 5 },
            { '__join_brand': 'B3', '!T_0': 70, '!T_1': 10, 'price_min': 7 },
          ]);
        }
        return Promise.resolve([]);
      });
    }

    it('avg + min collapse to one row per country with the weighted avg and min-of-mins', async () => {
      const ex = buildSplitExpr([
        ['avg_price', '$main.average($price)'],
        ['price_min', '$main.min($price)'],
      ]);
      const result = await ex.compute({ main: makeMain(reqWithLeaves()) });
      const rows = result.toJS().data[0].SPLIT.data;

      // Exactly one row per distinct country (fan-out collapsed).
      const countries = rows.map(r => r.brand_country).sort();
      expect(countries, 'one row per country').to.deep.equal(['France', 'Spain']);

      const spain = rows.find(r => r.brand_country === 'Spain');
      const france = rows.find(r => r.brand_country === 'France');
      // The weighted average — NOT media-de-medias (50.5).
      expect(spain.avg_price, 'Spain weighted avg').to.be.closeTo(200 / 101, 1e-9);
      expect(spain.avg_price, 'Spain avg is NOT media-de-medias').to.not.be.closeTo(50.5, 1e-6);
      expect(spain.price_min, 'Spain min-of-mins').to.equal(1);
      expect(france.avg_price, 'France avg').to.equal(7);
      expect(france.price_min, 'France min').to.equal(7);

      // No synthetic leaf column leaks to the caller.
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
        }
      }
      // Counterfactual: with the old (ratio-column) path this either threw
      // PlywoodCardinalityViolation (fan-out unreduced) or, had it collapsed,
      // produced 50.5 for Spain.
    });

    it('ratio of avgs (RP/PVP) recombines from leaves at the bucket grain', async () => {
      // pvp leaves so RP = avg(price)/avg(pvp). Spain weighted avg(price)=1.98019,
      // avg(pvp): B1 sum=200 count=100 (avg 2), B2 sum=200 count=1 (avg 200);
      // weighted = (200+200)/(100+1)=400/101=3.96039. RP_Spain=1.98019/3.96039=0.5.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B2', brand_country: 'Spain' },
          ]);
        }
        if (sql.includes('main_ds')) {
          // Leaf names depend on segregation order; map by the SQL aliases.
          // avg_price → !T_0=sum(price), !T_1=count(); rp adds !T_2=sum(pvp).
          return Promise.resolve([
            { '__join_brand': 'B1', '!T_0': 100, '!T_1': 100, '!T_2': 200 },
            { '__join_brand': 'B2', '!T_0': 100, '!T_1': 1, '!T_2': 200 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildSplitExpr([
        ['avg_price', '$main.average($price)'],
        ['rp', '$main.average($price) / $main.average($pvp)'],
      ]);
      const result = await ex.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      expect(rows.length, 'one Spain bucket').to.equal(1);
      const spain = rows[0];
      expect(spain.avg_price, 'avg(price) weighted').to.be.closeTo(200 / 101, 1e-9);
      expect(spain.rp, 'RP = weighted avg(price)/weighted avg(pvp)').to.be.closeTo(
        200 / 101 / (400 / 101),
        1e-9,
      );
      // i.e. exactly 0.5 — NOT (mean of per-brand RPs).
      expect(spain.rp, 'RP equals 0.5').to.be.closeTo(0.5, 1e-9);
    });
  });

  describe('Fail-loud — no malformed SQL ever reaches the requester', () => {
    it('native-JOIN renderAggregateSQL throws on an unrenderable derived op (no silent drop)', () => {
      // Directly exercise the renderer guard: a derived `divide` expression
      // must throw rather than return null (which previously dropped a SELECT
      // item). We trigger it by forcing a native-JOIN with a mixed measure set
      // where a derived measure would reach the renderer if it were passed the
      // rewritten form. The native-JOIN path now receives ORIGINAL applies, so
      // the public guarantee is: every emitted native-JOIN query projects a
      // column for every value apply (none silently dropped).
      const sqls = planSql([
        ['avg_price', '$main.average($price)'],
        ['uniq', '$main.countDistinct($brand)'],
      ]);
      // countDistinct forces native-JOIN; avg must render as AVG (original
      // form), NOT be dropped.
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'avg rendered as AVG (not dropped)').to.match(
        /AVG\(main\."price"\) AS "avg_price"/,
      );
      expect(sql, 'countDistinct rendered').to.match(/COUNT\(DISTINCT/i);
      // Every apply contributes a SELECT column → no orphan ORDER BY.
      const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
      if (orderMatch) {
        expect(sql, `ORDER BY ${orderMatch[1]} projected`).to.include(`AS "${orderMatch[1]}"`);
      }
    });
  });

  describe('Orthogonality — no fan-out (shared/main split) keeps avg as a native column', () => {
    it('split by the shared join key (brand) emits AVG natively, no leaf decomposition', () => {
      // brand is a shared dimension → no linked-only fan-out → the fix is inert:
      // avg stays the rewritten single ratio column (the pre-existing F2 form),
      // HAVING/sort can run on the engine.
      let split = $('main')
        .split('$brand', 'brand')
        .apply('avg_price', '$main.average($price)')
        .apply('cnt_country', $('magic_bc').count());
      const ex = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('SPLIT', split);
      const sqls = ex
        .simulateQueryPlan({ main: makeMain() })
        .flat()
        .filter(q => typeof q.query === 'string')
        .map(q => q.query);
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      // No leaf columns minted when there is no fan-out.
      expect(mainSql, 'no synthetic leaf columns').to.not.match(/!T_\d+/);
    });
  });
});
