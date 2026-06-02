/*
 * COMPOSITION test — countDistinct(concat) family × magic-dimension split, and
 * its measure×measure composition with the avg/min/sum leaf-decomposition fix.
 *
 * Why this family: `countDistinct(concat(...))` (nº URLs, nº sellers) is present
 * in ALL 349 cubes of the real catalogue (see /tmp/cube-formula-shapes.md). It
 * is the canonical NON-decomposable measure: `CountDistinctExpression.decomposable
 * = 'none'` — a count of distinct values cannot be recombined from per-partition
 * partials (∑ of per-brand distinct counts ≠ per-country distinct count). So when
 * the user splits by a LINKED-ONLY dimension (`brand_country`, joinKey `brand`,
 * timeAlignment 'eternal'), the JS-join leaf path is unsafe and the INV-2 gate
 * MUST divert the whole panel to the native-JOIN path: a single Druid SQL with an
 * in-engine INNER JOIN that computes COUNT(DISTINCT …) at the bucket grain — no
 * fan-out, no leaf decomposition.
 *
 * This file pins three things, with the canonical cube's real measure shapes:
 *
 *   (1) ROUTE. countDistinct over a linked-only split routes to a single
 *       well-formed native-JOIN SQL (INNER JOIN, COUNT(DISTINCT …), GROUP BY 1,
 *       ORDER BY on a projected column — no orphan). The concat inner renders
 *       through the Druid `||` operator.
 *
 *   (2) COMPOSITION measure×measure. A panel that mixes a DECOMPOSABLE measure
 *       (avg → leaves on jsJoin) with a NON-decomposable one (countDistinct →
 *       nativeJoin) in the SAME split. Which route wins? The non-decomposable
 *       measure dominates: the gate sees a 'none'-trait leaf and routes the
 *       ENTIRE panel to native-JOIN, passing the ORIGINAL (un-rewritten) applies.
 *       avg/min/sum/count then render as single aggregates (AVG/MIN/SUM/…)
 *       directly in that one SQL — correct, because native-JOIN groups at the
 *       bucket grain so there is NO fan-out to produce media-de-medias. They do
 *       NOT split into a second query; countDistinct does not get dropped; both
 *       measures land in one SELECT. This is the orthogonality the fixes were
 *       supposed to give: countDistinct's route subsumes the avg-leaf route
 *       harmlessly because native-JOIN is already fan-out-free.
 *
 *   (3) FAIL-LOUD LIMIT (known, documented — NOT a silent bug). A DERIVED measure
 *       whose ROOT op is not a single aggregate — e.g. RP/PVP = `avg(price) /
 *       avg(pvp)` (root op `divide`), or `(avg/avg) - 1` (root op `subtract`),
 *       both extremely common in the catalogue — composes FINE on the jsJoin leaf
 *       path when ALONE (segregates to SUM/COUNT leaves, recombines post-join).
 *       But when a sibling countDistinct forces the panel onto native-JOIN, that
 *       derived measure reaches `renderAggregateSQL`, which only knows single
 *       aggregates, and THROWS `PlywoodUnsupportedNativeJoinShape` naming the
 *       offending root op. This is the v1 native-JOIN renderer's documented limit
 *       surfaced FAIL-LOUD (P2): no malformed SQL, no dropped SELECT item, no
 *       500-from-the-engine, no wrong numbers — a clear, actionable error before
 *       any query is dispatched. The task brief calls this out explicitly: a
 *       PlywoodUnsupportedNativeJoinShape for countDistinct + linked-only is a
 *       KNOWN LIMIT (loud), not a composition bug. We PIN the throw (so a future
 *       fix that makes derived-measure native-JOIN work flips this test loudly
 *       rather than regressing silently) AND prove the same measure works alone.
 *
 * Every assertion has a counterfactual spelled out. Wire-real measure shapes
 * (countDistinct(concat), ratio-of-avgs) mirror the canonical GrupoIfa cube 507.
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

// Canonical-cube mirror: main histories + an eternal magic-dim lookup carrying
// brand_country (joinKey brand). `product_id` + `competitor` feed the
// countDistinct(concat) (nº URLs / nº sellers) measure; price + pvp feed the
// avg / ratio-of-avgs measures.
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

// Turnilo-style top-level expression: scope registrations + a SPLIT apply by the
// linked-only dim, value applies, sort + limit.
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
  return buildSplitExpr(valueApplies, sortName)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

// The countDistinct(concat) measure exactly as the catalogue writes it.
const UNIQ = '$main.countDistinct($product_id.concat($competitor))';

describe('Compose: countDistinct(concat) × magic-dimension split (native-JOIN route)', () => {
  describe('(1) ROUTE — countDistinct over a linked-only split → single native-JOIN SQL', () => {
    it('countDistinct(concat) emits ONE well-formed native-JOIN SQL (no orphan ORDER BY)', () => {
      const sqls = planSql([['uniq', UNIQ]]);
      expect(sqls.length, 'single combined SQL (native-JOIN, not 2 sub-queries)').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN against the lookup').to.match(/INNER JOIN/i);
      // countDistinct rendered (NOT silently dropped → would have been the bug).
      expect(sql, 'COUNT(DISTINCT …)').to.match(/COUNT\(DISTINCT/i);
      // concat rendered through the Druid `||` operator inside the distinct count.
      expect(sql, 'concat via || inside the distinct').to.match(
        /COUNT\(DISTINCT\s*\(.*\|\|.*\)\)/i,
      );
      // The split column comes from the lookup side.
      expect(sql, 'split column projected from lookup').to.match(
        /lookup\."brand_country" AS "brand_country"/,
      );
      // GROUP BY 1 (single bucket grain) — never mutilated.
      expect(sql, 'GROUP BY 1').to.match(/GROUP BY 1\b/);
      // ORDER BY references a projected alias — no orphan.
      const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
      expect(orderMatch, 'ORDER BY present').to.exist;
      expect(sql, `ORDER BY "${orderMatch[1]}" must be projected`).to.include(
        `AS "${orderMatch[1]}"`,
      );
      // No malformed SELECT.
      expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
      expect(sql, 'no double comma').to.not.match(/,\s*,/);
      // Counterfactual: countDistinct has trait 'none'; if the gate had treated
      // it as decomposable it would have taken the jsJoin leaf path and produced
      // a SUM/COUNT-of-distinct nonsense, or returned null → engine "column not
      // found". The single COUNT(DISTINCT …) SQL is the correct route.
    });

    it('countDistinct over a BARE ref also routes native-JOIN (concat is not load-bearing for the route)', () => {
      const sqls = planSql([['uniq', '$main.countDistinct($product_id)']]);
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      expect(sqls[0], 'COUNT(DISTINCT main.product_id)').to.match(
        /COUNT\(DISTINCT main\."product_id"\)/i,
      );
      expect(sqls[0], 'INNER JOIN').to.match(/INNER JOIN/i);
    });
  });

  describe('(2) COMPOSITION measure×measure — decomposable + non-decomposable in one panel', () => {
    it('avg (decomposable) + countDistinct (none) → ONE native-JOIN SQL; avg renders as AVG, not leaves', () => {
      // The crux of the composition: avg ALONE over a linked-only split would
      // segregate into SUM/COUNT leaves (jsJoin). countDistinct ALONE routes to
      // native-JOIN. Put them in the SAME panel: the 'none' leaf wins → the
      // whole panel goes native-JOIN, and avg is passed in its ORIGINAL form so
      // it renders as AVG (one SQL, no fan-out, no media-de-medias).
      const sqls = planSql(
        [
          ['avg_price', '$main.average($price)'],
          ['uniq', UNIQ],
        ],
        'avg_price',
      );
      expect(sqls.length, 'measures do NOT split into two routes — one SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'avg rendered as AVG (original form, NOT dropped)').to.match(
        /AVG\(main\."price"\) AS "avg_price"/,
      );
      expect(sql, 'countDistinct rendered in the SAME SELECT').to.match(/COUNT\(DISTINCT/i);
      // No leaf decomposition columns minted on the native-JOIN path.
      expect(sql, 'no synthetic !T_ leaf columns').to.not.match(/!T_\d+/);
      expect(sql, 'no synthetic __join_ key (native JOIN, not jsJoin)').to.not.match(/__join_/);
      // Both sort orders keep the ORDER BY column projected.
      expect(sql, 'ORDER BY avg_price projected').to.include('AS "avg_price"');
      // Counterfactual: pre-fix, native-JOIN received the avg-REWRITTEN
      // divide(sum,count) form; renderAggregateSQL had no `divide` case and
      // returned null → the avg SELECT item was dropped while ORDER BY avg_price
      // survived → "Column avg_price not found" (engine 500). The fix passes the
      // original AVG + throws on unrenderable ops, so this asserts AVG is present.
    });

    it('sorting by the countDistinct measure keeps the SAME single-SQL route, ORDER BY uniq projected', () => {
      const sqls = planSql(
        [
          ['avg_price', '$main.average($price)'],
          ['uniq', UNIQ],
        ],
        'uniq',
      );
      expect(sqls.length, 'still one SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'ORDER BY uniq').to.match(/ORDER BY "uniq"/);
      expect(sql, 'ORDER BY uniq projected (no orphan)').to.include('AS "uniq"');
      expect(sql, 'avg still AVG').to.match(/AVG\(main\."price"\)/);
    });

    it('min/sum/count each compose with countDistinct as single aggregates in one SQL', () => {
      for (const [name, formula, rendered] of [
        ['price_min', '$main.min($price)', /MIN\(main\."price"\) AS "price_min"/],
        ['s', '$main.sum($price)', /SUM\(main\."price"\) AS "s"/],
        ['c', '$main.count()', /COUNT\(\*\) AS "c"/],
      ]) {
        const sqls = planSql(
          [
            [name, formula],
            ['uniq', UNIQ],
          ],
          'uniq',
        );
        expect(sqls.length, `${name} + cd → one SQL`).to.equal(1);
        expect(sqls[0], `${name} rendered`).to.match(rendered);
        expect(sqls[0], `${name}: countDistinct co-present`).to.match(/COUNT\(DISTINCT/i);
        expect(sqls[0], `${name}: no leaf columns`).to.not.match(/!T_\d+/);
      }
      // Counterfactual: if the gate split the panel by trait (decomposable→leaves,
      // non→native) we'd get TWO query routes that can't be re-joined coherently.
      // The single SQL proves the non-decomposable measure subsumes the rest.
    });

    it('two countDistincts compose into one SQL (both COUNT(DISTINCT …))', () => {
      const sqls = planSql(
        [
          ['u1', '$main.countDistinct($product_id)'],
          ['u2', UNIQ],
        ],
        'u1',
      );
      expect(sqls.length, 'one SQL').to.equal(1);
      const cds = (sqls[0].match(/COUNT\(DISTINCT/gi) || []).length;
      expect(cds, 'two distinct counts projected').to.equal(2);
    });
  });

  describe('(3) FAIL-LOUD LIMIT — derived measure + countDistinct on native-JOIN (KNOWN, loud)', () => {
    it('RP/PVP (avg/avg, root op divide) + countDistinct THROWS PlywoodUnsupportedNativeJoinShape', () => {
      // RP/PVP is the catalogue's ratio-of-avgs. Alone it segregates into leaves
      // (see the orthogonality test below — it works). Forced onto native-JOIN by
      // the sibling countDistinct, its root op `divide` is not a single aggregate
      // → renderAggregateSQL throws. This is FAIL-LOUD, not malformed SQL: the
      // error names the op and fires before any query reaches the engine.
      expect(
        () =>
          planSql(
            [
              ['rp', '$main.average($price) / $main.average($pvp)'],
              ['uniq', UNIQ],
            ],
            'uniq',
          ),
        'derived ratio + countDistinct must throw loudly',
      ).to.throw(/Cross-source native-JOIN cannot emit SQL/);
      // Pin the specific diagnostic (the op is named so the limit is actionable).
      let msg = '';
      try {
        planSql(
          [
            ['rp', '$main.average($price) / $main.average($pvp)'],
            ['uniq', UNIQ],
          ],
          'uniq',
        );
      } catch (e) {
        msg = e.message;
        expect(e.name, 'error class').to.equal('PlywoodUnsupportedNativeJoinShape');
      }
      expect(msg, "names root op 'divide'").to.match(/root op='divide'/);
      expect(msg, 'mentions single-aggregate requirement').to.match(/single native-JOIN aggregate/);
      // Counterfactual / regression guard: if a future change makes derived
      // measures renderable on native-JOIN (decompose-then-recombine inside the
      // single SQL, or split-route), this test FLIPS loudly — it must not start
      // silently emitting a SELECT that drops the ratio while ORDER BY keeps it.
    });

    it('(avg/avg) - 1 (root op subtract) + countDistinct ALSO throws loudly, naming subtract', () => {
      let msg = '';
      try {
        planSql(
          [
            ['rp_diff', '($main.average($price) / $main.average($pvp)) - 1'],
            ['uniq', UNIQ],
          ],
          'uniq',
        );
        expect.fail('expected a throw');
      } catch (e) {
        msg = e.message;
      }
      expect(msg, 'PlywoodUnsupportedNativeJoinShape').to.match(/native-JOIN cannot emit SQL/);
      expect(msg, "names root op 'subtract'").to.match(/root op='subtract'/);
    });

    it('the SAME ratio measure ALONE (no countDistinct) composes fine via jsJoin leaves', () => {
      // Orthogonality proof: the throw above is caused by the COMPOSITION
      // (derived measure forced onto native-JOIN by countDistinct), NOT by the
      // ratio being intrinsically broken. Alone, the linked-only split segregates
      // avg/avg into SUM(price)/COUNT(*)/SUM(pvp) leaves across two sub-queries.
      const sqls = planSql([['rp', '$main.average($price) / $main.average($pvp)']]);
      expect(sqls.length, 'jsJoin emits main + lookup sub-queries').to.equal(2);
      const mainSql = sqls.find(s => s.includes('"main_ds"') && !s.includes('lookup_bc_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      expect(mainSql, 'SUM(price) leaf').to.match(/SUM\("price"\) AS "!T_\d+"/);
      expect(mainSql, 'COUNT(*) leaf').to.match(/COUNT\(\*\) AS "!T_\d+"/);
      expect(mainSql, 'SUM(pvp) leaf').to.match(/SUM\("pvp"\) AS "!T_\d+"/);
      // No INNER JOIN here — this is the JS-join path, not native-JOIN.
      expect(mainSql, 'jsJoin main is not a native JOIN').to.not.match(/INNER JOIN/i);
    });
  });

  describe('Row-level correctness — native-JOIN compute returns one row per bucket, both measures intact', () => {
    it('avg + countDistinct compute: 2 country buckets, AVG and COUNT(DISTINCT) co-exist', async () => {
      // Native-JOIN dispatches ONE combined SQL whose requester returns the final
      // per-bucket rows (the engine already did the JOIN + GROUP BY). Assert the
      // caller sees one row per country with both measures and no synthetic leaks.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('INNER JOIN') && sql.includes('main_ds')) {
          return Promise.resolve([
            { brand_country: 'Spain', avg_price: 200 / 101, uniq: 42 },
            { brand_country: 'France', avg_price: 7, uniq: 5 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildSplitExpr(
        [
          ['avg_price', '$main.average($price)'],
          ['uniq', UNIQ],
        ],
        'uniq',
      );
      const result = await ex.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;

      const countries = rows.map(r => r.brand_country).sort();
      expect(countries, 'one row per country').to.deep.equal(['France', 'Spain']);
      const spain = rows.find(r => r.brand_country === 'Spain');
      const france = rows.find(r => r.brand_country === 'France');
      expect(spain.uniq, 'Spain distinct count').to.equal(42);
      expect(spain.avg_price, 'Spain avg passes through from the single SQL').to.be.closeTo(
        200 / 101,
        1e-9,
      );
      expect(france.uniq, 'France distinct count').to.equal(5);
      expect(france.avg_price, 'France avg').to.equal(7);
      // No synthetic columns leak.
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf leak: ${k}`).to.not.equal(0);
          expect(k, 'no __join_ leak').to.not.equal('__join_brand');
        }
      }
    });
  });

  describe('Orthogonality — no fan-out: countDistinct on a SHARED/main split is a plain query', () => {
    it('countDistinct split by the shared join key (brand) does NOT native-JOIN, no leaf columns', () => {
      // brand is a shared dimension → no linked-only fan-out → the cross-source
      // gate keeps the historic JS-join shape (main pre-aggregates at the grid
      // grain already). countDistinct renders natively on main; the fix is inert.
      let split = $('main')
        .split('$brand', 'brand')
        .apply('uniq', UNIQ)
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
      // No INNER JOIN, no synthetic leaf columns — fix inert without fan-out.
      expect(mainSql, 'no native INNER JOIN for a shared split').to.not.match(/INNER JOIN/i);
      expect(mainSql, 'no synthetic !T_ leaf columns').to.not.match(/!T_\d+/);
      expect(mainSql, 'countDistinct renders natively on main').to.match(/COUNT\(DISTINCT/i);
    });

    it('countDistinct TOTALS (no split) is a single main-only SQL — never touches the lookup', () => {
      // A total (GROUP BY ()) has no linked-only split → no cross-source path.
      const ex = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('uniq', UNIQ);
      const sqls = ex
        .simulateQueryPlan({ main: makeMain() })
        .flat()
        .filter(q => typeof q.query === 'string')
        .map(q => q.query);
      expect(sqls.length, 'single totals SQL').to.equal(1);
      expect(sqls[0], 'GROUP BY ()').to.match(/GROUP BY \(\)/);
      expect(sqls[0], 'no JOIN against the lookup').to.not.match(/lookup_bc_rev1|INNER JOIN/i);
      expect(sqls[0], 'countDistinct(concat) rendered').to.match(
        /COUNT\(DISTINCT\s*\(.*\|\|.*\)\)/i,
      );
    });
  });
});
