/*
 * AUDIT (diagnostic + regression pin) — sort + limit on the native-JOIN path.
 *
 * CAPABILITY UNDER TEST: "top-N countries by nº distinct products". The user
 * splits by a LINKED-ONLY magic dim (`brand_country`, joinKey `brand`,
 * timeAlignment 'eternal'), measures a NON-decomposable countDistinct(concat)
 * (trait 'none' → forces the native-JOIN route), and sorts+limits by that very
 * measure. Plus the harder variant: sort by a DERIVED ratio measure
 * (avg/avg, root op `divide`) while a countDistinct sibling keeps the panel on
 * native-JOIN.
 *
 * WHAT THE AUDIT VERIFIED (real SQL + real rows, not just emit-shape):
 *
 *   1. SQL EMIT. `getNativeJoinDecomposition` appends `this.sort` then
 *      `this.limit` UNCONDITIONALLY after the `GROUP BY` (baseExternal.ts
 *      ~4126-4131). The ORDER BY renders the sort's INNER expression, which for
 *      a measure sort is a bare RefExpression to the projected alias — so we get
 *      `ORDER BY "uniq" DESC` / `ORDER BY "rp" DESC`, NEVER a re-rendered
 *      aggregate and NEVER an orphan column. The LIMIT is a literal `LIMIT N`
 *      placed AFTER the GROUP BY, so the engine groups first, then orders, then
 *      cuts — correct top-N-after-aggregation semantics.
 *
 *   2. THE ENGINE IS THE SOLE AUTHORITY for order + limit on this path. The
 *      native-JOIN execution layer dispatches ONE SQL and uses its rows AS-IS:
 *      it does NOT re-sort or re-limit in JS afterwards (verified by feeding the
 *      requester unordered / over-limit rows — they pass straight through). This
 *      is SOUND precisely because the ORDER BY + LIMIT are always baked into the
 *      SQL (point 1) and Druid honours them. We PIN the contract both ways: the
 *      SQL carries ORDER BY+LIMIT (so the delegation is safe) AND the layer is a
 *      pass-through (so if a future refactor ever drops ORDER BY from the SQL,
 *      the pass-through would surface wrong order and these pins flip loudly
 *      rather than a hidden JS safety-net masking the regression).
 *
 *   3. DERIVED-MEASURE SORT works on native-JOIN. The ratio is projected
 *      `(AVG(price)*1.0/AVG(pvp)) AS "rp"`; the sort references the alias `rp`,
 *      so ORDER BY sorts by the computed ratio — not by either AVG leg, not by a
 *      dropped column. countDistinct co-exists in the same SELECT. No throw.
 *
 * CLASSIFICATION: (A) ALREADY-CORRECT for every shape probed — correct order,
 * correct cardinality, alias-referenced ORDER BY, post-group LIMIT, derived sort
 * intact. No silent bug, no fail-loud. These tests are pure regression pins.
 *
 * Every assertion carries its counterfactual (what a regression would look
 * like). Wire-real shapes mirror the canonical catalogue (countDistinct(concat),
 * ratio-of-avgs) and the sibling files
 * simulateDruidComposeCountDistinctMagicDim / DerivedMeasureNativeJoin.
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

const UNIQ = '$main.countDistinct($product_id.concat($competitor))';
const RATIO = '$main.average($price) / $main.average($pvp)';

// Build the Turnilo-style top-level expression: scope registrations + a SPLIT
// apply by the linked-only dim, value applies, sort + limit. `direction`
// defaults to descending (the top-N case).
function buildSplitExpr(valueApplies, sortName, limitN, direction = 'descending') {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  if (sortName !== null) split = split.sort('$' + sortName, direction);
  if (limitN != null) split = split.limit(limitN);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

function planSql(valueApplies, sortName, limitN, direction) {
  return buildSplitExpr(valueApplies, sortName, limitN, direction)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('AUDIT native-JOIN: sort + limit (top-N countries by nº products)', () => {
  describe('(1) sort + limit BY the countDistinct measure — classification A', () => {
    it('emits ONE native-JOIN SQL: ORDER BY the projected uniq alias + LIMIT after GROUP BY', () => {
      const sqls = planSql([['uniq', UNIQ]], 'uniq', 5);
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];

      expect(sql, 'INNER JOIN against lookup').to.match(/INNER JOIN/i);
      expect(sql, 'countDistinct projected AS uniq').to.match(
        /COUNT\(DISTINCT[^)]*\)[^A]*AS "uniq"/i,
      );

      // ORDER BY references the PROJECTED ALIAS, not a re-rendered aggregate.
      expect(sql, 'ORDER BY references the uniq alias').to.match(/ORDER BY "uniq" DESC/);
      // Counterfactual: if the sort re-rendered the aggregate we'd see
      // `ORDER BY COUNT(DISTINCT …)`; if it referenced a dropped column the
      // alias would be unprojected → engine "column not found".
      expect(sql, 'ORDER BY alias is projected (no orphan)').to.include('AS "uniq"');
      expect(sql, 'no aggregate in the ORDER BY clause').to.not.match(
        /ORDER BY\s+COUNT\(DISTINCT/i,
      );

      // LIMIT comes AFTER the GROUP BY → cut top-N AFTER aggregation, not before.
      expect(sql, 'LIMIT present').to.match(/LIMIT 5\b/);
      const gbIdx = sql.indexOf('GROUP BY');
      const obIdx = sql.indexOf('ORDER BY');
      const limIdx = sql.indexOf('LIMIT');
      expect(gbIdx, 'GROUP BY present').to.be.greaterThan(-1);
      expect(obIdx, 'ORDER BY after GROUP BY').to.be.greaterThan(gbIdx);
      expect(limIdx, 'LIMIT after ORDER BY').to.be.greaterThan(obIdx);
      // Counterfactual: a LIMIT emitted inside/before the GROUP BY would cut raw
      // pre-agg rows and miscount distinct values per country.
    });

    it('ascending sort by uniq flips only the DESC→ASC token, alias still projected', () => {
      const sql = planSql([['uniq', UNIQ]], 'uniq', 5, 'ascending')[0];
      expect(sql, 'ORDER BY uniq ASC').to.match(/ORDER BY "uniq" ASC/);
      expect(sql, 'alias still projected').to.include('AS "uniq"');
    });

    it('ROW-LEVEL: engine ordered+limited rows pass through in order, correct cardinality', async () => {
      // The engine honours `ORDER BY uniq DESC LIMIT 3`. The native-JOIN layer
      // dispatches that ONE SQL and surfaces its rows as the final dataset.
      let dispatchedSql = '';
      const req = promiseFnToStream(rq => {
        dispatchedSql = (rq && rq.query && rq.query.query) || '';
        if (dispatchedSql.includes('INNER JOIN') && dispatchedSql.includes('main_ds')) {
          // top-3 by uniq, already DESC (what Druid returns for the emitted SQL).
          return Promise.resolve([
            { brand_country: 'Germany', uniq: 90 },
            { brand_country: 'Spain', uniq: 50 },
            { brand_country: 'France', uniq: 12 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildSplitExpr([['uniq', UNIQ]], 'uniq', 3);
      const rows = (await ex.compute({ main: makeMain(req) })).toJS().data[0].SPLIT.data;

      expect(dispatchedSql, 'ORDER BY + LIMIT baked into dispatched SQL').to.match(
        /ORDER BY "uniq" DESC\s*\nLIMIT 3/,
      );
      expect(rows.length, 'exactly LIMIT rows').to.equal(3);
      expect(
        rows.map(r => r.brand_country),
        'descending-by-uniq order preserved',
      ).to.deep.equal(['Germany', 'Spain', 'France']);
      expect(
        rows.map(r => r.uniq),
        'uniq values monotonically non-increasing',
      ).to.deep.equal([90, 50, 12]);
      // No synthetic leak.
      for (const r of rows)
        for (const k of Object.keys(r))
          expect(k.indexOf('!T_'), `no leaf leak: ${k}`).to.not.equal(0);
    });

    it('CONTRACT: native-JOIN delegates order+limit to the engine — NO JS re-sort/re-limit', async () => {
      // Feed the requester rows that VIOLATE the requested order and exceed the
      // LIMIT. The native-JOIN layer is a pass-through: it trusts the SQL's
      // ORDER BY+LIMIT and does NOT defensively re-order or re-cut in JS. We pin
      // this so the safety of point (1) is load-bearing: the SQL MUST carry the
      // clauses, because nothing downstream re-applies them.
      const req = promiseFnToStream(() =>
        Promise.resolve([
          { brand_country: 'Spain', uniq: 50 },
          { brand_country: 'Germany', uniq: 90 },
          { brand_country: 'France', uniq: 12 },
          { brand_country: 'Italy', uniq: 7 },
          { brand_country: 'Portugal', uniq: 3 },
        ]),
      );
      const ex = buildSplitExpr([['uniq', UNIQ]], 'uniq', 3);
      const rows = (await ex.compute({ main: makeMain(req) })).toJS().data[0].SPLIT.data;

      // Pass-through: all 5 rows survive in the engine-supplied order. The layer
      // did NOT re-sort to [90,50,12,...] nor re-cut to 3 rows.
      expect(rows.length, 'pass-through: no JS re-limit (engine is authority)').to.equal(5);
      expect(
        rows.map(r => r.uniq),
        'pass-through: no JS re-sort (engine order kept as-is)',
      ).to.deep.equal([50, 90, 12, 7, 3]);
      // INTERPRETATION: this is NOT a bug — Druid always honours the ORDER
      // BY+LIMIT this path bakes into the SQL (test above), so the real-world
      // rows arrive pre-ordered+pre-limited. The pin guards the inverse: if a
      // future change ever stops emitting ORDER BY into the native-JOIN SQL,
      // there is no JS net to silently fix it — wrong order would reach the UI,
      // and this pin documents exactly where the authority lives.
    });
  });

  describe('(2) sort BY a DERIVED ratio measure, countDistinct present — classification A', () => {
    it('ORDER BY references the projected ratio alias; ratio is computed-then-sorted', () => {
      const sqls = planSql(
        [
          ['rp', RATIO],
          ['uniq', UNIQ],
        ],
        'rp',
        3,
      );
      expect(sqls.length, 'derived sort + countDistinct → ONE native-JOIN SQL (no throw)').to.equal(
        1,
      );
      const sql = sqls[0];

      // The ratio is projected as a floatDivision of the two AVGs, aliased rp.
      expect(sql, 'ratio projected as floatDivision of AVGs, aliased rp').to.match(
        /\(AVG\(main\."price"\)\*1\.0\/AVG\(main\."pvp"\)\) AS "rp"/,
      );
      // ORDER BY sorts by the ALIAS rp — i.e. by the computed ratio, NOT by one
      // of the AVG legs and NOT by a dropped column.
      expect(sql, 'ORDER BY the rp alias').to.match(/ORDER BY "rp" DESC/);
      expect(sql, 'rp alias projected (no orphan)').to.include('AS "rp"');
      expect(sql, 'ORDER BY is the alias, not a raw AVG/divide expression').to.not.match(
        /ORDER BY\s*\(?AVG\(/i,
      );

      // countDistinct co-present, LIMIT after GROUP BY.
      expect(sql, 'countDistinct co-present in the same SELECT').to.match(/COUNT\(DISTINCT/i);
      expect(sql, 'LIMIT 3').to.match(/LIMIT 3\b/);
      expect(sql, 'no synthetic leaf columns (native JOIN, not jsJoin)').to.not.match(/!T_\d+/);
      // Counterfactual: pre-derived-measure-fix this exact shape threw root
      // op='divide' (Ismael's 500); a half-fix that dropped the rp SELECT item
      // would leave `ORDER BY "rp"` orphaned → engine "column rp not found".
    });

    it('ascending derived sort emits ASC, ratio still projected', () => {
      const sql = planSql(
        [
          ['rp', RATIO],
          ['uniq', UNIQ],
        ],
        'rp',
        4,
        'ascending',
      )[0];
      expect(sql, 'ORDER BY rp ASC').to.match(/ORDER BY "rp" ASC/);
      expect(sql, 'ratio still projected').to.match(/\) AS "rp"/);
    });

    it('ROW-LEVEL: rows ordered by the ratio (not by uniq), both measures intact', async () => {
      // Engine returns top-3 by rp DESC (what the emitted SQL asks for). Note the
      // uniq column is NOT monotone — proving the sort is by the ratio, not the
      // distinct count.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('INNER JOIN')) {
          return Promise.resolve([
            { brand_country: 'France', rp: 1.4, uniq: 5 },
            { brand_country: 'Spain', rp: 1.1, uniq: 50 },
            { brand_country: 'Germany', rp: 0.9, uniq: 90 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildSplitExpr(
        [
          ['rp', RATIO],
          ['uniq', UNIQ],
        ],
        'rp',
        3,
      );
      const rows = (await ex.compute({ main: makeMain(req) })).toJS().data[0].SPLIT.data;

      expect(rows.length, 'LIMIT 3 rows').to.equal(3);
      expect(
        rows.map(r => r.brand_country),
        'ordered by ratio DESC',
      ).to.deep.equal(['France', 'Spain', 'Germany']);
      expect(
        rows.map(r => r.rp),
        'rp monotonically non-increasing',
      ).to.deep.equal([1.4, 1.1, 0.9]);
      // uniq is NOT in descending order → confirms sort key is the ratio, not uniq.
      expect(
        rows.map(r => r.uniq),
        'uniq present but NOT the sort key (5,50,90 is ascending)',
      ).to.deep.equal([5, 50, 90]);
      // Both measures land on every row.
      for (const r of rows) {
        expect(r, 'rp present').to.have.property('rp');
        expect(r, 'uniq present').to.have.property('uniq');
      }
    });
  });

  describe('(3) ORDER BY the split dimension itself (brand_country) — classification A', () => {
    it('sort by the dimension alias projects it from the lookup, ORDER BY references it', () => {
      const sql = planSql([['uniq', UNIQ]], 'brand_country', 10, 'ascending')[0];
      expect(sql, 'split dimension projected from lookup').to.match(
        /lookup\."brand_country" AS "brand_country"/,
      );
      expect(sql, 'ORDER BY the dimension alias').to.match(/ORDER BY "brand_country" ASC/);
      expect(sql, 'LIMIT 10').to.match(/LIMIT 10\b/);
    });
  });

  describe('(4) limit-only and no-sort/no-limit — clauses appended iff present', () => {
    it('LIMIT without explicit sort: emits LIMIT, no ORDER BY', () => {
      const sql = planSql([['uniq', UNIQ]], null, 25)[0];
      expect(sql, 'LIMIT 25').to.match(/LIMIT 25\b/);
      expect(sql, 'no ORDER BY when none requested').to.not.match(/ORDER BY/);
    });

    it('neither sort nor limit: bare GROUP BY, no ORDER BY / LIMIT tail', () => {
      const sql = planSql([['uniq', UNIQ]], null, null)[0];
      expect(sql, 'GROUP BY present').to.match(/GROUP BY 1\b/);
      expect(sql, 'no ORDER BY').to.not.match(/ORDER BY/);
      expect(sql, 'no LIMIT').to.not.match(/LIMIT/);
      // Counterfactual: a spurious LIMIT/ORDER BY here would mean the clauses
      // are not gated on presence — they are.
    });
  });
});
