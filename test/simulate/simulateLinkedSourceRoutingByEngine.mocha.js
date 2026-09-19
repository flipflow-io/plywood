/*
 * Routing by engine (0.51.10) — the materialisation policy of 19 Sep 2026.
 *
 * A mapping or magic dimension is a linked source of the cube. Where it lives
 * decides the plan:
 *
 *   SAME ENGINE (a Druid datasource next to the main datasource)
 *     - split by the linked column, any measure    → ONE native JOIN SQL
 *     - filter on the linked column, main split    → main WHERE key IN (SELECT …)
 *     - filter on the linked column, totals        → same IN sub-query, GROUP BY ()
 *     Nothing is pre-aggregated or joined in memory; no key list leaves plywood.
 *
 *   CROSS ENGINE (a Postgres staging view under a Druid main)
 *     - the JS-join stays the plan, dispatched SEQUENTIALLY:
 *         display-only linked column → main first, lookup fetched for the
 *                                      result's keys (enrichment)
 *         inner join + linked filter → lookup first, main restricted to its
 *                                      keys (the split-level semijoin)
 *     - a 'none'-trait measure (countDistinct) has no correct plan across
 *       engines and fails loud.
 *
 * Every SQL shape is pinned through simulateQueryPlan; the sequential dispatch
 * is also executed against mock engines to prove the IN predicate carries
 * exactly the other side's keys and that the key cap falls back to the full
 * scan (still correct, the join matches in memory).
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply, Expression } = plywood;

const MAP = 'mapping_img';
const sqlOf = rq =>
  typeof rq.query === 'string' ? rq.query : (rq && rq.query && rq.query.query) || '';

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

// The rc cube shape: druidsql main, a mapping linked on imageUrl. `where`
// picks the mapping's home: 'druid' (materialised, same engine) or 'postgres'
// (staging view, cross-engine).
function makeMain({ where, joinMode = 'left', requester, pgRequester } = {}) {
  const value = {
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: '__time',
    allowEternity: true,
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'productName', type: 'STRING' },
      { name: 'productId', type: 'STRING' },
      { name: 'imageUrl', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      [MAP]: {
        source: where === 'postgres' ? 'mapping_view_abc' : 'mapping_abc',
        joinKeys: ['imageUrl'],
        autoInjectJoinKeys: ['imageUrl'],
        sharedDimensions: ['imageUrl'],
        joinMode,
        timeAlignment: 'eternal',
        ...(where === 'postgres' ? { engine: 'postgres', version: '16.0.0' } : {}),
        attributes: [
          ...(where === 'postgres' ? [] : [{ name: '__time', type: 'TIME' }]),
          { name: 'imageUrl', type: 'STRING' },
          { name: 'familia', type: 'STRING' },
          { name: 'generated_image', type: 'STRING' },
        ],
      },
    },
  };
  const main = External.fromJS(value, requester);
  if (where === 'postgres') {
    main.linkedSources[MAP].requester =
      pgRequester ||
      (() => {
        throw new Error('postgres requester must not run in simulate');
      });
  }
  return main;
}

const TIME = $('__time').overlap(
  new Date('2026-09-11T00:00:00Z'),
  new Date('2026-09-18T00:00:00Z'),
);

// The front's request shape: the same filter on main and on the linked apply,
// a totals apply and (optionally) a SPLIT with measures.
function query(filter, splits, measures) {
  let ex = ply()
    .apply('main', $('main').filter(filter))
    .apply(MAP, $(MAP).filter(filter))
    .apply('count', $('main').count());
  if (splits) {
    let split = $('main').split(splits);
    for (const [name, formula] of measures || [['count', '$main.count()']]) {
      split = split.apply(name, formula);
    }
    split = split.sort('$' + (measures ? measures[0][0] : 'count'), 'descending').limit(50);
    ex = ex.apply('SPLIT', split);
  }
  return ex;
}

function planSqls(ex, main) {
  return ex
    .simulateQueryPlan({ main })
    .flat()
    .map(q => (typeof q === 'string' ? q : q && q.query))
    .filter(q => typeof q === 'string');
}
const totalsSql = sqls => sqls.find(s => s.includes('"histories"') && /GROUP BY \(\)/.test(s));
const splitSql = sqls => sqls.find(s => s.includes('"histories"') && /GROUP BY 1(?!\))/.test(s));
const mappingOnly = sqls => sqls.filter(s => s.includes('mapping_') && !s.includes('"histories"'));

describe('Linked source routing by engine (0.51.10)', () => {
  describe('same engine — the mapping is a Druid datasource', () => {
    it('split by the mapping column with count → ONE native LEFT JOIN SQL, no lookup scan', () => {
      const sqls = planSqls(query(TIME, { familia: $('familia') }), makeMain({ where: 'druid' }));
      const join = sqls.find(s => /LEFT JOIN "mapping_abc" AS lookup/.test(s));
      expect(join, 'native JOIN emitted').to.exist;
      expect(join).to.match(/SELECT lookup\."familia" AS "familia", COUNT\(\*\) AS "count"/);
      expect(join).to.match(/ON main\."imageUrl" = lookup\."imageUrl"/);
      expect(join, 'main filter on the main alias').to.match(/main\."__time"|"main"\."__time"/);
      expect(join, 'inline sort + limit stay in the engine').to.match(
        /ORDER BY "count" DESC\nLIMIT 50/,
      );
      expect(mappingOnly(sqls), 'no separate lookup query').to.deep.equal([]);
      // The totals never touch the mapping (no linked filter).
      expect(totalsSql(sqls)).to.not.match(/mapping_abc/);
    });

    it('grid product × mapping with sum → ONE native JOIN grouped by both keys', () => {
      const sqls = planSqls(
        query(TIME, { productName: $('productName'), familia: $('familia') }, [
          ['total', '$main.sum($price)'],
        ]),
        makeMain({ where: 'druid' }),
      );
      const join = sqls.find(s => /LEFT JOIN "mapping_abc"/.test(s));
      expect(join, 'native JOIN emitted').to.exist;
      expect(join).to.match(
        /"main"\."productName" AS "productName", lookup\."familia" AS "familia"/,
      );
      expect(join).to.match(/SUM\(main\."price"\) AS "total"/);
      expect(join).to.match(/GROUP BY 1, 2/);
    });

    it('countDistinct split by the mapping → the same native JOIN (the route that was already mandatory)', () => {
      const sqls = planSqls(
        query(TIME, { familia: $('familia') }, [['products', '$main.countDistinct($productId)']]),
        makeMain({ where: 'druid' }),
      );
      const join = sqls.find(s => /LEFT JOIN "mapping_abc"/.test(s));
      expect(join).to.exist;
      expect(join).to.match(/COUNT\(DISTINCT main\."productId"\) AS "products"/);
    });

    it('filter on the mapping column + main split → main WHERE imageUrl IN (SELECT …), one statement per query', () => {
      const filter = TIME.and($('familia').is(Expression.fromJS({ op: 'literal', value: 'F3' })));
      const sqls = planSqls(
        query(filter, { productName: $('productName') }),
        makeMain({ where: 'druid', joinMode: 'inner' }),
      );
      expect(mappingOnly(sqls), 'no lookup round-trip').to.deep.equal([]);
      const sub =
        /"imageUrl" IN \(SELECT "imageUrl" AS "imageUrl" FROM "mapping_abc" AS t WHERE \("familia"='F3'\) GROUP BY 1\)/;
      expect(totalsSql(sqls), 'totals restricted by the sub-query').to.match(sub);
      expect(splitSql(sqls), 'split restricted by the sub-query').to.match(sub);
      expect(splitSql(sqls)).to.match(/^SELECT\n"productName" AS "productName"/);
    });

    it('a left-joined mapping filtered to one image (rejects orphans) rides the same sub-query', () => {
      const filter = TIME.and(
        $('generated_image').is(Expression.fromJS({ op: 'literal', value: 'https://x/1.jpg' })),
      );
      const sqls = planSqls(
        query(filter, { productName: $('productName') }),
        makeMain({ where: 'druid', joinMode: 'left' }),
      );
      expect(totalsSql(sqls)).to.match(
        /"imageUrl" IN \(SELECT "imageUrl"[\s\S]*'https:\/\/x\/1\.jpg'/,
      );
      expect(splitSql(sqls)).to.match(/"imageUrl" IN \(SELECT/);
    });

    it('filter AND split on the mapping column → native INNER JOIN with the clause on the lookup alias', () => {
      const filter = TIME.and($('familia').is(Expression.fromJS({ op: 'literal', value: 'F3' })));
      const sqls = planSqls(
        query(filter, { familia: $('familia') }),
        makeMain({ where: 'druid', joinMode: 'left' }),
      );
      const join = sqls.find(s => /JOIN "mapping_abc" AS lookup/.test(s));
      expect(join, 'left + orphan-rejecting filter joins inner').to.match(/INNER JOIN/);
      expect(join).to.match(/WHERE[\s\S]*("lookup"|lookup)\."familia"='F3'/);
    });
  });

  describe('cross engine — the mapping is a Postgres staging view', () => {
    it('display-only mapping column → main first, the view is fetched for the result keys (sample IN)', () => {
      const sqls = planSqls(
        query(TIME, { productName: $('productName'), generated_image: $('generated_image') }),
        makeMain({ where: 'postgres' }),
      );
      const view = mappingOnly(sqls);
      expect(view, 'one Postgres query').to.have.length(1);
      expect(view[0]).to.match(/FROM "mapping_view_abc"/);
      expect(view[0], 'narrowed to the result keys').to.match(
        /WHERE \("imageUrl" (IN \(|IS NOT DISTINCT FROM )/,
      );
      // Order in the plan: main before the view.
      const iMain = sqls.findIndex(s => s.includes('"histories"') && /GROUP BY 1/.test(s));
      const iView = sqls.indexOf(view[0]);
      expect(iMain, 'main dispatched first').to.be.lessThan(iView);
    });

    it('inner join + filter on the mapping column → the view first, then main split restricted to its keys', () => {
      const filter = TIME.and($('familia').is(Expression.fromJS({ op: 'literal', value: 'F3' })));
      const sqls = planSqls(
        query(filter, { productName: $('productName') }),
        makeMain({ where: 'postgres', joinMode: 'inner' }),
      );
      // The totals take the semijoin-to-root (lookup DISTINCT keys, IN-list on
      // main); the split takes the same idea at split level.
      expect(totalsSql(sqls)).to.match(/"imageUrl"\s*(IN \(|=)/);
      expect(totalsSql(sqls), 'cross-engine: no sub-query, a literal list').to.not.match(
        /IN \(SELECT/,
      );
      const split = splitSql(sqls);
      expect(split, 'main split exists').to.exist;
      expect(split, 'main split restricted by the lookup keys').to.match(
        /WHERE[\s\S]*"imageUrl"\s*(IN \(|=)/,
      );
      expect(split).to.not.match(/familia/);
      const views = mappingOnly(sqls);
      expect(views.length, 'lookup queried for totals and for the split').to.be.at.least(1);
      for (const v of views) expect(v).to.match(/"familia"/);
    });

    it('countDistinct split by the mapping column has no correct cross-engine plan → fails loud', () => {
      expect(() =>
        planSqls(
          query(TIME, { familia: $('familia') }, [['products', '$main.countDistinct($productId)']]),
          makeMain({ where: 'postgres' }),
        ),
      ).to.throw(plywood.PlywoodUnsupportedNativeJoinShape, /cannot span two engines/);
    });
  });

  describe('cross engine — executed against mock engines', () => {
    const IMG = 'https://img/1.jpg';

    it("main first: the view receives exactly the distinct keys of main's result; orphans keep the column undefined", async () => {
      let viewSql = null;
      const druid = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        if (/GROUP BY \(\)/.test(sql)) return Promise.resolve([{ __VALUE__: 3 }]);
        return Promise.resolve([
          { __join_imageUrl: 'u1', productName: 'P1', count: 1 },
          { __join_imageUrl: 'u2', productName: 'P2', count: 1 },
          { __join_imageUrl: 'u1', productName: 'P3', count: 1 },
        ]);
      });
      const pg = promiseFnToStream(rq => {
        viewSql = sqlOf(rq);
        return Promise.resolve([{ __join_imageUrl: 'u1', generated_image: IMG }]);
      });
      const ds = await query(TIME, {
        productName: $('productName'),
        generated_image: $('generated_image'),
      }).compute({ main: makeMain({ where: 'postgres', requester: druid, pgRequester: pg }) });
      expect(viewSql, 'the view was queried').to.be.a('string');
      expect(viewSql, 'keys deduplicated, both present').to.match(/"imageUrl" IN \('u1','u2'\)/);
      const rows = ds.toJS().data[0].SPLIT.data;
      expect(rows.map(r => [r.productName, r.generated_image]).sort()).to.deep.equal([
        ['P1', IMG],
        ['P2', undefined],
        ['P3', IMG],
      ]);
    });

    it('above the key cap the view is scanned whole and the join still matches in memory', async () => {
      const saved = External.CROSS_SOURCE_KEY_LIST_MAX;
      External.CROSS_SOURCE_KEY_LIST_MAX = 1;
      try {
        let viewSql = null;
        const druid = promiseFnToStream(rq => {
          const sql = sqlOf(rq);
          if (/GROUP BY \(\)/.test(sql)) return Promise.resolve([{ __VALUE__: 2 }]);
          return Promise.resolve([
            { __join_imageUrl: 'u1', productName: 'P1', count: 1 },
            { __join_imageUrl: 'u2', productName: 'P2', count: 1 },
          ]);
        });
        const pg = promiseFnToStream(rq => {
          viewSql = sqlOf(rq);
          return Promise.resolve([{ __join_imageUrl: 'u2', generated_image: IMG }]);
        });
        const ds = await query(TIME, {
          productName: $('productName'),
          generated_image: $('generated_image'),
        }).compute({ main: makeMain({ where: 'postgres', requester: druid, pgRequester: pg }) });
        expect(viewSql).to.not.match(/WHERE/);
        const rows = ds.toJS().data[0].SPLIT.data;
        expect(rows.find(r => r.productName === 'P2').generated_image).to.equal(IMG);
        expect(rows.find(r => r.productName === 'P1').generated_image).to.equal(undefined);
      } finally {
        External.CROSS_SOURCE_KEY_LIST_MAX = saved;
      }
    });

    it('lookup first (filter + split on the mapping column): main is asked only for the keys behind the filter value', async () => {
      // S4 of the load test: filter familia = F3 AND split by familia. The
      // linked side (inner, filtered) runs first; main's join-key-grain query
      // carries `imageUrl IN (<its keys>)` instead of grouping every image.
      const filter = TIME.and($('familia').is(Expression.fromJS({ op: 'literal', value: 'F3' })));
      const mainSqls = [];
      const druid = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        mainSqls.push(sql);
        if (/GROUP BY \(\)/.test(sql)) return Promise.resolve([{ __VALUE__: 5 }]);
        if (/"imageUrl" IN \('u1','u3'\)/.test(sql)) {
          return Promise.resolve([
            { __join_imageUrl: 'u1', count: 4 },
            { __join_imageUrl: 'u3', count: 1 },
          ]);
        }
        return Promise.reject(new Error('main split asked without the key restriction: ' + sql));
      });
      const pg = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        expect(sql).to.match(/"familia"/);
        // The split's lookup (keyed by the synthetic alias) and the totals'
        // DISTINCT-key query (semijoin-to-root) both answer the F3 images.
        if (/__join_imageUrl/.test(sql)) {
          return Promise.resolve([
            { __join_imageUrl: 'u1', familia: 'F3' },
            { __join_imageUrl: 'u3', familia: 'F3' },
          ]);
        }
        return Promise.resolve([{ imageUrl: 'u1' }, { imageUrl: 'u3' }]);
      });
      const ds = await query(filter, { familia: $('familia') }).compute({
        main: makeMain({ where: 'postgres', joinMode: 'inner', requester: druid, pgRequester: pg }),
      });
      const rows = ds.toJS().data[0].SPLIT.data;
      expect(rows).to.deep.equal([{ familia: 'F3', count: 5 }]);
      const split = mainSqls.find(s => /__join_imageUrl/.test(s));
      expect(split, 'main join-key query carried the key list').to.match(
        /"imageUrl" IN \('u1','u3'\)/,
      );
    });

    it('lookup first: an empty key set asks main for no rows (IN of nothing), never for everything', async () => {
      const filter = TIME.and($('familia').is(Expression.fromJS({ op: 'literal', value: 'F9' })));
      const mainSqls = [];
      const druid = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        mainSqls.push(sql);
        return Promise.resolve([]);
      });
      const pg = promiseFnToStream(() => Promise.resolve([]));
      const ds = await query(filter, { familia: $('familia') }).compute({
        main: makeMain({ where: 'postgres', joinMode: 'inner', requester: druid, pgRequester: pg }),
      });
      expect(ds.toJS().data[0].SPLIT.data).to.deep.equal([]);
      const split = mainSqls.find(s => /__join_imageUrl/.test(s));
      expect(split, 'main join-key query emitted').to.exist;
      expect(split, 'restricted to no key').to.match(/FALSE|"imageUrl" IN \(\)/i);
    });
  });

  describe('measure-level filter on a mapped column — the "generated images" measure', () => {
    const GEN = '$main.filter($generated_image.isnt(null)).count()';
    const GEN_ONE = "$main.filter($generated_image == 'https://x/1.jpg').count()";

    it('same engine, split by product: conditional aggregate over the native JOIN; the sibling count stays unrestricted', () => {
      const sqls = planSqls(
        query(TIME, { productName: $('productName') }, [
          ['count', '$main.count()'],
          ['gen', GEN_ONE],
        ]),
        makeMain({ where: 'druid' }),
      );
      const join = sqls.find(s => /LEFT JOIN "mapping_abc" AS lookup/.test(s));
      expect(join, 'native JOIN emitted for the split').to.exist;
      expect(join).to.match(/"main"\."productName" AS "productName"/);
      expect(join, 'plain count untouched').to.match(/COUNT\(\*\) AS "count"/);
      expect(join, 'measure filter as a conditional aggregate on the lookup alias').to.match(
        /SUM\(CASE WHEN \(("lookup"|lookup)\."generated_image"='https:\/\/x\/1\.jpg'\) THEN 1 ELSE 0 END\) AS "gen"/,
      );
      expect(join, 'the measure filter never becomes a WHERE clause').to.not.match(
        /WHERE[\s\S]*generated_image/,
      );
      expect(join).to.match(/GROUP BY 1\n/);
    });

    it('same engine, totals: one native JOIN statement with GROUP BY () and the conditional count', () => {
      const sqls = planSqls(query(TIME).apply('gen', GEN), makeMain({ where: 'druid' }));
      const totals = sqls.find(
        s => /JOIN "mapping_abc" AS lookup/.test(s) && /GROUP BY \(\)/.test(s),
      );
      expect(totals, 'totals joined natively').to.exist;
      expect(totals).to.match(/COUNT\(\*\) AS "count"/);
      expect(totals).to.match(
        /SUM\(CASE WHEN \(?\(("lookup"|lookup)\."generated_image" IS (NOT NULL|NULL\) IS NOT TRUE)\)? THEN 1 ELSE 0 END\) AS "gen"/,
      );
      expect(totals, 'LEFT join keeps every main row for the plain count').to.match(/LEFT JOIN/);
      expect(
        sqls.filter(s => s.includes('"histories"')),
        'one statement for the totals',
      ).to.have.length(1);
    });

    it('same engine, single value: the bare number comes back from the joined statement', async () => {
      let sql = null;
      const druid = promiseFnToStream(rq => {
        sql = sqlOf(rq);
        return Promise.resolve([{ __VALUE__: 42 }]);
      });
      const v = await ply()
        .apply('main', $('main').filter(TIME))
        .apply(MAP, $(MAP).filter(TIME))
        .apply('gen', GEN)
        .compute({ main: makeMain({ where: 'druid', requester: druid }) });
      expect(sql, 'joined statement dispatched').to.match(/LEFT JOIN "mapping_abc" AS lookup/);
      expect(sql).to.match(/AS "__VALUE__"/);
      expect(v.toJS().data[0].gen).to.equal(42);
    });

    it('cross engine: refused loudly BY MEASURE NAME (the front marks only that measure as materializing)', () => {
      expect(() =>
        planSqls(
          query(TIME, { productName: $('productName') }, [
            ['count', '$main.count()'],
            ['gen', GEN_ONE],
          ]),
          makeMain({ where: 'postgres' }),
        ),
      ).to.throw(
        plywood.PlywoodUnsupportedNativeJoinShape,
        /measure\(s\) \[gen\] filter on a column of linkedSource "mapping_img"/,
      );
    });

    it('regression: the measure filter is never harvested as a cube filter (0.51.9 restricted every sibling measure)', () => {
      // Cross-engine, totals only: before 0.51.10 the clause was hoisted into a
      // semijoin and the plain count came back filtered. Now the shape is
      // refused instead — and a plain count WITHOUT the measure is untouched.
      const sqls = planSqls(query(TIME), makeMain({ where: 'postgres' }));
      expect(totalsSql(sqls)).to.not.match(/imageUrl|generated_image/);
      expect(() =>
        planSqls(query(TIME).apply('gen', GEN_ONE), makeMain({ where: 'postgres' })),
      ).to.throw(plywood.PlywoodUnsupportedNativeJoinShape);
    });

    it('the refusal names the measures and the linked source as fields, so a host can drop exactly those applies', () => {
      let caught;
      try {
        planSqls(query(TIME).apply('gen', GEN_ONE), makeMain({ where: 'postgres' }));
      } catch (e) {
        caught = e;
      }
      expect(caught).to.be.instanceOf(plywood.PlywoodUnsupportedNativeJoinShape);
      expect(caught.name).to.equal('PlywoodUnsupportedNativeJoinShape');
      expect(caught.measures).to.deep.equal(['gen']);
      expect(caught.linkedSource).to.equal(MAP);
      expect(caught.message).to.match(/measure\(s\) \[gen\]/);
    });
  });
});
