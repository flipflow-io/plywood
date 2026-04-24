/*
 * Spec for magic-attribute dim-only cross-source shape.
 *
 * Two fixtures are exercised:
 *   `makeMainWithDerivedAttrLookup` — the original reconciler output,
 *     where the user-facing dim name lives only in
 *     linkedSource.derivedAttributes (aliased onto a raw column
 *     'category'). Plywood HEAD (bb5ecfb) fails to resolve the split
 *     ref's type through this alias — split.mapSplits crashes with
 *     "unsupported simulation on: null". Three red specs pin this bug.
 *   `makeMainWithRawAttrLookup` — the Plan-B reconciler output, where
 *     the user-facing dim name is materialised as a raw column on the
 *     lookup. Decomposition works and produces two queries + join.
 *
 * Also covers the turnilo-style nested expression shape (filter-on-main
 * wrapped in an outer apply rebinding 'main', split/limit chain around
 * the SPLIT apply) — that's the structure the /plywood route receives
 * from the client and must decompose the same way as the flat form.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { Expression, External, $ } = plywood;

/**
 * Wrap a promise-returning query handler into the requester shape
 * plywood expects (a PassThrough of row objects). Mirrors the helper
 * used in test/external/druidSqlExternalIntrospection.mocha.js.
 */
function promiseFnToStream(promiseRq) {
  return rq => {
    const stream = new PassThrough({ objectMode: true });
    promiseRq(rq).then(
      res => {
        if (Array.isArray(res)) {
          for (const row of res) stream.write(row);
        } else if (res) {
          stream.write(res);
        }
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
  start: new Date('2026-04-22T00:00:00Z'),
  end: new Date('2026-04-23T00:00:00Z'),
});

/**
 * Original reconciler output (pre-Plan-B). The lookup schema is
 * canonical (`category`) and the user-facing name lives behind a
 * derivedAttribute alias.
 */
const makeMainWithDerivedAttrLookup = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'histories-42f0bec',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'brand', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
      { name: 'pvp', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      magic_abc: {
        source: 'lookup_abc_rev1',
        joinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: 'inner',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'category', type: 'STRING' },
        ],
        derivedAttributes: {
          brand_tier: '$category',
        },
      },
    },
    filter: timeFilter,
  });

/**
 * Plan-B reconciler output: the MSQ INSERT names the output column
 * under spec.name, so the lookup schema is
 * `[time, brand, brand_tier]` and linkedSource exposes it as a raw
 * attribute. No derivedAttribute indirection.
 */
const makeMainWithRawAttrLookup = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'histories-42f0bec',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'brand', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
      { name: 'pvp', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      magic_abc: {
        source: 'lookup_abc_rev1',
        joinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: 'inner',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'brand_tier', type: 'STRING' },
          { name: 'confidence', type: 'NUMBER' },
        ],
      },
    },
    filter: timeFilter,
  });

describe('External decomposition — magic-attribute dim-only shape', () => {
  // Green control on the pre-Plan-B fixture. Split by a raw linked
  // attribute (category). Pins that the dim-only path itself works
  // when the split ref resolves via ls.attributes.
  it('[control] split by raw linked attribute (category) → two queries + join', () => {
    const ex = $('main')
      .split('$category', 'category')
      .apply('AvgPrice', '$main.average($price)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithDerivedAttrLookup() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQueries = queries.filter(
      q => q.query.includes('"histories-42f0bec"') && !q.query.includes('lookup_abc_rev1'),
    );
    const linkedQueries = queries.filter(q => q.query.includes('"lookup_abc_rev1"'));

    expect(mainQueries, 'main query').to.have.length(1);
    expect(linkedQueries, 'lookup query').to.have.length(1);
    expect(linkedQueries[0].query).to.match(/"category"/);
  });

  // Control on Plan-B fixture: split by brand_tier as a raw linked
  // attribute. Mirrors the post-migration shape. Expected green —
  // same structural case as the control above.
  it('[plan-B] split by raw linked attribute (brand_tier) → two queries + join', () => {
    const ex = $('main')
      .split('$brand_tier', 'brand_tier')
      .apply('AvgPrice', '$main.average($price)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithRawAttrLookup() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQueries = queries.filter(
      q => q.query.includes('"histories-42f0bec"') && !q.query.includes('lookup_abc_rev1'),
    );
    const linkedQueries = queries.filter(q => q.query.includes('"lookup_abc_rev1"'));

    expect(mainQueries, 'main query').to.have.length(1);
    expect(linkedQueries, 'lookup query').to.have.length(1);
    expect(linkedQueries[0].query).to.match(/"brand_tier"/);
  });

  // Second green control. Split by a main attribute only — declaring
  // linkedSources must NOT force decomposition on queries that don't
  // touch linked data.
  it('[control] split by main attribute only → single query, lookup untouched', () => {
    const ex = $('main')
      .split('$brand', 'Brand')
      .apply('AvgPrice', '$main.average($price)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithRawAttrLookup() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    expect(queries).to.have.length(1);
    expect(queries[0].query).to.include('"histories-42f0bec"');
    expect(queries[0].query).to.not.include('lookup_abc_rev1');
  });

  // Bug spec: split ref resolves only through ls.derivedAttributes.
  // Red against plywood HEAD bb5ecfb. Left RED as a canary; Plan-B
  // avoids hitting this path entirely.
  it('[RED canary] split via ls.derivedAttributes alias → fails in current plywood', () => {
    const ex = $('main')
      .split('$brand_tier', 'brand_tier')
      .apply('AvgPrice', '$main.average($price)');

    expect(() => ex.simulateQueryPlan({ main: makeMainWithDerivedAttrLookup() }))
      .to.throw(/unsupported simulation on: null/);
  });

  // Turnilo-style nested expression — the exact shape the /plywood
  // client builds. An outer apply rebinds `$main` to a time-filtered
  // version, then a named SPLIT apply wraps the split+limit chain.
  // Pins that this wrapping doesn't suppress the decomposition trigger
  // that the flat equivalent above exercises successfully.
  //
  // Flat form (works): $main.split(X).apply(Y)
  // Turnilo form:
  //   ply()
  //     .apply('main',  $main.filter(time in R))
  //     .apply('SPLIT', $main.split(X).apply(Y).limit(N))
  it('[plan-B] turnilo-style nested expression with filter+limit wrapper → decomposes', () => {
    const ex = plywood.ply()
      .apply('main', $('main').filter('$time.overlap({...})' ? timeFilter : timeFilter))
      .apply(
        'SPLIT',
        $('main')
          .split('$brand_tier', 'brand_tier')
          .apply('AvgPrice', '$main.average($price)')
          .limit(10),
      );

    const plan = ex.simulateQueryPlan({ main: makeMainWithRawAttrLookup() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQueries = queries.filter(
      q => q.query.includes('"histories-42f0bec"') && !q.query.includes('lookup_abc_rev1'),
    );
    const linkedQueries = queries.filter(q => q.query.includes('"lookup_abc_rev1"'));

    expect(mainQueries, 'main query').to.have.length.at.least(1);
    expect(linkedQueries, 'lookup query').to.have.length.at.least(1);
  });

  // Even closer to what turnilo's client emits — use Expression.fromJS
  // with the literal tree captured from a real /plywood POST. This is
  // the ground-truth shape that the runtime hits and must decompose.
  //
  // The shape (simplified):
  //   apply(apply(literal(emptyDataset), filter(main, timeRange), "main"),
  //         limit(apply(split(main, brand_tier), avg(price), "avgPrice"), 10),
  //         "SPLIT")
  // GROUND-TRUTH shape captured from a real /plywood POST body. Outer
  // apply rebinds $main to a time-filtered view, followed by a chain
  // of four totals-level applies (MillisecondsInInterval, avg_price,
  // avg_pvp, min_price, diff_with_pvp), all on $main, then the SPLIT
  // apply holding a split+4 apply+sort+limit chain. No foreign apply
  // anywhere — the only cross-source indicator is the split ref to
  // `$brand_tier` which resolves on the linked side.
  //
  // This is what turnilo's client sends. If this test turns green but
  // the live /plywood returns SPLIT without the brand_tier key, the
  // divergence is elsewhere (runtime wiring, not expression shape).
  it('[plan-B] Expression.fromJS of FULL real /plywood body decomposes', () => {
    const timeRange = {
      op: 'literal',
      value: {
        setType: 'TIME_RANGE',
        elements: [
          { start: '2026-04-22T00:00:00.000Z', end: '2026-04-23T00:00:00.000Z' },
        ],
      },
      type: 'SET',
    };
    const avgRef = (col) => ({
      op: 'average',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: col },
    });
    const minRef = (col) => ({
      op: 'min',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: col },
    });

    const applyChain = {
      op: 'apply',
      operand: {
        op: 'apply',
        operand: {
          op: 'apply',
          operand: {
            op: 'apply',
            operand: {
              op: 'apply',
              operand: {
                op: 'apply',
                operand: {
                  op: 'apply',
                  operand: {
                    op: 'apply',
                    operand: {
                      op: 'literal',
                      value: { attributes: [], data: [{}] },
                      type: 'DATASET',
                    },
                    expression: {
                      op: 'filter',
                      operand: { op: 'ref', name: 'main' },
                      expression: {
                        op: 'overlap',
                        operand: { op: 'ref', name: '__time' },
                        expression: timeRange,
                      },
                    },
                    name: 'main',
                  },
                  expression: { op: 'literal', value: 86400000 },
                  name: 'MillisecondsInInterval',
                },
                expression: avgRef('price'),
                name: 'avg_price',
              },
              expression: avgRef('pvp'),
              name: 'avg_pvp',
            },
            expression: minRef('price'),
            name: 'min_price',
          },
          expression: {
            op: 'divide',
            operand: { op: 'subtract', operand: avgRef('price'), expression: avgRef('pvp') },
            expression: avgRef('pvp'),
          },
          name: 'diff_with_pvp',
        },
        expression: {
          op: 'limit',
          operand: {
            op: 'apply',
            operand: {
              op: 'apply',
              operand: {
                op: 'apply',
                operand: {
                  op: 'apply',
                  operand: {
                    op: 'split',
                    operand: { op: 'ref', name: 'main' },
                    name: 'brand_tier',
                    expression: { op: 'ref', name: 'brand_tier' },
                    dataName: 'main',
                  },
                  expression: avgRef('price'),
                  name: 'avg_price',
                },
                expression: avgRef('pvp'),
                name: 'avg_pvp',
              },
              expression: minRef('price'),
              name: 'min_price',
            },
            expression: {
              op: 'divide',
              operand: {
                op: 'subtract',
                operand: avgRef('price'),
                expression: avgRef('pvp'),
              },
              expression: avgRef('pvp'),
            },
            name: 'diff_with_pvp',
          },
          value: 50,
        },
        name: 'SPLIT',
      },
      expression: { op: 'literal', value: 86400000 },
      name: 'MillisecondsInInterval',
    };

    const ex = Expression.fromJS(applyChain);
    const plan = ex.simulateQueryPlan({ main: makeMainWithRawAttrLookup() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQueries = queries.filter(
      q => q.query.includes('"histories-42f0bec"') && !q.query.includes('lookup_abc_rev1'),
    );
    const linkedQueries = queries.filter(q => q.query.includes('"lookup_abc_rev1"'));

    expect(mainQueries, 'main query').to.have.length.at.least(1);
    expect(linkedQueries, 'lookup query').to.have.length.at.least(1);
  });

  // The REAL runtime path is `ex.compute(datasets)` — NOT
  // `simulateQueryPlan`. They go through different External methods
  // (queryValue vs simulateValue), so simulate-passing does not imply
  // compute-passing. This spec exercises the compute() path with a
  // proper stream-shaped mock requester (mirrors how turnilo's live
  // Druid requester behaves). If compute dispatches both a main and a
  // linked query here, the runtime should too — and the empty-cells
  // behaviour observed in turnilo is due to some later layer (the
  // server's executor wrapping, a cube caching path, etc.) rather
  // than plywood itself.
  it('[plan-B] compute() dispatches main + linked queries via mock requester', async () => {
    const dispatched = [];
    const requester = promiseFnToStream(rq => {
      dispatched.push(rq);
      const sql = (rq && rq.query && rq.query.query) || '';
      // Return a single row per query so plywood can assemble a
      // Dataset — the row shape must carry the SELECT columns.
      if (sql.includes('"lookup_abc_rev1"')) {
        // lookup side: join key + category
        return Promise.resolve([{ __join_brand: 'Jbl', brand_tier: 'premium' }]);
      }
      if (sql.includes('"histories-42f0bec"')) {
        // main side: join key + avgPrice
        return Promise.resolve([{ __join_brand: 'Jbl', avgPrice: 42 }]);
      }
      return Promise.resolve([]);
    });

    const mainValue = {
      engine: 'druidsql',
      source: 'histories-42f0bec',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
      ],
      linkedSources: {
        magic_abc: {
          source: 'lookup_abc_rev1',
          joinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          attributes: [
            { name: '__time', type: 'TIME' },
            { name: 'brand', type: 'STRING' },
            { name: 'brand_tier', type: 'STRING' },
          ],
        },
      },
    };
    const main = External.fromJS(mainValue, requester);

    const ex = $('main')
      .filter(
        $('__time').overlap({
          start: new Date('2026-04-22T00:00:00Z'),
          end: new Date('2026-04-23T00:00:00Z'),
        }),
      )
      .split('$brand_tier', 'brand_tier')
      .apply('avgPrice', '$main.average($price)')
      .limit(10);

    await ex.compute({ main });

    const sqls = dispatched.map(rq => (rq && rq.query && rq.query.query) || '');
    const mainSql = sqls.filter(
      q => q.includes('"histories-42f0bec"') && !q.includes('lookup_abc_rev1'),
    );
    const linkedSql = sqls.filter(q => q.includes('"lookup_abc_rev1"'));

    expect(mainSql, 'main SQL dispatched').to.have.length.at.least(1);
    expect(linkedSql, 'lookup SQL dispatched').to.have.length.at.least(1);
  });

  // Same compute() check, but driving the FULL nested expression
  // shape captured from the real /plywood POST (totals apply chain
  // wrapping the SPLIT apply). If simulate passes but compute fails
  // on THIS shape, the divergence is plywood's compute path around
  // nested applies + cross-source splits.
  it('[plan-B] compute() on FULL real /plywood expression dispatches main + linked', async () => {
    const dispatched = [];
    const requester = promiseFnToStream(rq => {
      dispatched.push(rq);
      const sql = (rq && rq.query && rq.query.query) || '';
      if (sql.includes('"lookup_abc_rev1"')) {
        return Promise.resolve([{ __join_brand: 'Jbl', brand_tier: 'premium' }]);
      }
      if (sql.includes('"histories-42f0bec"')) {
        return Promise.resolve([
          {
            __join_brand: 'Jbl',
            avg_price: 42,
            avg_pvp: 50,
            min_price: 10,
            diff_with_pvp: -0.16,
          },
        ]);
      }
      return Promise.resolve([]);
    });

    const main = External.fromJS(
      {
        engine: 'druidsql',
        source: 'histories-42f0bec',
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
          { name: 'pvp', type: 'NUMBER', unsplitable: true },
        ],
        linkedSources: {
          magic_abc: {
            source: 'lookup_abc_rev1',
            joinKeys: ['brand'],
            sharedDimensions: ['brand'],
            joinMode: 'inner',
            attributes: [
              { name: '__time', type: 'TIME' },
              { name: 'brand', type: 'STRING' },
              { name: 'brand_tier', type: 'STRING' },
            ],
          },
        },
      },
      requester,
    );

    const timeRange = {
      op: 'literal',
      value: {
        setType: 'TIME_RANGE',
        elements: [
          { start: '2026-04-22T00:00:00.000Z', end: '2026-04-23T00:00:00.000Z' },
        ],
      },
      type: 'SET',
    };
    const avgRef = col => ({
      op: 'average',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: col },
    });
    const minRef = col => ({
      op: 'min',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: col },
    });
    const applyChain = {
      op: 'apply',
      operand: {
        op: 'apply',
        operand: {
          op: 'apply',
          operand: {
            op: 'apply',
            operand: {
              op: 'apply',
              operand: {
                op: 'apply',
                operand: {
                  op: 'literal',
                  value: { attributes: [], data: [{}] },
                  type: 'DATASET',
                },
                expression: {
                  op: 'filter',
                  operand: { op: 'ref', name: 'main' },
                  expression: {
                    op: 'overlap',
                    operand: { op: 'ref', name: '__time' },
                    expression: timeRange,
                  },
                },
                name: 'main',
              },
              expression: avgRef('price'),
              name: 'avg_price',
            },
            expression: avgRef('pvp'),
            name: 'avg_pvp',
          },
          expression: minRef('price'),
          name: 'min_price',
        },
        expression: {
          op: 'divide',
          operand: { op: 'subtract', operand: avgRef('price'), expression: avgRef('pvp') },
          expression: avgRef('pvp'),
        },
        name: 'diff_with_pvp',
      },
      expression: {
        op: 'limit',
        operand: {
          op: 'apply',
          operand: {
            op: 'apply',
            operand: {
              op: 'apply',
              operand: {
                op: 'apply',
                operand: {
                  op: 'split',
                  operand: { op: 'ref', name: 'main' },
                  name: 'brand_tier',
                  expression: { op: 'ref', name: 'brand_tier' },
                  dataName: 'main',
                },
                expression: avgRef('price'),
                name: 'avg_price',
              },
              expression: avgRef('pvp'),
              name: 'avg_pvp',
            },
            expression: minRef('price'),
            name: 'min_price',
          },
          expression: {
            op: 'divide',
            operand: { op: 'subtract', operand: avgRef('price'), expression: avgRef('pvp') },
            expression: avgRef('pvp'),
          },
          name: 'diff_with_pvp',
        },
        value: 50,
      },
      name: 'SPLIT',
    };

    const ex = Expression.fromJS(applyChain);
    await ex.compute({ main });

    const sqls = dispatched.map(rq => (rq && rq.query && rq.query.query) || '');
    const mainSql = sqls.filter(
      q => q.includes('"histories-42f0bec"') && !q.includes('lookup_abc_rev1'),
    );
    const linkedSql = sqls.filter(q => q.includes('"lookup_abc_rev1"'));

    expect(mainSql, 'main SQL dispatched').to.have.length.at.least(1);
    expect(linkedSql, 'lookup SQL dispatched').to.have.length.at.least(1);
  });

  // The SIMPLEST real runtime shape: the client's curl body stripped
  // to just `apply(literal, filter(main, time), 'main') expression=
  // apply(literal_top, SPLIT_apply, 'SPLIT')`. Observed in the live
  // server: returns SPLIT.data with measure values but no brand_tier
  // key and NO Druid query fires (plywood completely skips the
  // external). This test tries to reproduce that in pure plywood.
  it('[plan-B] compute() on the curl-simple shape — dispatches, does NOT skip external', async () => {
    const dispatched = [];
    const requester = promiseFnToStream(rq => {
      dispatched.push(rq);
      const sql = (rq && rq.query && rq.query.query) || '';
      if (sql.includes('"lookup_abc_rev1"')) {
        return Promise.resolve([
          { __join_brand: 'Jbl', brand_tier: 'premium' },
          { __join_brand: 'Harman', brand_tier: 'premium' },
        ]);
      }
      if (sql.includes('"histories-42f0bec"')) {
        return Promise.resolve([
          { __join_brand: 'Jbl', avgPrice: 6200 },
          { __join_brand: 'Harman', avgPrice: 4207 },
        ]);
      }
      return Promise.resolve([]);
    });

    const main = External.fromJS(
      {
        engine: 'druidsql',
        source: 'histories-42f0bec',
        suppress: true,   // ← matches turnilo's dataCubeToExternal.ts
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
        ],
        linkedSources: {
          magic_abc: {
            source: 'lookup_abc_rev1',
            joinKeys: ['brand'],
            sharedDimensions: ['brand'],
            joinMode: 'inner',
            attributes: [
              { name: '__time', type: 'TIME' },
              { name: 'brand', type: 'STRING' },
              { name: 'brand_tier', type: 'STRING' },
              { name: 'confidence', type: 'NUMBER' },
            ],
          },
        },
      },
      requester,
    );

    // EXACT curl body shape (stream=false), simplified (1 measure).
    const body = {
      op: 'apply',
      operand: {
        op: 'literal',
        value: { attributes: [], data: [{}] },
        type: 'DATASET',
      },
      expression: {
        op: 'limit',
        operand: {
          op: 'apply',
          operand: {
            op: 'split',
            operand: {
              op: 'filter',
              operand: { op: 'ref', name: 'main' },
              expression: {
                op: 'overlap',
                operand: { op: 'ref', name: '__time' },
                expression: {
                  op: 'literal',
                  value: {
                    setType: 'TIME_RANGE',
                    elements: [
                      {
                        start: '2026-04-22T00:00:00.000Z',
                        end: '2026-04-24T00:00:00.000Z',
                      },
                    ],
                  },
                  type: 'SET',
                },
              },
            },
            name: 'brand_tier',
            expression: { op: 'ref', name: 'brand_tier' },
            dataName: 'main',
          },
          expression: {
            op: 'average',
            operand: { op: 'ref', name: 'main' },
            expression: { op: 'ref', name: 'price' },
          },
          name: 'avgPrice',
        },
        value: 10,
      },
      name: 'SPLIT',
    };

    const ex = Expression.fromJS(body);
    const result = await ex.compute({ main });

    const sqls = dispatched.map(rq => (rq && rq.query && rq.query.query) || '');
    const mainSql = sqls.filter(
      q => q.includes('"histories-42f0bec"') && !q.includes('lookup_abc_rev1'),
    );
    const linkedSql = sqls.filter(q => q.includes('"lookup_abc_rev1"'));

    // At least ONE query of each — this is what live runtime skips.
    expect(mainSql, 'main SQL dispatched').to.have.length.at.least(1);
    expect(linkedSql, 'lookup SQL dispatched').to.have.length.at.least(1);

    // And the result should carry brand_tier on the SPLIT rows.
    const resultJs = result && result.toJS ? result.toJS() : result;
    // eslint-disable-next-line no-console
    console.log('[curl-shape result]', JSON.stringify(resultJs).slice(0, 400));
    const splitRows =
      resultJs && resultJs.data && resultJs.data[0] && resultJs.data[0].SPLIT
        ? resultJs.data[0].SPLIT.data
        : [];
    const keys = splitRows.length > 0 ? Object.keys(splitRows[0]) : [];
    expect(keys, 'SPLIT row carries brand_tier').to.include('brand_tier');
  });

  // ───────────────────────────────────────────────────────────────────────
  // timeAlignment contract — see
  // turnilo-er-redesign/packages/tyrell-kernel/src/magic-attributes/validators.ts
  // A linkedSource declares how its __time relates to main's:
  //   - undefined / "bucketed" — main's time filter propagates to the
  //     lookup query (current behaviour; bucketed joins).
  //   - "eternal"              — main's time filter is stripped before
  //     querying the lookup (the lookup is a materialised snapshot at a
  //     sentinel timestamp; applying main's interval returns zero rows
  //     and the join silently drops linked columns).
  //
  // Four fixtures pin the contract:
  //   (a) bucketed-default — time filter must appear in lookup SQL.
  //   (b) eternal-strip    — time filter must NOT appear in lookup SQL.
  //   (c) eternal-compute  — full compute() with 1970-timestamped lookup
  //                          rows returns main rows enriched with
  //                          brand_tier (the live-runtime failure mode).
  //   (d) bucketed-compute — explicit "bucketed" matches default compute
  //                          behaviour — a regression guard.
  // ───────────────────────────────────────────────────────────────────────

  const makeMainWithTimeAlignment = alignment =>
    External.fromJS({
      engine: 'druidsql',
      source: 'histories-42f0bec',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
      ],
      linkedSources: {
        magic_abc: {
          source: 'lookup_abc_rev1',
          joinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          ...(alignment !== undefined ? { timeAlignment: alignment } : {}),
          attributes: [
            { name: '__time', type: 'TIME' },
            { name: 'brand', type: 'STRING' },
            { name: 'brand_tier', type: 'STRING' },
          ],
        },
      },
      filter: timeFilter,
    });

  it('[timeAlignment=bucketed default] lookup SQL carries main time filter', () => {
    const ex = $('main')
      .split('$brand_tier', 'brand_tier')
      .apply('AvgPrice', '$main.average($price)');

    const queries = ex
      .simulateQueryPlan({ main: makeMainWithTimeAlignment(undefined) })
      .flat()
      .filter(q => typeof q.query === 'string');

    const linked = queries.filter(q => q.query.includes('"lookup_abc_rev1"'));
    expect(linked, 'lookup query dispatched').to.have.length(1);
    // Bucketed default: time filter must propagate.
    expect(linked[0].query, 'bucketed lookup carries time filter').to.match(
      /"__time"/,
    );
    expect(linked[0].query).to.match(/2026-04-22/);
  });

  it('[timeAlignment=eternal] lookup SQL drops main time filter', () => {
    const ex = $('main')
      .split('$brand_tier', 'brand_tier')
      .apply('AvgPrice', '$main.average($price)');

    const queries = ex
      .simulateQueryPlan({ main: makeMainWithTimeAlignment('eternal') })
      .flat()
      .filter(q => typeof q.query === 'string');

    const mainQ = queries.filter(
      q => q.query.includes('"histories-42f0bec"') && !q.query.includes('lookup_abc_rev1'),
    );
    const linked = queries.filter(q => q.query.includes('"lookup_abc_rev1"'));

    expect(mainQ, 'main still dispatched').to.have.length(1);
    expect(linked, 'lookup query dispatched').to.have.length(1);

    // Main keeps its time filter.
    expect(mainQ[0].query, 'main retains time filter').to.match(/2026-04-22/);

    // Lookup must NOT reference __time in its WHERE (the point of eternal).
    expect(linked[0].query, 'lookup query text').to.not.match(/"__time"/);
    expect(linked[0].query, 'lookup query has no 2026 bound').to.not.match(
      /2026-04-22/,
    );
  });

  it('[timeAlignment=eternal compute] lookup at 1970 joined into main rows', async () => {
    const dispatched = [];
    const requester = promiseFnToStream(rq => {
      dispatched.push(rq);
      const sql = (rq && rq.query && rq.query.query) || '';
      if (sql.includes('"lookup_abc_rev1"')) {
        // Simulates the MSQ-materialised snapshot at 1970. The rows
        // carry no time bound so they'd be filtered out under
        // "bucketed"; under "eternal" the query has no time clause
        // and these rows survive.
        return Promise.resolve([
          { __join_brand: 'Jbl', brand_tier: 'premium' },
          { __join_brand: 'Harman', brand_tier: 'premium' },
        ]);
      }
      if (sql.includes('"histories-42f0bec"')) {
        return Promise.resolve([
          { __join_brand: 'Jbl', AvgPrice: 6200 },
          { __join_brand: 'Harman', AvgPrice: 4207 },
        ]);
      }
      return Promise.resolve([]);
    });

    const main = External.fromJS(
      {
        engine: 'druidsql',
        source: 'histories-42f0bec',
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
        ],
        linkedSources: {
          magic_abc: {
            source: 'lookup_abc_rev1',
            joinKeys: ['brand'],
            sharedDimensions: ['brand'],
            joinMode: 'inner',
            timeAlignment: 'eternal',
            attributes: [
              { name: '__time', type: 'TIME' },
              { name: 'brand', type: 'STRING' },
              { name: 'brand_tier', type: 'STRING' },
            ],
          },
        },
        filter: timeFilter,
      },
      requester,
    );

    const ex = $('main')
      .split('$brand_tier', 'brand_tier')
      .apply('AvgPrice', '$main.average($price)');

    const result = await ex.compute({ main });
    const sqls = dispatched.map(rq => (rq && rq.query && rq.query.query) || '');
    const linkedSql = sqls.find(q => q.includes('"lookup_abc_rev1"')) || '';
    expect(linkedSql, 'lookup dispatched without __time').to.not.match(/"__time"/);

    const resultJs = result && result.toJS ? result.toJS() : result;
    const rows = (resultJs && resultJs.data) || [];
    const keys = rows.length > 0 ? Object.keys(rows[0]) : [];
    expect(keys, 'compute result carries brand_tier').to.include('brand_tier');
    const tiers = rows.map(r => r.brand_tier).sort();
    expect(tiers, 'both rows enriched as premium').to.deep.equal(['premium', 'premium']);
  });

  it('[timeAlignment=bucketed explicit] matches default compute behaviour', async () => {
    const dispatched = [];
    const requester = promiseFnToStream(rq => {
      dispatched.push(rq);
      // No rows — the filter excludes them. Only behaviour under test
      // is the dispatched SQL shape.
      return Promise.resolve([]);
    });

    const main = External.fromJS(
      {
        engine: 'druidsql',
        source: 'histories-42f0bec',
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
        ],
        linkedSources: {
          magic_abc: {
            source: 'lookup_abc_rev1',
            joinKeys: ['brand'],
            sharedDimensions: ['brand'],
            joinMode: 'inner',
            timeAlignment: 'bucketed',
            attributes: [
              { name: '__time', type: 'TIME' },
              { name: 'brand', type: 'STRING' },
              { name: 'brand_tier', type: 'STRING' },
            ],
          },
        },
        filter: timeFilter,
      },
      requester,
    );

    const ex = $('main')
      .split('$brand_tier', 'brand_tier')
      .apply('AvgPrice', '$main.average($price)');

    await ex.compute({ main });
    const sqls = dispatched.map(rq => (rq && rq.query && rq.query.query) || '');
    const linked = sqls.find(q => q.includes('"lookup_abc_rev1"')) || '';
    expect(linked, 'explicit bucketed carries time filter').to.match(/"__time"/);
    expect(linked).to.match(/2026-04-22/);
  });
});
