/*
 * RED spec — Phase 4 native-JOIN SQL emission.
 *
 * When the decomposability gate diverts a query to the native-JOIN
 * path (countDistinct, quantile, average, mode, etc. + linked-only
 * split), the engine emits a SINGLE Druid SQL with an INNER/LEFT
 * JOIN against the lookup datasource. Pattern:
 *
 *   SELECT lookup.<linkedOnlyAlias> AS "<alias>",
 *          <measureSQL> AS "<measure>"
 *   FROM <main_source> main
 *   INNER|LEFT JOIN <lookup_source> lookup
 *     ON main.<joinKey> = lookup.<joinKey>
 *   WHERE <time + filters on main>
 *   GROUP BY 1
 *   [ORDER BY ... LIMIT ...]
 *
 * Result-shape: with stubbed main + linked rows (3 products → 2
 * buckets, main per-product values [10,20,30]), the executor must
 * return exactly 2 SPLIT rows, one per bucket, each with the
 * correctly re-aggregated measure.
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

const filterStart = new Date('2026-05-01T00:00:00Z');
const filterEnd = new Date('2026-05-02T00:00:00Z');
const outerTimeFilter = $('__time').overlap({ start: filterStart, end: filterEnd });
const innerTimeFilter = $('__time').overlap({ start: filterStart, end: filterEnd });

function makeMainWithUsoTipicoLookup(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'main_ds',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'productName', type: 'STRING' },
        { name: 'userIdProduct', type: 'STRING' },
      ],
      linkedSources: {
        lookup_uso_tipico: {
          source: 'lookup_uso_tipico_rev1',
          joinKeys: ['productName'],
          autoInjectJoinKeys: ['productName'],
          sharedDimensions: ['productName'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: 'productName', type: 'STRING' },
            { name: 'uso_tipico', type: 'STRING' },
          ],
        },
      },
      filter: outerTimeFilter,
    },
    requester,
  );
}

function buildUserExpression() {
  return ply()
    .apply('main', $('main').filter(innerTimeFilter))
    .apply('lookup_uso_tipico', $('lookup_uso_tipico').filter(innerTimeFilter))
    .apply(
      'SPLIT',
      $('main')
        .split('$uso_tipico', 'uso_tipico')
        .apply('unique_products', '$main.countDistinct($userIdProduct)')
        .sort('$uso_tipico', 'descending')
        .limit(5000),
    );
}

describe('Cross-source native-JOIN SQL emission (Phase 4)', () => {
  it('countDistinct + linked-only split emits exactly 1 SQL query with INNER JOIN', () => {
    const main = makeMainWithUsoTipicoLookup();
    const ex = buildUserExpression();
    const plan = ex.simulateQueryPlan({ main });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    // Native-JOIN path: a single SQL that mentions both sources.
    const joinQueries = queries.filter(
      q => q.query.includes('"main_ds"') && q.query.includes('"lookup_uso_tipico_rev1"'),
    );
    expect(joinQueries, 'one combined SQL with main + lookup').to.have.length(1);
    const sql = joinQueries[0].query;
    expect(sql, 'has INNER JOIN').to.match(/INNER\s+JOIN/i);
    expect(sql, 'JOINs on productName').to.match(/"productName"\s*=\s*[^=]*"productName"/);
    expect(sql, 'projects uso_tipico AS the split alias').to.match(
      /"uso_tipico"\s+AS\s+"uso_tipico"/,
    );
    expect(sql, 'projects COUNT(DISTINCT) as unique_products').to.match(
      /COUNT\(DISTINCT[^)]*"userIdProduct"\s*\)/i,
    );
    expect(sql, 'has GROUP BY').to.match(/GROUP BY 1/);
  });

  it('SPLIT.data row count equals distinct linked-only bucket count after compute()', async () => {
    const dispatched = [];
    const requester = promiseFnToStream(rq => {
      dispatched.push(rq);
      // Native-JOIN path: requester gets ONE SQL with both sources
      // referenced. Return the post-aggregation rows directly: the
      // executor reads them as the final SPLIT.data.
      const sql = (rq && rq.query && rq.query.query) || '';
      if (sql.includes('"main_ds"') && sql.includes('"lookup_uso_tipico_rev1"')) {
        // Stub: 3 products P1,P2,P3 → buckets Industrial,Industrial,Retail.
        // After native JOIN + GROUP BY uso_tipico + COUNT(DISTINCT
        // userIdProduct), the expected result is one row per bucket.
        // We return the post-aggregated shape the SQL would have
        // produced: 2 rows, one per bucket.
        return Promise.resolve([
          { uso_tipico: 'Industrial', unique_products: 2 },
          { uso_tipico: 'Retail', unique_products: 1 },
        ]);
      }
      // Outer totals (the apply('unique_products', ...) at ply level)
      if (sql.includes('"main_ds"')) {
        return Promise.resolve([{ __VALUE__: 3 }]);
      }
      return Promise.resolve([]);
    });

    const main = makeMainWithUsoTipicoLookup(requester);
    const result = await buildUserExpression().compute({ main });
    const resultJs = result && result.toJS ? result.toJS() : result;
    const splitRows =
      resultJs && resultJs.data && resultJs.data[0] && resultJs.data[0].SPLIT
        ? resultJs.data[0].SPLIT.data
        : null;

    expect(splitRows, 'SPLIT.data populated').to.be.an('array');
    expect(splitRows, 'exactly 2 bucket rows').to.have.length(2);
    const buckets = new Set(splitRows.map(r => r.uso_tipico));
    expect(buckets.has('Industrial')).to.equal(true);
    expect(buckets.has('Retail')).to.equal(true);
  });
});
