const { expect } = require('chai');

const plywood = require('../plywood');

const { Expression, External, Dataset, TimeRange, $, ply, r, s$ } = plywood;

const attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
];

const context = {
  diamonds: External.fromJS({
    engine: 'druidsql',
    version: '0.20.0',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $('time').overlap({
      start: new Date('2015-03-12T00:00:00Z'),
      end: new Date('2015-03-19T00:00:00Z'),
    }),
  }),
};

const timeFilter =
  '(TIMESTAMP \'2015-03-12 00:00:00\'<="time" AND "time"<TIMESTAMP \'2015-03-19 00:00:00\')';

describe('simulate DruidSql MODE', () => {
  // ============================================================
  // 1. MODE in value mode (no split, single mode apply)
  //    → scalar subquery, single query
  // ============================================================
  it('works with mode in total (no split)', () => {
    const ex = ply().apply('MostFrequentColor', $('diamonds').mode('$color'));

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0].length).to.equal(1);
    const query = queryPlan[0][0].query;
    // Scalar subquery for value mode
    expect(query).to.contain('SELECT');
    expect(query).to.contain('"color"');
    expect(query).to.contain('COUNT(*)');
    expect(query).to.contain('ORDER BY');
    expect(query).to.contain('LIMIT 1');
  });

  // ============================================================
  // 2. MODE with a split — decomposed into 2 queries
  //    Query 1: normal aggregates (COUNT)
  //    Query 2: mode via ROW_NUMBER subquery
  //    Results joined in memory by split key
  // ============================================================
  it('works with mode in split', () => {
    const ex = ply().apply(
      'Colors',
      $('diamonds')
        .split('$cut', 'Cut')
        .apply('Count', $('diamonds').count())
        .apply('MostFrequentColor', $('diamonds').mode('$color'))
        .sort('$Count', 'descending')
        .limit(10),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    // Decomposed: 2 queries in the same stage
    expect(queryPlan[0].length).to.equal(2);

    // Query 1: normal aggregates — clean, no CTE, no JOIN
    const q1 = queryPlan[0][0].query;
    expect(q1).to.contain('"cut" AS "Cut"');
    expect(q1).to.contain('COUNT(*) AS "Count"');
    expect(q1).to.contain('GROUP BY');
    expect(q1).to.contain('ORDER BY "Count" DESC');
    expect(q1).to.contain('LIMIT 10');
    expect(q1).to.not.contain('WITH');
    expect(q1).to.not.contain('LEFT JOIN');
    expect(q1).to.not.contain('ROW_NUMBER');
    expect(q1).to.not.contain('__mode');

    // Query 2: mode query with ROW_NUMBER
    const q2 = queryPlan[0][1].query;
    expect(q2).to.contain('"cut" AS "Cut"');
    expect(q2).to.contain('"color"');
    expect(q2).to.contain('ROW_NUMBER()');
    expect(q2).to.contain('PARTITION BY');
    expect(q2).to.contain('ORDER BY COUNT(*) DESC');
    expect(q2).to.contain('"color" IS NOT NULL');
    expect(q2).to.contain('"__rn" = 1');
    expect(q2).to.contain('AS "MostFrequentColor"');
    // Mode query should NOT contain normal aggregates
    expect(q2).to.not.contain('COUNT(*) AS "Count"');
  });

  // ============================================================
  // 3. MODE alongside COUNT + SUM — decomposed
  // ============================================================
  it('works with mode alongside count and sum', () => {
    const ex = ply().apply(
      'Colors',
      $('diamonds')
        .split('$cut', 'Cut')
        .apply('Count', $('diamonds').count())
        .apply('TotalPrice', $('diamonds').sum('$price'))
        .apply('MostFrequentColor', $('diamonds').mode('$color'))
        .sort('$Count', 'descending')
        .limit(10),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0].length).to.equal(2);

    // Query 1: COUNT + SUM (no mode)
    const q1 = queryPlan[0][0].query;
    expect(q1).to.contain('COUNT(*) AS "Count"');
    expect(q1).to.contain('SUM("price") AS "TotalPrice"');
    expect(q1).to.not.contain('ROW_NUMBER');

    // Query 2: mode
    const q2 = queryPlan[0][1].query;
    expect(q2).to.contain('ROW_NUMBER()');
    expect(q2).to.contain('"color"');
    expect(q2).to.contain('AS "MostFrequentColor"');
  });

  // ============================================================
  // 4. Multiple MODE expressions — one mode query per mode field
  // ============================================================
  it('works with multiple modes in same split', () => {
    const ex = ply().apply(
      'Colors',
      $('diamonds')
        .split('$cut', 'Cut')
        .apply('Count', $('diamonds').count())
        .apply('MostFrequentColor', $('diamonds').mode('$color'))
        .apply('MostFrequentTag', $('diamonds').mode('$tags'))
        .sort('$Count', 'descending')
        .limit(10),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    // 1 normal + 2 mode queries
    expect(queryPlan[0].length).to.equal(3);

    // Query 1: normal aggregates
    const q1 = queryPlan[0][0].query;
    expect(q1).to.contain('COUNT(*) AS "Count"');
    expect(q1).to.not.contain('ROW_NUMBER');

    // Query 2: mode for color
    const q2 = queryPlan[0][1].query;
    expect(q2).to.contain('"color"');
    expect(q2).to.contain('AS "MostFrequentColor"');
    expect(q2).to.contain('ROW_NUMBER()');

    // Query 3: mode for tags
    const q3 = queryPlan[0][2].query;
    expect(q3).to.contain('"tags"');
    expect(q3).to.contain('AS "MostFrequentTag"');
    expect(q3).to.contain('ROW_NUMBER()');
  });

  // ============================================================
  // 5. MODE of a NUMBER field — decomposed
  // ============================================================
  it('works with mode of a number field', () => {
    const ex = ply().apply(
      'Colors',
      $('diamonds')
        .split('$color', 'Color')
        .apply('Count', $('diamonds').count())
        .apply('MostFrequentPrice', $('diamonds').mode('$price'))
        .sort('$Count', 'descending')
        .limit(10),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0].length).to.equal(2);

    const q2 = queryPlan[0][1].query;
    expect(q2).to.contain('"price"');
    expect(q2).to.contain('ROW_NUMBER()');
    expect(q2).to.contain('AS "MostFrequentPrice"');
  });

  // ============================================================
  // 6. MODE with a filter on the mode aggregate — decomposed
  // ============================================================
  it('works with filtered mode', () => {
    const ex = ply().apply(
      'Colors',
      $('diamonds')
        .split('$cut', 'Cut')
        .apply('Count', $('diamonds').count())
        .apply('MostFrequentColor', $('diamonds').filter('$color != "null"').mode('$color'))
        .sort('$Count', 'descending')
        .limit(10),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0].length).to.equal(2);

    const q2 = queryPlan[0][1].query;
    expect(q2).to.contain('ROW_NUMBER()');
    expect(q2).to.contain('"color"');
    // The mode filter should appear in the mode query's WHERE clause
    expect(q2).to.contain('"color" IS NOT NULL');
  });

  // ============================================================
  // 7. MODE as only aggregate in split — still decomposed
  //    Normal query: just split dims (no applies)
  //    Mode query: ROW_NUMBER subquery
  // ============================================================
  it('works with mode as only aggregate in split', () => {
    const ex = ply().apply(
      'Colors',
      $('diamonds')
        .split('$cut', 'Cut')
        .apply('MostFrequentColor', $('diamonds').mode('$color'))
        .sort('$Cut', 'descending')
        .limit(10),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0].length).to.equal(2);

    // Query 1: just split dimensions
    const q1 = queryPlan[0][0].query;
    expect(q1).to.contain('"cut" AS "Cut"');
    expect(q1).to.not.contain('ROW_NUMBER');

    // Query 2: mode
    const q2 = queryPlan[0][1].query;
    expect(q2).to.contain('"color"');
    expect(q2).to.contain('ROW_NUMBER()');
    expect(q2).to.contain('AS "MostFrequentColor"');
  });

  // ============================================================
  // 8. MODE in nested split (subsplit) — decomposed in stage 2
  // ============================================================
  it('works with mode in nested split', () => {
    const ex = ply().apply(
      'Cuts',
      $('diamonds')
        .split('$cut', 'Cut')
        .sort('$Cut', 'descending')
        .limit(10)
        .apply(
          'Colors',
          $('diamonds')
            .split('$color', 'Color')
            .apply('Count', $('diamonds').count())
            .apply('MostFrequentTag', $('diamonds').mode('$tags'))
            .sort('$Count', 'descending')
            .limit(5),
        ),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    // Stage 1: outer split, Stage 2: inner split (decomposed)
    expect(queryPlan.length).to.equal(2);
    // Stage 2 should have 2 queries (normal + mode)
    expect(queryPlan[1].length).to.equal(2);

    const q2 = queryPlan[1][1].query;
    expect(q2).to.contain('"tags"');
    expect(q2).to.contain('ROW_NUMBER()');
    expect(q2).to.contain('AS "MostFrequentTag"');
  });

  // ============================================================
  // 9. MODE with time split — decomposed
  // ============================================================
  it('works with mode in time split', () => {
    const ex = ply().apply(
      'TimeSplit',
      $('diamonds')
        .split($('time').timeBucket('P1D', 'Etc/UTC'), 'TimeDay')
        .apply('Count', $('diamonds').count())
        .apply('MostFrequentColor', $('diamonds').mode('$color'))
        .sort('$TimeDay', 'ascending')
        .limit(10),
    );

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0].length).to.equal(2);

    // Query 1: normal aggregates with TIME_FLOOR
    const q1 = queryPlan[0][0].query;
    expect(q1).to.contain('TIME_FLOOR');
    expect(q1).to.contain('COUNT(*)');
    expect(q1).to.not.contain('ROW_NUMBER');

    // Query 2: mode with TIME_FLOOR partition
    const q2 = queryPlan[0][1].query;
    expect(q2).to.contain('TIME_FLOOR');
    expect(q2).to.contain('"color"');
    expect(q2).to.contain('ROW_NUMBER()');
    expect(q2).to.contain('AS "MostFrequentColor"');
  });

  // ============================================================
  // 10. MODE in total mode alongside other aggregates
  //     → scalar subquery, single query (no decomposition)
  // ============================================================
  it('works with mode in total alongside count', () => {
    const ex = ply()
      .apply('Count', $('diamonds').count())
      .apply('MostFrequentColor', $('diamonds').mode('$color'));

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0].length).to.equal(1);
    const query = queryPlan[0][0].query;
    // Total mode: scalar subquery inline
    expect(query).to.contain('COUNT(*)');
    expect(query).to.contain('"color"');
    expect(query).to.contain('ORDER BY COUNT(*) DESC LIMIT 1');
  });
});
