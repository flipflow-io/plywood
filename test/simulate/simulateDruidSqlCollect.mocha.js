const { expect } = require('chai');

const plywood = require('../plywood');

const { Expression, External, Dataset, TimeRange, $, ply, r, s$ } = plywood;

const attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'channel', type: 'STRING' },
  { name: 'page', type: 'STRING' },
  { name: 'user', type: 'STRING' },
  { name: 'added', type: 'NUMBER', unsplitable: true },
];

const context = {
  wiki: External.fromJS({
    engine: 'druidsql',
    version: '0.20.0',
    source: 'wikipedia',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $('time').overlap({
      start: new Date('2015-09-12T00:00:00Z'),
      end: new Date('2015-09-13T00:00:00Z'),
    }),
  }),
};

const timeFilter =
  '(TIMESTAMP \'2015-09-12 00:00:00\'<="time" AND "time"<TIMESTAMP \'2015-09-13 00:00:00\')';

describe('simulate DruidSql COLLECT', () => {
  it('works with collect in total (no split)', () => {
    const ex = ply().apply('AllChannels', $('wiki').collect('$channel'));

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0]).to.have.length(1);
    expect(queryPlan[0][0].query).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT');
    expect(queryPlan[0][0].query).to.contain('"channel"');
  });

  it('decomposes collect in split alongside count', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('Count', '$wiki.count()')
      .apply('AllUsers', '$wiki.collect($user)')
      .sort('$Count', 'descending')
      .limit(5);

    const queryPlan = ex.simulateQueryPlan(context);
    // Decomposed: normal query + collect query
    expect(queryPlan[0]).to.have.length(2);

    const normalQuery = queryPlan[0][0].query;
    expect(normalQuery).to.contain('"page" AS "Page"');
    expect(normalQuery).to.contain('COUNT(*)');
    expect(normalQuery).to.not.contain('ARRAY_AGG');

    const collectQuery = queryPlan[0][1].query;
    expect(collectQuery).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT');
    expect(collectQuery).to.contain('"user"');
  });

  it('decomposes collect of same field as split dimension', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('AllChannels', '$wiki.collect($channel)')
      .limit(3);

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0]).to.have.length(2);

    const collectQuery = queryPlan[0][1].query;
    expect(collectQuery).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT');
    expect(collectQuery).to.contain('"channel"');
  });

  it('decomposes multiple collects into separate queries', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('AllUsers', '$wiki.collect($user)')
      .apply('AllChannels', '$wiki.collect($channel)')
      .limit(5);

    const queryPlan = ex.simulateQueryPlan(context);
    // 3 queries: normal (no applies) + collect user + collect channel
    expect(queryPlan[0]).to.have.length(3);

    const collectQuery1 = queryPlan[0][1].query;
    const collectQuery2 = queryPlan[0][2].query;
    expect(collectQuery1).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT "user")');
    expect(collectQuery2).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT "channel")');
  });

  it('decomposes collect in split mode (split + collect)', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('Count', '$wiki.count()')
      .apply('AllChannels', '$wiki.collect($channel)')
      .sort('$Count', 'descending')
      .limit(5);

    const queryPlan = ex.simulateQueryPlan(context);
    // Should decompose into 2 queries: normal + collect
    expect(queryPlan[0]).to.have.length(2);

    const normalQuery = queryPlan[0][0].query;
    const collectQuery = queryPlan[0][1].query;

    // Normal query should NOT have ARRAY_AGG
    expect(normalQuery).to.not.contain('ARRAY_AGG');
    expect(normalQuery).to.contain('COUNT(*)');
    expect(normalQuery).to.contain('ORDER BY');

    // Collect query should have ARRAY_AGG and GROUP BY split key only
    expect(collectQuery).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT');
    expect(collectQuery).to.contain('"channel"');
    expect(collectQuery).to.contain('GROUP BY');
    expect(collectQuery).to.contain('"page"');
  });

  it('decomposes collect alongside mode in split', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('Count', '$wiki.count()')
      .apply('MostFreqUser', '$wiki.mode($user)')
      .apply('AllChannels', '$wiki.collect($channel)')
      .sort('$Count', 'descending')
      .limit(5);

    const queryPlan = ex.simulateQueryPlan(context);
    // 3 queries: normal + mode + collect
    expect(queryPlan[0]).to.have.length(3);

    const normalQuery = queryPlan[0][0].query;
    const modeQuery = queryPlan[0][1].query;
    const collectQuery = queryPlan[0][2].query;

    expect(normalQuery).to.contain('COUNT(*)');
    expect(normalQuery).to.not.contain('ARRAY_AGG');
    expect(normalQuery).to.not.contain('ROW_NUMBER');

    expect(modeQuery).to.contain('ROW_NUMBER');
    expect(modeQuery).to.contain('"user"');

    expect(collectQuery).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT');
    expect(collectQuery).to.contain('"channel"');
  });
});
