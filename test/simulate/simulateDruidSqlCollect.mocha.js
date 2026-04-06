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

  it('works with collect in split alongside count', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('Count', '$wiki.count()')
      .apply('AllUsers', '$wiki.collect($user)')
      .sort('$Count', 'descending')
      .limit(5);

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0]).to.have.length(1);

    const query = queryPlan[0][0].query;
    expect(query).to.contain('"page" AS "Page"');
    expect(query).to.contain('COUNT(*)');
    expect(query).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT');
    expect(query).to.contain('"user"');
    expect(query).to.contain('ORDER BY "Count" DESC');
    expect(query).to.contain('LIMIT 5');
  });

  it('works with collect of same field as split dimension', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('AllChannels', '$wiki.collect($channel)')
      .limit(3);

    const queryPlan = ex.simulateQueryPlan(context);
    const query = queryPlan[0][0].query;
    expect(query).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT');
    expect(query).to.contain('"channel"');
    expect(query).to.contain('GROUP BY 1');
  });

  it('works with multiple collects in same split', () => {
    const ex = $('wiki')
      .split('$page', 'Page')
      .apply('AllUsers', '$wiki.collect($user)')
      .apply('AllChannels', '$wiki.collect($channel)')
      .limit(5);

    const queryPlan = ex.simulateQueryPlan(context);
    const query = queryPlan[0][0].query;

    // Both collects should be in the same query (inline, no decomposition)
    expect(query).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT "user")');
    expect(query).to.contain('ARRAY_TO_STRING(ARRAY_AGG(DISTINCT "channel")');
  });
});
