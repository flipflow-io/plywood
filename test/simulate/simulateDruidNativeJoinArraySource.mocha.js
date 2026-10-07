/*
 * A main External whose source is a list (a datasource and its history, read
 * as one table through TABLE(APPEND(...))) joined natively to a magic lookup.
 *
 * Why (7 Oct 2026, Flipflow Devic HITL W8c, 7-29): getNativeJoinDecomposition
 * wrote `FROM ${escapeName(String(source))}`, so the list became one quoted
 * identifier "a,b" and Druid refused it («Length of identifier … must be less
 * than or equal to 128 characters» on the Dermofarm cube). Every magic
 * dimension filter or split on a list-sourced cube failed.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

const timeFilter = $('__time').overlap({
  start: new Date('2026-09-01T00:00:00Z'),
  end: new Date('2026-10-01T00:00:00Z'),
});

function makeMain(source) {
  return External.fromJS({
    engine: 'druidsql',
    source,
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'product_id', type: 'STRING' },
    ],
    linkedSources: {
      magic_tt: {
        source: 'lookup_tt_rev1',
        joinKeys: ['competitor'],
        autoInjectJoinKeys: ['competitor'],
        sharedDimensions: ['competitor'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: 'competitor', type: 'STRING' },
          { name: 'tipo_tienda', type: 'STRING' },
        ],
      },
    },
    filter: timeFilter,
  });
}

function planSql(source) {
  const split = $('main')
    .split('$tipo_tienda', 'tipo_tienda')
    .apply('uniq', '$main.countDistinct($product_id)')
    .sort('$uniq', 'descending')
    .limit(50);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_tt', $('magic_tt').filter(timeFilter))
    .apply('SPLIT', split)
    .simulateQueryPlan({ main: makeMain(source) })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('native JOIN on a main whose source is a list', () => {
  it('reads the list through TABLE(APPEND(...)), as the plain Druid SQL path does', () => {
    const sqls = planSql(['histories-x', 'histories-x-history']);
    expect(sqls.length, 'one native-JOIN SQL').to.equal(1);
    expect(sqls[0]).to.include(`FROM TABLE(APPEND('histories-x', 'histories-x-history')) AS main`);
    expect(sqls[0]).to.include('INNER JOIN "lookup_tt_rev1" AS lookup');
    expect(sqls[0], 'never one quoted identifier made of the list').to.not.include(
      '"histories-x,histories-x-history"',
    );
  });

  it('a single source is still one quoted table', () => {
    const sqls = planSql('histories-x');
    expect(sqls[0]).to.include('FROM "histories-x" AS main');
  });
});
