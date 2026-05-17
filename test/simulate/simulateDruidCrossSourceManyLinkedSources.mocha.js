/*
 * Scale pin — N=11 canonical linkedSources, all `timeAlignment:
 * "eternal"`, all with `__time` in their attributes. Mirrors the
 * shape turnilo emits in production once a cube has accumulated
 * ~10 magic dims that all transitioned past MSQ INSERT.
 *
 * Hypothesis under test: when the cube acquires this many peer-
 * expandable linkedSources at once, the main sub-query's time-
 * filter gets lost somewhere in `getCrossExternalDecomposition`
 * or its supporting machinery. With 1-3 linkedSources (existing
 * fixtures) the path is correct; with N=11 a real production
 * cube fails with the Druid error "must filter on time unless
 * the allowEternity flag is set".
 *
 * What this pin asserts:
 *
 *   The main sub-query SQL must contain a `WHERE` clause that
 *   references the time column. Specifically, after we issue
 *   a `.split(magicDim)` query against any one of the N linked
 *   sources, the main external's emitted Druid query must carry
 *   the cube's time bound through to the WHERE.
 *
 * If this test fails, the bug is in plywood. If it passes, the
 * bug is in turnilo's request shape and we re-instrument from
 * the request body.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

const ATTR_IDS = [
  '51c1b49d-b2b9-4477-96ec-c3c6aa4b0a22',
  '47154db0-271d-498c-8f16-517d30f9489c',
  'b182b63f-da6e-400c-8576-c7ec08c592b4',
  '608927c7-2bc3-4937-982e-f733b1d837e8',
  '9cab6003-b6e7-4842-bb20-145974b28214',
  '9bf97581-5155-4330-9d23-b00106f3379b',
  'b84e8162-cc4d-4caa-8a34-40da6e45e2da',
  'f1eb55ca-61a8-43b1-9c0f-5ab685e6e522',
  '721bc4e5-9654-4ddb-9b99-c79be9b6a7fe',
  '871c8866-8edd-4cc4-9051-c33ebdb095e0',
  '33eeef44-a8d2-42f1-9bca-2ed989fee909',
];
const SOURCE_COLUMNS = [
  'competitor',
  'brand',
  'competitor',
  'competitor',
  'brand',
  'brand',
  'brand',
  'productName',
  'productName',
  'productName',
  'productName',
];
const VALUE_NAMES = [
  'revenue_tier',
  'brand_popularity',
  'competitor_type',
  'competitor_size',
  'brand_vintage',
  'brand_region',
  'brand_tier',
  'headphone_form',
  'audio_codec',
  'connectivity_type',
  'battery_class',
];

function makeMain() {
  const linkedSources = {};
  for (let i = 0; i < ATTR_IDS.length; i++) {
    linkedSources[`magic_${ATTR_IDS[i]}`] = {
      source: `lookup_${ATTR_IDS[i]}_rev1`,
      joinKeys: [SOURCE_COLUMNS[i]],
      autoInjectJoinKeys: [SOURCE_COLUMNS[i]],
      sharedDimensions: [SOURCE_COLUMNS[i]],
      joinMode: 'inner',
      timeAlignment: 'eternal',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: SOURCE_COLUMNS[i], type: 'STRING' },
        { name: VALUE_NAMES[i], type: 'STRING' },
        { name: 'confidence', type: 'NUMBER' },
      ],
    };
  }
  return External.fromJS({
    engine: 'druid',
    source: 'histories',
    timeAttribute: '__time',
    allowSelectQueries: true,
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'brand', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'productName', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources,
  });
}

const timeFilter = $('__time').overlap({
  start: new Date('2026-04-26T00:00:00Z'),
  end: new Date('2026-04-27T00:00:00Z'),
});

// Reproduce the production shape: top-level ply() that registers
// `main` and every linkedSource alias with the time filter, then a
// SPLIT apply with an inline-filtered aggregate. This is what
// turnilo's `visualization-query.ts` emits.
function buildQuery(splitAttrIdx) {
  const splitName = VALUE_NAMES[splitAttrIdx];
  let q = ply().apply('main', $('main').filter(timeFilter));
  for (let i = 0; i < ATTR_IDS.length; i++) {
    const lsName = `magic_${ATTR_IDS[i]}`;
    q = q.apply(lsName, $(lsName).filter(timeFilter));
  }
  q = q.apply(
    'S',
    $('main')
      .split({ [splitName]: `$${splitName}` })
      .apply('AvgPrice', $('main').filter(timeFilter).average('$price')),
  );
  return q;
}

describe('Cross-source decomposition with many (N=11) canonical linkedSources', () => {
  it('main sub-query for split on the first linkedSource carries the time filter', () => {
    const ex = buildQuery(0);
    const plan = ex.simulateQueryPlan({ main: makeMain() });
    const queries = plan.flat();
    const mainQ = queries.find(q => q && q.dataSource === 'histories' && q.queryType);
    expect(mainQ, 'main native Druid query was emitted').to.exist;
    const intervals = mainQ.intervals;
    expect(intervals, `main query must declare intervals (got: ${JSON.stringify(intervals)})`).to
      .exist;
    // Intervals can be a string ("start/end") or an array; both forms
    // count as "carrying the time bound".
    const intervalIsBound =
      (typeof intervals === 'string' && intervals.length > 0 && intervals !== '1000/3000') ||
      (Array.isArray(intervals) && intervals.length > 0);
    expect(
      intervalIsBound,
      `main query intervals must reference the cube's time window (got: ${JSON.stringify(
        intervals,
      )})`,
    ).to.equal(true);
  });

  it('main sub-query for split on the LAST linkedSource carries the time filter (the production-failing case)', () => {
    // battery_class — the dim the user reported failing in production.
    const ex = buildQuery(ATTR_IDS.length - 1);
    const plan = ex.simulateQueryPlan({ main: makeMain() });
    const queries = plan.flat();
    const mainQ = queries.find(q => q && q.dataSource === 'histories' && q.queryType);
    expect(mainQ, 'main native Druid query was emitted').to.exist;
    const intervals = mainQ.intervals;
    expect(intervals, `main query must declare intervals (got: ${JSON.stringify(intervals)})`).to
      .exist;
    // Intervals can be a string ("start/end") or an array; both forms
    // count as "carrying the time bound".
    const intervalIsBound =
      (typeof intervals === 'string' && intervals.length > 0 && intervals !== '1000/3000') ||
      (Array.isArray(intervals) && intervals.length > 0);
    expect(
      intervalIsBound,
      `main query intervals must reference the cube's time window (got: ${JSON.stringify(
        intervals,
      )})`,
    ).to.equal(true);
  });

  it('main sub-query simulation does not throw "must filter on time"', () => {
    const ex = buildQuery(ATTR_IDS.length - 1);
    expect(() => ex.simulateQueryPlan({ main: makeMain() })).to.not.throw();
  });
});
