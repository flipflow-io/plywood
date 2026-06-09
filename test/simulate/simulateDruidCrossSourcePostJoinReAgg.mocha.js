/*
 * Post-join re-aggregation to the user's split grain (INV-1).
 *
 * Reproduces the production bug (Pernod cube, magic dim `brand_country`,
 * lookup ready): a linked-only split (`brand_country`) + a main-side measure
 * with joinMode `inner`. The JS-join pre-aggregates main at the JOIN-KEY grain
 * (one row per `brand`) and the linked side maps brand → country. The join
 * fans those brand rows out: a country with N brands appears in N rows. The
 * user asked for the COUNTRY grain, so those rows MUST collapse, combining
 * each measure by its decomposability trait.
 *
 * Before the re-agg the result had row count > distinct split tuples and
 * `assertDatasetShape` fired `PlywoodCardinalityViolation` (fail-loud, no bad
 * data). After the re-agg the result is correct: one row per country with the
 * measure recombined (count/sum summed, min minimised, max maximised).
 *
 * Counterfactual: remove the `reAggregateToSplitGrain` call in
 * queryBasicValueStream → the join's fan-out reaches assertDatasetShape and
 * every test here throws PlywoodCardinalityViolation.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $ } = plywood;

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

// Five brands → three countries. Reino Unido has three brands (fan-out 3).
const BRAND_COUNTRY = [
  { __join_brand: 'Beefeater', brand_country: 'Reino Unido' },
  { __join_brand: 'Plymouth', brand_country: 'Reino Unido' },
  { __join_brand: 'Ballantines', brand_country: 'Reino Unido' },
  { __join_brand: 'Absolut', brand_country: 'Suecia' },
  { __join_brand: 'Havana', brand_country: 'Cuba' },
];

function makeMain(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'histories',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
      ],
      linkedSources: {
        magic_bc: {
          source: 'lookup_bc_rev1',
          joinKeys: ['brand'],
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

// Build a requester that returns the lookup mapping for the linked query and
// `mainRows` (per-brand pre-aggregated measure) for the main query.
function makeRequester(mainRows) {
  return promiseFnToStream(rq => {
    const sql = (rq && rq.query && rq.query.query) || '';
    if (sql.includes('"lookup_bc_rev1"')) return Promise.resolve(BRAND_COUNTRY);
    if (sql.includes('"histories"')) return Promise.resolve(mainRows);
    return Promise.resolve([]);
  });
}

function rowsByCountry(result) {
  const js = result && result.toJS ? result.toJS() : result;
  const data = (js && js.data) || [];
  const map = {};
  for (const r of data) map[r.brand_country] = r;
  return { count: data.length, map };
}

describe('Cross-source post-join re-aggregation to split grain (INV-1)', () => {
  it('count + linked-only split + inner → counts SUMMED per country (5 brands → 3 rows)', async () => {
    const mainRows = [
      { __join_brand: 'Beefeater', Count: 3 },
      { __join_brand: 'Plymouth', Count: 2 },
      { __join_brand: 'Ballantines', Count: 7 },
      { __join_brand: 'Absolut', Count: 4 },
      { __join_brand: 'Havana', Count: 5 },
    ];
    const main = makeMain(makeRequester(mainRows));
    const ex = $('main').split('$brand_country', 'brand_country').apply('Count', '$main.count()');
    const result = await ex.compute({ main });
    const { count, map } = rowsByCountry(result);
    expect(count, 'collapsed to one row per country').to.equal(3);
    expect(map['Reino Unido'].Count, 'RU = 3+2+7').to.equal(12);
    expect(map['Suecia'].Count).to.equal(4);
    expect(map['Cuba'].Count).to.equal(5);
  });

  it('sum + linked-only split + inner → sums SUMMED per country', async () => {
    const mainRows = [
      { __join_brand: 'Beefeater', Revenue: 100 },
      { __join_brand: 'Plymouth', Revenue: 50 },
      { __join_brand: 'Ballantines', Revenue: 25 },
      { __join_brand: 'Absolut', Revenue: 80 },
      { __join_brand: 'Havana', Revenue: 60 },
    ];
    const main = makeMain(makeRequester(mainRows));
    const ex = $('main')
      .split('$brand_country', 'brand_country')
      .apply('Revenue', '$main.sum($price)');
    const result = await ex.compute({ main });
    const { count, map } = rowsByCountry(result);
    expect(count).to.equal(3);
    expect(map['Reino Unido'].Revenue, 'RU = 100+50+25').to.equal(175);
    expect(map['Suecia'].Revenue).to.equal(80);
    expect(map['Cuba'].Revenue).to.equal(60);
  });

  // Unit-level coverage of the min/max reducers in reAggregateToSplitGrain.
  // NB: at the gate level min/max currently route to the native-JOIN path
  // (`isMeasureDecomposable` returns false for them — cycle-2's conservative
  // R-4-pending stance), so they do NOT flow through the JS-join re-agg in a
  // full compute(). The reducer is nonetheless implemented and exercised
  // directly here so it is ready when the gate enables min/max for JS-join.
  it('reAggregateToSplitGrain combines min/max reducers correctly', () => {
    const { Dataset } = plywood;
    const fan = Dataset.fromJS({
      keys: ['__join_brand'],
      data: [
        { __join_brand: 'Beefeater', MinP: 30, MaxP: 30 },
        { __join_brand: 'Plymouth', MinP: 12, MaxP: 12 },
        { __join_brand: 'Ballantines', MinP: 45, MaxP: 45 },
      ],
    }).join(
      Dataset.fromJS({
        keys: ['__join_brand'],
        data: BRAND_COUNTRY.filter(b => b.brand_country === 'Reino Unido'),
      }),
      'inner',
    );
    const out = External.reAggregateToSplitGrain(fan, ['brand_country'], {
      MinP: 'min',
      MaxP: 'max',
    });
    expect(out.data.length, 'collapsed to one country').to.equal(1);
    expect(out.data[0].MinP, 'min(30,12,45)').to.equal(12);
    expect(out.data[0].MaxP, 'max(30,12,45)').to.equal(45);
  });

  it("reAggregateToSplitGrain skips (no-op) when a measure is non-recombinable ('none')", () => {
    // An avg-projected ratio column cannot be summed across partitions. The
    // reducer must NOT touch the dataset — leaving the fan-out for the INV-1
    // net (assertDatasetShape) to reject loud rather than silently collapse
    // with a wrong average.
    const { Dataset } = plywood;
    const fan = Dataset.fromJS({
      keys: ['brand_country'],
      data: [
        { brand_country: 'Reino Unido', Avg: 30 },
        { brand_country: 'Reino Unido', Avg: 12 },
      ],
    });
    const out = External.reAggregateToSplitGrain(fan, ['brand_country'], { Avg: 'none' });
    expect(out.data.length, 'left untouched (2 fan-out rows)').to.equal(2);
    expect(() => External.assertDatasetShape(out), 'INV-1 net still fires loud').to.throw(
      plywood.PlywoodCardinalityViolation,
    );
  });

  it('cross-engine (Postgres staging linked) + count → same re-agg applies', async () => {
    // The staging linkedSource is Postgres; native-JOIN is impossible across
    // stores, so JS-join is the ONLY path — making post-join re-agg mandatory.
    const mainRows = [
      { __join_brand: 'Beefeater', Count: 3 },
      { __join_brand: 'Plymouth', Count: 2 },
      { __join_brand: 'Ballantines', Count: 7 },
      { __join_brand: 'Absolut', Count: 4 },
      { __join_brand: 'Havana', Count: 5 },
    ];
    // Route the main (Druid) query to the Druid requester and the staging
    // (Postgres) query to the staging requester; assert the staging one is
    // the side that returns the brand→country mapping.
    let stagingHit = false;
    const druidReq = makeRequester(mainRows); // serves main "histories"
    const stagingReq = promiseFnToStream(rq => {
      stagingHit = true;
      return Promise.resolve(BRAND_COUNTRY);
    });
    const main = External.fromJS(
      {
        engine: 'druidsql',
        source: 'histories',
        timeAttribute: '__time',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
        ],
        linkedSources: {
          magic_staging: {
            source: 'magic_staging_bc_rev1',
            joinKeys: ['brand'],
            sharedDimensions: ['brand'],
            joinMode: 'inner',
            timeAlignment: 'eternal',
            engine: 'postgres',
            attributes: [
              { name: 'brand', type: 'STRING' },
              { name: 'brand_country', type: 'STRING' },
            ],
          },
        },
        filter: timeFilter,
      },
      druidReq,
    );
    main.linkedSources.magic_staging.requester = stagingReq;

    const ex = $('main').split('$brand_country', 'brand_country').apply('Count', '$main.count()');
    const result = await ex.compute({ main });
    const { count, map } = rowsByCountry(result);
    expect(stagingHit, 'staging Postgres requester was used for the linked side').to.equal(true);
    expect(count).to.equal(3);
    expect(map['Reino Unido'].Count, 'RU = 3+2+7').to.equal(12);
  });
});
