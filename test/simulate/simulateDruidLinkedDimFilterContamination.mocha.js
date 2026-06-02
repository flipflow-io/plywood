/*
 * Cross-request CONTAMINATION reproduction for the linked-only dimension
 * filter fix.
 *
 * The v1 fix (see simulateDruidLinkedDimFilter.mocha.js) made the linked-only
 * filter clause (`$brand_country.overlap(['Francia'])`) survive `.simplify()`
 * by STASHING it onto the owning linkedSource's config object
 * (`external.linkedSources[lsName].filter`). That object is SHARED by
 * reference: in the server it lives in the settings-manager and the SAME
 * External instance (and the SAME linkedSources config map) is reused across
 * every request for that cube.
 *
 * Consequence observed in the UI local (log /tmp/local-front-3088.log lines
 * 3464/3645/3695/3746/3909): after ONE query with a Francia filter, EVERY
 * subsequent query on the same cube — even one with NO filter — emits the
 * lookup sub-query with `WHERE brand_country = 'Francia'`. The stash leaked
 * across requests because it mutated a long-lived shared object.
 *
 * v2 requirement (Ogievetsky): the linked-only filter must travel WITH THE
 * QUERY — an immutable per-request derivation. The harvest must NEVER write
 * into the config it received. This suite builds ONE External (one shared
 * linkedSources config, exactly as the server does) and runs queries
 * SEQUENTIALLY against the SAME instance, asserting that:
 *   (a) a query WITH a Francia filter → lookup SQL carries WHERE Francia;
 *   (b) a query WITH NO filter → lookup SQL does NOT carry Francia;
 *   (c) a query WITH an Italia filter → lookup SQL carries Italia, NOT Francia;
 *   (+) the source config.filter is byte-identical before/after each eval.
 *
 * Against the v1 code (a) passes but (b) and the immutability asserts FAIL —
 * that is the contamination, confirmed RED before the v2 rewrite.
 */

const { expect } = require('chai');

const plywood = require('../plywood');
const { External, $ } = plywood;

const D01 = 'magic_d01f07da-6a6f-41bc-9bf0-13ddb3bdc422'; // owns brand_country, joinKey brand
const A14 = 'magic_a14b8bf6-4f82-442f-bdc6-c0152c9eaf73'; // sibling, joinKey competitor

// Build ONE External, ONCE, and reuse it across queries — mirrors the
// settings-manager lifecycle where a single cube External (and its single
// linkedSources config map) is shared by every request.
function makeSharedMain() {
  return External.fromValue({
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: '__time',
    allowEternity: true,
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'brand', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
      { name: 'pvp', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      [D01]: {
        source: 'lookup_d01f07da_rev1',
        joinKeys: ['brand'],
        autoInjectJoinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'brand_country', type: 'STRING' },
        ],
      },
      [A14]: {
        source: 'lookup_a14b8bf6_rev1',
        joinKeys: ['competitor'],
        autoInjectJoinKeys: ['competitor'],
        sharedDimensions: ['competitor'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'competitor', type: 'STRING' },
          { name: 'competitor_group', type: 'STRING' },
        ],
      },
    },
  });
}

const TIME = $('__time').overlap({
  start: new Date('2026-05-02T11:50:00.000Z'),
  end: new Date('2026-06-02T11:50:00.000Z'),
});

// A split on the linked-only dimension with a value apply that references it,
// so the linked-only split drives the cross-source decomposition (the shape
// where the lookup sub-query is emitted).
function splitQuery(extraFilter) {
  let filter = TIME;
  if (extraFilter) filter = filter.and(extraFilter);
  return $('main')
    .filter(filter)
    .split({ brand_country: '$brand_country' }, 'main')
    .apply('avg_price', '$main.average($price)');
}

function planSqls(expression, mainExternal) {
  return expression
    .simulateQueryPlan({ main: mainExternal })
    .flat()
    .map(q => (typeof q === 'string' ? q : q && q.query))
    .filter(q => typeof q === 'string');
}

function lookupSubQuery(sqls) {
  return sqls.find(s => s.includes('lookup_d01f07da_rev1'));
}

// Snapshot of the source config.filter values — the thing v1 mutated. We read
// from the live External instance, NOT a copy, so any in-place write the eval
// performs is visible here afterwards.
function configFilterSnapshot(main) {
  const out = {};
  for (const lsName in main.linkedSources) {
    const f = main.linkedSources[lsName].filter;
    out[lsName] = f == null ? null : f.toString();
  }
  return out;
}

describe('Linked-only dimension filter — cross-request contamination (v2 immutability)', () => {
  it('the SAME External instance does not leak a Francia filter across sequential queries', () => {
    const main = makeSharedMain();

    const before = configFilterSnapshot(main);

    // (a) Query WITH Francia filter → lookup SQL must carry Francia.
    const sqlsA = planSqls(splitQuery($('brand_country').overlap(['Francia'])), main);
    const lookupA = lookupSubQuery(sqlsA);
    expect(lookupA, '(a) lookup sub-query exists').to.exist;
    expect(lookupA, '(a) lookup WHERE carries Francia').to.match(/Francia/);

    // Immutability after (a): the source config.filter must be UNCHANGED. v1
    // wrote the harvested Francia clause here, so this is the contamination
    // surface.
    expect(
      configFilterSnapshot(main),
      '(a) source config.filter unchanged after Francia query',
    ).to.deep.equal(before);

    // (b) Query WITH NO extra filter on the SAME instance → lookup SQL must
    // NOT carry Francia. Under v1 the stash from (a) leaked here.
    const sqlsB = planSqls(splitQuery(null), main);
    const lookupB = lookupSubQuery(sqlsB);
    expect(lookupB, '(b) lookup sub-query exists').to.exist;
    expect(lookupB, '(b) no-filter query lookup must NOT carry Francia').to.not.match(/Francia/);

    expect(
      configFilterSnapshot(main),
      '(b) source config.filter unchanged after no-filter query',
    ).to.deep.equal(before);

    // (c) Query WITH Italia filter on the SAME instance → lookup SQL carries
    // Italia and NOT Francia (no leak of the earlier clause, no accumulation).
    const sqlsC = planSqls(splitQuery($('brand_country').overlap(['Italia'])), main);
    const lookupC = lookupSubQuery(sqlsC);
    expect(lookupC, '(c) lookup sub-query exists').to.exist;
    expect(lookupC, '(c) lookup WHERE carries Italia').to.match(/Italia/);
    expect(lookupC, '(c) lookup must NOT carry Francia from earlier query').to.not.match(/Francia/);

    expect(
      configFilterSnapshot(main),
      '(c) source config.filter unchanged after Italia query',
    ).to.deep.equal(before);
  });

  it('direct immutability: the source linkedSources config object identity is preserved', () => {
    const main = makeSharedMain();
    const d01ConfigBefore = main.linkedSources[D01];
    const a14ConfigBefore = main.linkedSources[A14];
    const filterBefore = d01ConfigBefore.filter; // undefined on a fresh config

    planSqls(splitQuery($('brand_country').overlap(['Francia'])), main);

    // The very SAME config objects must still hang off the source External, and
    // their `.filter` slot must not have been written.
    expect(main.linkedSources[D01], 'd01 config identity preserved').to.equal(d01ConfigBefore);
    expect(main.linkedSources[A14], 'a14 config identity preserved').to.equal(a14ConfigBefore);
    expect(main.linkedSources[D01].filter, 'd01 config.filter not mutated').to.equal(filterBefore);
  });
});
