/*
 * Mixed-engine cross-source pin — main Druid SQL `histories` + TWO
 * linkedSources:
 *
 *   - lookup_canon  (canonical): a materialised lookup co-located with the
 *     main Druid datasource. Declares NO engine override → inherits the
 *     main external's engine/version/requester. This is the steady-state
 *     "magic dim is ready" shape.
 *
 *   - magic_staging (staging): a Postgres view (magic_staging_<attrId>_revN)
 *     written during the `materializing` phase, before the lookup lands in
 *     Druid. Declares `engine: "postgres"` + a runtime requester sentinel →
 *     its sub-query MUST be routed to the Postgres engine, NOT the Druid
 *     requester. Turnilo stamps this binding so a split on the staging
 *     value reads live values from Postgres while the Druid lookup is still
 *     materialising.
 *
 * The property under test (the cross-engine override re-applied on top of
 * the cycle-2 decomposition rewrite):
 *
 *   1. getCrossExternalDecomposition produces a JS-join (each side runs its
 *      own query) — cross-engine cannot be a single SQL JOIN.
 *   2. The staging linkedExternal is built with engine="postgres" and the
 *      injected requester sentinel — it is NOT sent to the Druid requester.
 *   3. The canonical linkedExternal (no override) still inherits the main
 *      engine ("druidsql") — counterfactual: omitting the override preserves
 *      the previous same-store behaviour.
 *   4. FAIL-LOUD: declaring engine="postgres" with NO requester throws,
 *      rather than silently inheriting the main (Druid) requester and
 *      dispatching the Postgres SQL to the wrong store.
 *   5. The native-JOIN path refuses the cross-engine shape loudly
 *      (PlywoodUnsupportedNativeJoinShape) instead of emitting a single SQL
 *      JOIN that spans two engines.
 *
 * Counterfactual: revert resolveLinkedEngineBinding to `engine: this.engine`
 * and assertions #2 and #4 fail (staging inherits druidsql; no throw).
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, Expression } = plywood;

const STAGING_ATTR_ID = '9999dead-beef-4000-aaaa-cccccccccccc';
const STAGING_SOURCE = `magic_staging_${STAGING_ATTR_ID.replace(/-/g, '_')}_rev1`;

const filterStart = new Date('2026-05-01T00:00:00Z');
const filterEnd = new Date('2026-05-02T00:00:00Z');
const timeFilter = $('__time').overlap({ start: filterStart, end: filterEnd });

// A named function so the test can identify the requester by its `.name`
// (proving the staging external got THIS requester, not the main one).
function pgStagingRequester() {
  throw new Error('pgStagingRequester invoked — simulate path should never call it');
}

/**
 * @param {Object} opts
 * @param {boolean} opts.injectStagingRequester  inject the runtime requester
 *        on the staging linkedSource value object (turnilo does this).
 */
function makeMainMixed(opts = {}) {
  const ext = External.fromJS({
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'brand', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      // Canonical lookup — no engine override → inherits main (druidsql).
      lookup_canon: {
        source: 'lookup_canon_rev1',
        joinKeys: ['brand'],
        autoInjectJoinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: 'brand', type: 'STRING' },
          { name: 'brand_tier', type: 'STRING' },
        ],
      },
      // Staging view — Postgres engine override. `engine` round-trips
      // through fromJS; `requester` is injected at runtime below.
      magic_staging: {
        source: STAGING_SOURCE,
        joinKeys: ['brand'],
        autoInjectJoinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: 'left',
        timeAlignment: 'eternal',
        engine: 'postgres',
        attributes: [
          { name: 'brand', type: 'STRING' },
          { name: 'rating_band', type: 'STRING' },
        ],
      },
    },
    filter: timeFilter,
  });
  if (opts.injectStagingRequester) {
    ext.linkedSources.magic_staging.requester = pgStagingRequester;
  }
  return ext;
}

// avg($price) so the measure DECOMPOSES (sum/count) and routes through the
// JS-join path — the path that supports cross-engine.
const avgPriceApply = Expression.fromJS({
  op: 'apply',
  operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
  expression: {
    op: 'average',
    operand: { op: 'ref', name: 'main' },
    expression: { op: 'ref', name: 'price' },
  },
  name: 'AvgPrice',
});

function buildSplitExt(main, splitRef) {
  const withSplit = main.addExpression(
    Expression.fromJS({ op: 'ref', name: 'main' }).split(splitRef, 'Bucket'),
  );
  return withSplit.addExpression(avgPriceApply);
}

describe('Cross-source mixed-engine — canonical Druid lookup + Postgres staging view', () => {
  it('staging linkedSource routes to the Postgres engine with the injected requester (NOT Druid)', () => {
    const main = makeMainMixed({ injectStagingRequester: true });
    const splitExt = buildSplitExt(main, '$rating_band');
    expect(splitExt.mode, 'mode is split').to.equal('split');

    const crossExt = splitExt.getCrossExternalDecomposition();
    // (1) JS-join, not a single cross-engine SQL JOIN.
    expect(crossExt, 'gate must produce a decomposition').to.not.be.null;
    expect(crossExt.kind, 'cross-engine must NOT use nativeJoin').to.not.equal('nativeJoin');
    expect(crossExt.linkedExternals, 'JS-join populates linkedExternals').to.exist;

    const staging = (crossExt.linkedExternals || []).find(le => le.name === 'magic_staging');
    expect(staging, 'magic_staging linkedExternal present').to.exist;
    // (2) The staging sub-query is bound to Postgres + the injected requester.
    expect(staging.external.engine, 'staging external engine is postgres').to.equal('postgres');
    expect(staging.external.source, 'staging external targets the Postgres view').to.equal(
      STAGING_SOURCE,
    );
    expect(
      staging.external.requester && staging.external.requester.name,
      'staging external carries the injected Postgres requester, not the main Druid one',
    ).to.equal('pgStagingRequester');
    // Main itself stays Druid SQL.
    expect(crossExt.mainExternal.engine, 'main external stays druidsql').to.equal('druidsql');
  });

  it('canonical linkedSource (no override) inherits the main engine — counterfactual for the override', () => {
    const main = makeMainMixed({ injectStagingRequester: true });
    const splitExt = buildSplitExt(main, '$brand_tier');
    const crossExt = splitExt.getCrossExternalDecomposition();
    expect(crossExt).to.not.be.null;

    const canon = (crossExt.linkedExternals || []).find(le => le.name === 'lookup_canon');
    expect(canon, 'lookup_canon linkedExternal present').to.exist;
    // No engine override → inherits main's druidsql engine + requester.
    expect(canon.external.engine, 'canonical lookup inherits druidsql').to.equal('druidsql');
    expect(canon.external.source, 'canonical lookup targets its Druid lookup table').to.equal(
      'lookup_canon_rev1',
    );
  });

  it('FAIL-LOUD: engine override declared without a requester throws (never inherits the wrong store)', () => {
    const main = makeMainMixed({ injectStagingRequester: false });
    const splitExt = buildSplitExt(main, '$rating_band');
    expect(() => splitExt.getCrossExternalDecomposition())
      .to.throw(Error)
      .with.property('message')
      .that.match(/cross-engine override engine="postgres"/);
  });

  it('native-JOIN path refuses the cross-engine shape loudly (single SQL JOIN cannot span engines)', () => {
    // A single native SQL JOIN references both the main source and the
    // linked source in ONE statement — impossible when the linkedSource
    // lives in a different engine. getNativeJoinDecomposition must throw
    // PlywoodUnsupportedNativeJoinShape rather than emit SQL that names a
    // Postgres view the Druid engine cannot see. (The decomposability gate
    // upstream already prefers the JS-join for these shapes; this guard is
    // the fail-loud backstop if the native path is ever reached directly.)
    const main = makeMainMixed({ injectStagingRequester: true });
    const withSplit = main.addExpression(
      Expression.fromJS({ op: 'ref', name: 'main' }).split('$rating_band', 'rating_band'),
    );
    const countDistinctApply = Expression.fromJS({
      op: 'apply',
      operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
      expression: {
        op: 'countDistinct',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'brand' },
      },
      name: 'uniqueBrands',
    });
    const splitExt = withSplit.addExpression(countDistinctApply);
    expect(() => splitExt.getNativeJoinDecomposition(['rating_band'], [], { magic_staging: true }))
      .to.throw(plywood.PlywoodUnsupportedNativeJoinShape)
      .with.property('message')
      .that.match(/cannot span two engines/);
  });
});
