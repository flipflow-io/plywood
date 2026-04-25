/*
 * Main-side schema completeness w.r.t. timeAttribute — pin for a bug
 * that masquerades as "magic dim recently created → split → 500" in
 * downstream cubes.
 *
 * The shape that production introspect returns is asymmetric: a Druid
 * SQL cube declares its time column ONLY as `timeAttribute`, never as
 * a member of `attributes`. The cube knows main can resolve `time`;
 * it just doesn't list it twice. Plywood's filter-pruning machinery
 * (pruneLinkedFilterRefsInTree) builds the main side's resolvable
 * schema as `rawAttributes ∪ derivedAttributes` and asks
 * `pruneFilterToSchema` to drop any clause whose refs land outside.
 * If the cube's filter is `$time.overlap(...)` and `time` isn't in
 * the schema set, the time clause rewrites to TRUE — and the main
 * sub-query goes to Druid carrying no time bound, with `allowEternity:
 * false`, and Druid (correctly) rejects it:
 *   "must filter on time unless the allowEternity flag is set"
 *
 * The bug only fires when `linkedSources` is non-empty: without them,
 * pruneLinkedFilterRefsInTree short-circuits and the filter is never
 * touched. So in production it appears the moment a cube acquires its
 * first magic dim. The user reported it as "split immediately after
 * create → 500".
 *
 * What this test pins:
 *   - main side: split by linked alias + filter on time → main query
 *     in the simulated plan carries the time bound (visible as a
 *     `'time' >= ...` literal in the SQL string).
 *   - linked side: time clause is correctly stripped from the linked
 *     sub-query (the linked schema doesn't list time, so prune drops
 *     it). This stays correct after the fix.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

// Match the request shape turnilo's visualization-query.ts +
// applySeries emit in production: a top-level ply() with one
// .apply("main", $main.filter(F)) per source binding for the totals,
// and a SPLIT apply whose aggregates carry the filter INLINE on
// `$main.filter(F).<aggregate>($column)` (this is what
// ConcreteSeries.plywoodExpression generates per measure). The inline
// filter is what plywood's `_addFilterExpression` actually absorbs into
// the External; the top ply() apply only registers the alias and is
// not the path that propagates the time bound to Druid.
function buildTurniloShape() {
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_headphone_form', $('magic_headphone_form').filter(timeFilter))
    .apply(
      'S',
      $('main')
        .split({ form: '$headphone_form' })
        .apply('AvgPrice', $('main').filter(timeFilter).average('$price')),
    );
}

function makeMainTimeAttributeOnly() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: 'time', // ← declared HERE only
    attributes: [
      // 'time' is intentionally NOT listed — this mirrors what Druid SQL
      // introspect emits for a real cube. The bug lives in the gap.
      { name: 'brand', type: 'STRING' },
      { name: 'productName', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      magic_headphone_form: {
        // staging-fresh shape: PostgresExternal-backed view, joins on
        // productName, classifies into a small enum. timeAlignment
        // omitted — the staging view has no __time column. That part
        // is correct; the bug is on the OTHER side (main).
        source: 'magic_staging_headphone_form_rev1',
        joinKeys: ['productName'],
        autoInjectJoinKeys: ['productName'],
        sharedDimensions: ['productName'],
        joinMode: 'inner',
        attributes: [
          { name: 'productName', type: 'STRING' },
          { name: 'headphone_form', type: 'STRING' },
        ],
      },
    },
  });
}

const timeFilter = $('time').overlap({
  start: new Date('2026-04-24T00:00:00Z'),
  end: new Date('2026-04-25T00:00:00Z'),
});

describe('Main timeAttribute schema completeness — split on linked alias keeps main time-filter', () => {
  it('main sub-query carries the time bound when timeAttribute is declared only via timeAttribute', () => {
    const ex = buildTurniloShape();

    const plan = ex.simulateQueryPlan({ main: makeMainTimeAttributeOnly() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQ = queries.find(q => q.query.includes('"histories"'));
    expect(mainQ, 'main query was emitted').to.exist;

    // The bug: with timeAttribute missing from the main schema set,
    // pruneFilterToSchema rewrites the time clause to TRUE and the
    // main SQL goes out with no temporal bound. The fix puts
    // timeAttribute into the schema set, the prune preserves the
    // clause, and the SQL ends up with a `"time"` reference.
    expect(
      mainQ.query.includes('"time"'),
      `main query must mention the time column. SQL was:\n${mainQ.query}`,
    ).to.equal(true);
  });

  it('linked sub-query stays time-free (the staging view has no time)', () => {
    const ex = buildTurniloShape();

    const plan = ex.simulateQueryPlan({ main: makeMainTimeAttributeOnly() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const linkedQ = queries.find(q => q.query.includes('magic_staging_headphone_form_rev1'));
    expect(linkedQ, 'linked query was emitted').to.exist;
    // The linked staging schema has no time column — its SQL must not
    // reference one. Pruning to the linked schema turns the time
    // clause to TRUE on this side; that part of the algebra was
    // already correct and should stay correct after the fix.
    expect(
      linkedQ.query.includes('"time"'),
      `linked query must not mention a time column. SQL was:\n${linkedQ.query}`,
    ).to.equal(false);
  });

  it('main query simulation does not throw "must filter on time"', () => {
    const ex = buildTurniloShape();

    expect(() => ex.simulateQueryPlan({ main: makeMainTimeAttributeOnly() })).to.not.throw();
  });

  it('multi-magic case (the original bug) — two staging linkedSources both keep main time-filter', () => {
    // Add a second magic dim on a different column. Both staging,
    // both omit timeAlignment. Cross-source decomposition must still
    // produce a main query with the time filter intact.
    function makeMainTwoMagic() {
      return External.fromJS({
        engine: 'druidsql',
        source: 'histories',
        timeAttribute: 'time',
        attributes: [
          { name: 'brand', type: 'STRING' },
          { name: 'productName', type: 'STRING' },
          { name: 'competitor', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
        ],
        linkedSources: {
          magic_headphone_form: {
            source: 'magic_staging_headphone_form_rev1',
            joinKeys: ['productName'],
            autoInjectJoinKeys: ['productName'],
            sharedDimensions: ['productName'],
            joinMode: 'inner',
            attributes: [
              { name: 'productName', type: 'STRING' },
              { name: 'headphone_form', type: 'STRING' },
            ],
          },
          magic_competitor_size: {
            source: 'magic_staging_competitor_size_rev1',
            joinKeys: ['competitor'],
            autoInjectJoinKeys: ['competitor'],
            sharedDimensions: ['competitor'],
            joinMode: 'inner',
            attributes: [
              { name: 'competitor', type: 'STRING' },
              { name: 'competitor_size', type: 'STRING' },
            ],
          },
        },
      });
    }

    const ex = ply()
      .apply('main', $('main').filter(timeFilter))
      .apply('magic_headphone_form', $('magic_headphone_form').filter(timeFilter))
      .apply('magic_competitor_size', $('magic_competitor_size').filter(timeFilter))
      .apply(
        'S',
        $('main')
          .split({ form: '$headphone_form', size: '$competitor_size' })
          .apply('AvgPrice', $('main').filter(timeFilter).average('$price')),
      );

    const plan = ex.simulateQueryPlan({ main: makeMainTwoMagic() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQ = queries.find(
      q => q.query.includes('"histories"') && !q.query.includes('magic_staging'),
    );
    expect(mainQ, 'main query was emitted').to.exist;
    expect(
      mainQ.query.includes('"time"'),
      `main query must mention the time column. SQL was:\n${mainQ.query}`,
    ).to.equal(true);
  });
});
