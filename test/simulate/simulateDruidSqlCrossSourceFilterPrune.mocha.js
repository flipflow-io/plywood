/*
 * Filter prune correctness — autonomous Plywood test that replays the
 * JSON shape turnilo's grid emits and pins the engine's behavior. No
 * server, no Druid, no client bundle required — Expression.fromJS +
 * simulateQueryPlan is enough to validate the whole algebra.
 *
 * Scenario: cross-source grid query with
 *   - filter: $__time.overlap(curr∪prev) AND $productName.overlap("X")
 *             AND $reference.overlap("REF1")       (main-only column)
 *   - timeshift: avg_price + _previous_avg_price + _delta (main)
 *                avg_rating + _previous + _delta (linked with .filter(rating>0))
 *   - SPLIT by review_title, review_content, productName + sort + limit
 *
 * What must hold:
 *   1. Query succeeds (no "could not resolve $reference" from linked refcheck)
 *   2. Main side applies the full filter (incl. $reference)
 *   3. Linked side applies the PRUNED filter ($__time + $productName,
 *      dropping $reference which isn't in its schema)
 *   4. The join on synthetic __join_<key> aliases carries main's extra
 *      narrowing over to linked rows.
 *
 * Current engine state (before the fix): the linked side inherits the
 * raw main filter from the ply-level registration, hits reference-check
 * on $reference, and throws. The test PINS this failure mode — when the
 * fix lands, the `expect(() => ...).to.throw(...)` flips to `expect(...).
 * to.have.length.at.least(N)` in commit B.
 */

const { expect } = require('chai');
const plywood = require('../plywood');
const { Expression, External } = plywood;

// Fixture: cube with reviews linkedSource. Attributes are populated as
// they would be after server-side introspection (the shape the server
// actually holds when a real query arrives; the CLIENT-side copy can
// be missing attributes, but this test operates at server-equivalent
// fidelity, which is where the fix lives).
function makeMain() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'partitionId', type: 'STRING' },
      { name: 'productName', type: 'STRING' },
      { name: 'reference', type: 'STRING' }, // main-only
      { name: 'ean', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      reviews: {
        source: 'main_ds-reviews',
        joinKeys: ['partitionId', '__time'],
        // productName lives on both sides (main as raw attr, linked via
        // derivedAttribute) — declared shared so the classifier treats
        // it as a shared split on both sides rather than raising the
        // "ambiguous — declare it" error.
        sharedDimensions: ['productName'],
        joinMode: 'inner',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'partition_id', type: 'STRING' },
          { name: 'title', type: 'STRING' },
          { name: 'content', type: 'STRING' },
          { name: 'rating', type: 'NUMBER', unsplitable: true },
          { name: 'product_name', type: 'STRING' },
        ],
        derivedAttributes: { partitionId: '$partition_id', productName: '$product_name' },
      },
    },
  });
}

// Build the exact JSON shape turnilo's grid make-query emits for this
// scenario. Kept as a separate builder so we can reuse and vary it
// across related specs.
function buildGridRequestExpression() {
  const curr = { start: '2026-04-14T00:00:00.000Z', end: '2026-04-21T00:00:00.000Z' };
  const prev = { start: '2026-04-07T00:00:00.000Z', end: '2026-04-14T00:00:00.000Z' };

  const timeFilter = {
    op: 'overlap',
    operand: { op: 'ref', name: '__time' },
    expression: {
      op: 'literal',
      value: { setType: 'TIME_RANGE', elements: [curr, prev] },
      type: 'SET',
    },
  };
  const productNameFilter = {
    op: 'overlap',
    operand: { op: 'ref', name: 'productName' },
    expression: {
      op: 'literal',
      value: { setType: 'STRING', elements: ['AMASA-20'] },
      type: 'SET',
    },
  };
  const referenceFilter = {
    op: 'overlap',
    operand: { op: 'ref', name: 'reference' },
    expression: {
      op: 'literal',
      value: { setType: 'STRING', elements: ['REF1'] },
      type: 'SET',
    },
  };
  const mainFilter = {
    op: 'and',
    operand: { op: 'and', operand: timeFilter, expression: productNameFilter },
    expression: referenceFilter,
  };
  const overlapTime = r => ({
    op: 'overlap',
    operand: { op: 'ref', name: '__time' },
    expression: {
      op: 'literal',
      value: { setType: 'TIME_RANGE', elements: [r] },
      type: 'SET',
    },
  });
  const filter_ = (operand, expression) => ({ op: 'filter', operand, expression });
  const apply_ = (operand, expression, name) => ({ op: 'apply', operand, expression, name });

  const base = { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' };

  // Top ply: main registration + linked registration (raw filter, unpruned;
  // this is what turnilo currently emits and what the server must handle).
  let e = apply_(base, filter_({ op: 'ref', name: 'main' }, mainFilter), 'main');
  e = apply_(e, filter_({ op: 'ref', name: 'reviews' }, mainFilter), 'reviews');

  // Totals-level measures (grid shows these above the row list)
  e = apply_(
    e,
    {
      op: 'average',
      operand: filter_({ op: 'ref', name: 'main' }, overlapTime(curr)),
      expression: { op: 'ref', name: 'price' },
    },
    'avg_price',
  );
  e = apply_(
    e,
    {
      op: 'average',
      operand: filter_({ op: 'ref', name: 'main' }, overlapTime(prev)),
      expression: { op: 'ref', name: 'price' },
    },
    '_previous__avg_price',
  );
  e = apply_(
    e,
    {
      op: 'subtract',
      operand: { op: 'ref', name: 'avg_price' },
      expression: { op: 'ref', name: '_previous__avg_price' },
    },
    '_delta__avg_price',
  );
  const reviewsRating = r => ({
    op: 'average',
    operand: filter_(filter_({ op: 'ref', name: 'reviews' }, overlapTime(r)), {
      op: 'greaterThan',
      operand: { op: 'ref', name: 'rating' },
      expression: { op: 'literal', value: 0 },
    }),
    expression: { op: 'ref', name: 'rating' },
  });
  e = apply_(e, reviewsRating(curr), 'avg_rating');
  e = apply_(e, reviewsRating(prev), '_previous__avg_rating');
  e = apply_(
    e,
    {
      op: 'subtract',
      operand: { op: 'ref', name: 'avg_rating' },
      expression: { op: 'ref', name: '_previous__avg_rating' },
    },
    '_delta__avg_rating',
  );

  // SPLIT sub-query — per-row data
  let s = {
    op: 'split',
    operand: { op: 'ref', name: 'main' },
    splits: {
      review_title: { op: 'ref', name: 'title' },
      review_content: { op: 'ref', name: 'content' },
      productName: { op: 'ref', name: 'productName' },
    },
    dataName: 'main',
  };
  s = apply_(
    s,
    {
      op: 'average',
      operand: filter_({ op: 'ref', name: 'main' }, overlapTime(curr)),
      expression: { op: 'ref', name: 'price' },
    },
    'avg_price',
  );
  s = apply_(s, reviewsRating(curr), 'avg_rating');
  s = {
    op: 'sort',
    operand: s,
    expression: { op: 'ref', name: 'avg_rating' },
    direction: 'descending',
  };
  s = { op: 'limit', operand: s, value: 50 };
  e = apply_(e, s, 'SPLIT');

  return e;
}

describe('Cross-source filter prune — autonomous plywood-level validation', () => {
  it('auto-prunes the main filter to the linked schema and emits a clean plan', () => {
    // The turnilo-emitted shape routes the raw main filter (with $reference
    // — a main-only column) into .apply("reviews", $reviews.filter(F)).
    // The engine's pre-refcheck rewrite recognises every .filter() applied
    // to a foreign linked-source ref and prunes F to that source's schema
    // (kept: $__time, $productName; dropped: $reference). The cross-source
    // decomposition template inherits an already-pruned filter, so the
    // downstream linked external resolves cleanly.
    const exprJS = buildGridRequestExpression();
    const ex = Expression.fromJS(exprJS);

    const plan = ex.simulateQueryPlan({ main: makeMain() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');
    expect(queries.length, 'at least one main query and one linked query').to.be.at.least(2);

    const mainQueries = queries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'),
    );
    const linkedQueries = queries.filter(q => q.query.includes('main_ds-reviews'));
    expect(mainQueries, 'main side present').to.have.length.at.least(1);
    expect(linkedQueries, 'linked side present').to.have.length.at.least(1);

    // Main keeps the full filter: $reference clause is present somewhere
    const anyMainHasReference = mainQueries.some(q => /"reference"\s*=\s*'REF1'/.test(q.query));
    expect(anyMainHasReference, 'main side retains reference clause').to.equal(true);

    // Linked never sees $reference — pruned out before it reached the linked
    // external. Any linked SQL that mentions it is a prune regression.
    for (const q of linkedQueries) {
      expect(q.query).to.not.match(/"reference"\s*=/);
    }

    // Productname filter: main has productName as a raw column; linked has it
    // as a derivedAttribute → after resolve + SQL lowering, the literal value
    // appears on both sides (main via "productName", linked via "product_name").
    const anyMainHasProduct = mainQueries.some(q => /productName/.test(q.query));
    const anyLinkedHasProduct = linkedQueries.some(q => /product_name|productName/.test(q.query));
    expect(anyMainHasProduct, 'main side keeps productName clause').to.equal(true);
    expect(anyLinkedHasProduct, 'linked side keeps productName clause').to.equal(true);
  });

  it('auto-prunes a linked-only clause off the main filter', () => {
    // Mirror of the scenario above: turnilo also routes the raw main filter
    // into .apply("main", $main.filter(F)). If F contains a clause over a
    // LINKED-only column (e.g. `$content.overlap("hola")`), main's SQL
    // lowering would blow up with "Column 'content' not found". The pre-
    // refcheck rewrite must prune the main side symmetrically: keep clauses
    // whose left operand resolves in main's schema, drop the rest.
    //
    // Keeps: $__time, $productName. Drops: $content (reviews-only).
    const curr = { start: '2026-04-14T00:00:00.000Z', end: '2026-04-21T00:00:00.000Z' };
    const prev = { start: '2026-04-07T00:00:00.000Z', end: '2026-04-14T00:00:00.000Z' };

    const timeFilter = {
      op: 'overlap',
      operand: { op: 'ref', name: '__time' },
      expression: {
        op: 'literal',
        value: { setType: 'TIME_RANGE', elements: [curr, prev] },
        type: 'SET',
      },
    };
    const productNameFilter = {
      op: 'overlap',
      operand: { op: 'ref', name: 'productName' },
      expression: {
        op: 'literal',
        value: { setType: 'STRING', elements: ['AMASA-20'] },
        type: 'SET',
      },
    };
    const contentFilter = {
      op: 'overlap',
      operand: { op: 'ref', name: 'content' },
      expression: {
        op: 'literal',
        value: { setType: 'STRING', elements: ['hola'] },
        type: 'SET',
      },
    };
    const mainFilter = {
      op: 'and',
      operand: { op: 'and', operand: timeFilter, expression: productNameFilter },
      expression: contentFilter,
    };

    const filter_ = (operand, expression) => ({ op: 'filter', operand, expression });
    const apply_ = (operand, expression, name) => ({ op: 'apply', operand, expression, name });
    const base = { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' };

    let e = apply_(base, filter_({ op: 'ref', name: 'main' }, mainFilter), 'main');
    e = apply_(e, filter_({ op: 'ref', name: 'reviews' }, mainFilter), 'reviews');
    e = apply_(
      e,
      {
        op: 'average',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'price' },
      },
      'avg_price',
    );
    e = apply_(
      e,
      {
        op: 'average',
        operand: { op: 'ref', name: 'reviews' },
        expression: { op: 'ref', name: 'rating' },
      },
      'avg_rating',
    );

    const ex = Expression.fromJS(e);
    const plan = ex.simulateQueryPlan({ main: makeMain() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');
    expect(queries.length, 'at least one main and one linked query').to.be.at.least(2);

    const mainQueries = queries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'),
    );
    const linkedQueries = queries.filter(q => q.query.includes('main_ds-reviews'));
    expect(mainQueries, 'main side present').to.have.length.at.least(1);
    expect(linkedQueries, 'linked side present').to.have.length.at.least(1);

    // Main never sees $content — pruned out before refcheck. Any main SQL
    // that references it is a prune regression (and in live Druid would
    // surface as a 500 "Column 'content' not found in any table").
    for (const q of mainQueries) {
      expect(q.query, 'main SQL must not reference linked-only $content').to.not.match(
        /"content"\s*=|'hola'/,
      );
    }

    // Linked keeps its own column resolution — $content here is the raw
    // linked attribute, not pruned from the linked side.
    const anyLinkedHasContent = linkedQueries.some(q => /'hola'/.test(q.query));
    expect(anyLinkedHasContent, 'linked side keeps its own content clause').to.equal(true);
  });
});
