/*
 * RED spec — linked-only split + main-side measure must re-aggregate post-join.
 *
 * User flow (Turnilo magic-attribute dimension):
 *   - Main cube `histories-<team>` has rows at (productName, userIdProduct,
 *     __time) granularity.
 *   - A magic-attribute lookup `lookup_<attrId>_rev1` maps each productName
 *     to a single canonical bucket (`uso_tipico` ∈ {Industrial, …}). It is
 *     declared as a linkedSource on the main cube with
 *       joinKeys:           ['productName'],
 *       autoInjectJoinKeys: ['productName'],
 *       sharedDimensions:   ['productName'],
 *       joinMode:           'inner',
 *       timeAlignment:      'eternal',
 *       backing:            { phase: 'canonical' }.
 *   - The user builds a Turnilo bar chart split by `uso_tipico` with a
 *     single main measure: `unique_products = countDistinct(userIdProduct)`.
 *
 * The expression that reaches plywood (verbatim shape from the live client):
 *
 *   ply()
 *     .apply('main',              $main.filter($__time.overlap([T0,T1])))
 *     .apply('lookup_uso_tipico', $lookup_uso_tipico.filter(<same filter>))
 *     .apply('unique_products',   $main.countDistinct($userIdProduct))
 *     .apply('SPLIT',
 *        $main
 *          .split($uso_tipico, 'uso_tipico')
 *          .apply('unique_products', $main.countDistinct($userIdProduct))
 *          .sort($uso_tipico, 'descending')
 *          .limit(5000))
 *
 * `uso_tipico` lives only in the lookup's attributes. `userIdProduct` lives
 * only in main. The user's SPLIT step is, semantically, a GROUP BY uso_tipico
 * over the joined dataset, summing countDistinct contributions per bucket.
 *
 * What plywood 0.49.0 emits today (verified with simulateQueryPlan against
 * this exact fixture before writing the assertions below):
 *
 *   main:    SELECT productName AS __join_productName,
 *                   COUNT(DISTINCT userIdProduct) AS unique_products
 *            FROM main_ds GROUP BY 1
 *   lookup:  SELECT productName AS __join_productName,
 *                   uso_tipico
 *            FROM lookup GROUP BY 1, 2
 *
 * Auto-inject promotes the cube's declared `productName` joinKey to a
 * synthetic split `__join_productName` on both sides. Main never groups by
 * `uso_tipico` (it can't — the column is linked-only) and the post-join
 * pipeline runs only `dropColumns(__join_productName) → sort → limit`. No
 * GROUP BY uso_tipico is ever applied to the joined dataset, so the user
 * gets one row per (productName, uso_tipico) tuple instead of one row per
 * bucket. Worse, countDistinct(userIdProduct) is not summable post-hoc —
 * even if a future fix routes the aggregation correctly, the engine must
 * recognise that this measure can't be losslessly re-aggregated from a
 * productName-level decomposition.
 *
 * Failure mode in the UI: a bar chart split by `uso_tipico` renders ~930
 * bars (one per product, labelled with the product's bucket), top 25
 * alphabetic-descending all happen to read 'Industrial' (the largest bucket
 * by member count). The user sees "everything is Industrial" instead of
 * "4 buckets with their relative sizes".
 *
 * What this spec pins (the invariant the engine MUST satisfy):
 *
 *   The compute() result's SPLIT.data contains exactly one row per distinct
 *   `uso_tipico` value in the joined dataset. Cardinality is the dimension
 *   contract; that's the user-observable invariant.
 *
 * The fixture is deliberately minimal — 3 products mapping to 2 buckets —
 * so the post-join row count is unambiguously distinguishable from the
 * pre-aggregation row count (3 vs 2). Counterfactual: if plywood did the
 * right thing today, the result would be 2 rows; if it returns 3 rows the
 * bug is present.
 *
 * Commits that landed in 0.49.0 and were expected to cover this case but
 * don't (the schema-only postJoinSort/postJoinLimit handling of the
 * existing fixes deals with row-identity routing but not with the missing
 * post-join GROUP BY itself):
 *   - 6ebe560 feat: decompose on linked-only split without foreign apply
 *   - 9e13148 fix: route sort/limit post-join when sort targets a non-main
 *   - f1cef0e fix: force sort+limit post-join when any linked-only split
 *
 * READ-ONLY in src/ — this file only adds a test. No production code edits.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

/**
 * Wrap a promise-returning query handler into plywood's requester shape
 * (a PassThrough of row objects). Same helper used in
 * simulateDruidSqlMagicAttributes.mocha.js.
 */
function promiseFnToStream(promiseRq) {
  return rq => {
    const stream = new PassThrough({ objectMode: true });
    promiseRq(rq).then(
      res => {
        if (Array.isArray(res)) {
          for (const row of res) stream.write(row);
        } else if (res) {
          stream.write(res);
        }
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

const filterStart = new Date('2026-05-01T00:00:00Z');
const filterEnd = new Date('2026-05-02T00:00:00Z');
const outerTimeFilter = $('__time').overlap({ start: filterStart, end: filterEnd });
const innerTimeFilter = $('__time').overlap({ start: filterStart, end: filterEnd });

/**
 * Build the same External shape Turnilo's magic-attribute reconciler emits
 * for a canonical lookup keyed by productName. Field-for-field parity with
 * the kernel's LinkedSourceConfig output — no invented fields, no missing
 * fields the contract requires.
 */
function makeMainWithUsoTipicoLookup(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'main_ds',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'productName', type: 'STRING' },
        { name: 'userIdProduct', type: 'STRING' },
      ],
      linkedSources: {
        lookup_uso_tipico: {
          source: 'lookup_uso_tipico_rev1',
          joinKeys: ['productName'],
          autoInjectJoinKeys: ['productName'],
          sharedDimensions: ['productName'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          backing: { phase: 'canonical' },
          attributes: [
            { name: 'productName', type: 'STRING' },
            { name: 'uso_tipico', type: 'STRING' },
            { name: 'confidence', type: 'NUMBER' },
          ],
        },
      },
      filter: outerTimeFilter,
    },
    requester,
  );
}

/**
 * Build the Turnilo-style top-level expression. Outer ply wraps a
 * scope-registration apply for both sources, a totals measure, and the
 * SPLIT apply where the linked-only dimension drives the grid.
 */
function buildUserExpression() {
  return ply()
    .apply('main', $('main').filter(innerTimeFilter))
    .apply('lookup_uso_tipico', $('lookup_uso_tipico').filter(innerTimeFilter))
    .apply('unique_products', '$main.countDistinct($userIdProduct)')
    .apply(
      'SPLIT',
      $('main')
        .split('$uso_tipico', 'uso_tipico')
        .apply('unique_products', '$main.countDistinct($userIdProduct)')
        .sort('$uso_tipico', 'descending')
        .limit(5000),
    );
}

describe('Cross-source — linked-only split with main-side measure (RED)', () => {
  /**
   * Post-fix (cycle-2): pin the native-JOIN shape. The previous bug
   * emitted 2 main queries + 1 lookup query and joined in JS without
   * re-aggregating, producing 3 rows for 2 buckets. The cycle-1 +
   * cycle-2 fix routes non-decomposable measures (countDistinct +
   * linked-only split) into a single combined SQL with an INNER JOIN
   * and GROUP BY 1 on the bucket — yielding exactly one row per
   * bucket inside Druid. The outer-ply totals apply still emits its
   * own main-only query.
   */
  it('plan: one combined main+lookup INNER JOIN query + one main-only totals query', () => {
    const main = makeMainWithUsoTipicoLookup();
    const ex = buildUserExpression();
    const plan = ex.simulateQueryPlan({ main });
    const queries = plan.flat().filter(q => typeof q.query === 'string');

    const mainOnlyQueries = queries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('lookup_uso_tipico_rev1'),
    );
    const combinedQueries = queries.filter(
      q =>
        q.query.includes('"main_ds"') &&
        q.query.includes('"lookup_uso_tipico_rev1"') &&
        /INNER JOIN/i.test(q.query),
    );

    expect(mainOnlyQueries, 'one main-only totals query').to.have.length(1);
    expect(combinedQueries, 'one combined INNER JOIN query').to.have.length(1);

    const combined = combinedQueries[0].query;
    // Native-JOIN must carry the linked-only split as the lookup column
    // and the main-side measure (countDistinct).
    expect(combined, 'combined query carries uso_tipico').to.match(/uso_tipico/);
    expect(combined, 'combined query GROUPs by the bucket').to.match(/GROUP BY 1/);
  });

  /**
   * The bug pin. Three products mapping to two buckets. After the
   * in-memory join plywood currently returns three rows (one per
   * productName) where the contract demands exactly two (one per
   * uso_tipico bucket). Pinning cardinality is the user-observable
   * invariant; we don't care which fields the engine projects, only how
   * many distinct grid rows reach the caller.
   */
  it('SPLIT.data must contain exactly one row per distinct uso_tipico bucket', async () => {
    const dispatched = [];
    const requester = promiseFnToStream(rq => {
      dispatched.push(rq);
      const sql = (rq && rq.query && rq.query.query) || '';

      // F6 (cycle-2): post-fix the engine emits ONE combined SQL with
      // INNER JOIN that references BOTH "main_ds" and
      // "lookup_uso_tipico_rev1" and GROUPs BY the bucket — yielding
      // 2 post-grouped rows (one per uso_tipico bucket). Match this
      // branch FIRST so the legacy 2-query stubs below don't fire on
      // the combined SQL.
      if (
        sql.includes('"main_ds"') &&
        sql.includes('"lookup_uso_tipico_rev1"') &&
        /INNER JOIN/i.test(sql)
      ) {
        return Promise.resolve([
          { uso_tipico: 'Industrial', unique_products: 2 }, // P1+P2 distinct userIdProducts
          { uso_tipico: 'Retail', unique_products: 1 }, // P3
        ]);
      }

      // Linked side: 3 products, 2 buckets. P1,P2 -> Industrial. P3 -> Retail.
      if (sql.includes('"lookup_uso_tipico_rev1"')) {
        return Promise.resolve([
          { __join_productName: 'P1', uso_tipico: 'Industrial' },
          { __join_productName: 'P2', uso_tipico: 'Industrial' },
          { __join_productName: 'P3', uso_tipico: 'Retail' },
        ]);
      }

      // Main split side: grouped by productName. Each product has a distinct
      // userIdProduct count contribution.
      if (sql.includes('"main_ds"') && /GROUP BY 1\b/.test(sql)) {
        return Promise.resolve([
          { __join_productName: 'P1', unique_products: 10 },
          { __join_productName: 'P2', unique_products: 20 },
          { __join_productName: 'P3', unique_products: 30 },
        ]);
      }

      // Main totals query — the outer-ply apply('unique_products', ...).
      if (sql.includes('"main_ds"')) {
        return Promise.resolve([{ __VALUE__: 60 }]);
      }

      return Promise.resolve([]);
    });

    const main = makeMainWithUsoTipicoLookup(requester);
    const result = await buildUserExpression().compute({ main });
    const resultJs = result && result.toJS ? result.toJS() : result;

    const splitRows =
      resultJs && resultJs.data && resultJs.data[0] && resultJs.data[0].SPLIT
        ? resultJs.data[0].SPLIT.data
        : null;

    expect(splitRows, 'SPLIT.data should be populated').to.be.an('array');

    // Hard cardinality assertion. Fixture: 3 products, 2 distinct buckets.
    // Expected: 2 rows. Bug today: 3 rows (one per productName).
    const distinctBuckets = new Set(splitRows.map(r => r.uso_tipico));
    expect(
      splitRows.length,
      `SPLIT.data row count: expected ${distinctBuckets.size} (one per distinct uso_tipico) ` +
        `but got ${splitRows.length}. ` +
        `Rows seen: ${JSON.stringify(splitRows)}. ` +
        `Distinct buckets in result: ${JSON.stringify([...distinctBuckets])}.`,
    ).to.equal(distinctBuckets.size);

    // Belt-and-braces: the dimension is single-valued per row. The bug
    // happens to satisfy this too (each row has one uso_tipico) so it
    // can't fire alone, but together with the cardinality assertion it
    // pins the full invariant.
    for (const row of splitRows) {
      expect(typeof row.uso_tipico, 'uso_tipico must be a string per row').to.equal('string');
    }

    // And: no two rows share the same uso_tipico value. The bug today
    // emits two rows both labelled 'Industrial' (P1 and P2). This
    // assertion is the orthogonal projection of the cardinality bug —
    // either firing is sufficient to declare RED.
    const seen = new Set();
    for (const row of splitRows) {
      expect(
        seen.has(row.uso_tipico),
        `duplicate uso_tipico bucket in SPLIT.data: '${row.uso_tipico}' appears ` +
          `more than once. Full row dump: ${JSON.stringify(splitRows)}.`,
      ).to.equal(false);
      seen.add(row.uso_tipico);
    }
  });
});
