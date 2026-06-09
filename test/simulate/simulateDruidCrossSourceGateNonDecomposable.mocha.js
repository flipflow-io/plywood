/*
 * RED spec — Phase 3 gate (INV-2).
 *
 * countDistinct + linked-only split must NOT take the JS-join path.
 * The gate inside getCrossExternalDecomposition reads
 * Expression.isMeasureDecomposable on each main-side apply; if any
 * is non-decomposable AND the query has a linked-only split, the
 * gate must short-circuit and either:
 *   - return null (Phase 3 stops here — caller falls through to the
 *     single-query path, which itself fails fast at SQL emission
 *     time with a clear "column not found" rather than silently
 *     producing duplicated rows), OR
 *   - return a `kind: 'nativeJoin'` discriminator (Phase 4).
 *
 * This test pins the Phase 3 behaviour: the gate returns null on
 * countDistinct + linked-only split. Phase 4 will reuse the same
 * fixture and assert `kind === 'nativeJoin'`.
 *
 * Counterfactual: hard-coding the gate to always-allow JS-join
 * makes this red AND lets the original bug reproduce.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $ } = plywood;

const filterStart = new Date('2026-05-01T00:00:00Z');
const filterEnd = new Date('2026-05-02T00:00:00Z');
const timeFilter = $('__time').overlap({ start: filterStart, end: filterEnd });

function makeMainWithUsoTipicoLookup() {
  return External.fromJS({
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
        attributes: [
          { name: 'productName', type: 'STRING' },
          { name: 'uso_tipico', type: 'STRING' },
        ],
      },
    },
    filter: timeFilter,
  });
}

describe('Cross-source decomposability gate (Phase 3)', () => {
  // Build a split-mode External directly by following the same
  // recipe getCrossExternalDecomposition uses — addExpression of the
  // split, then the apply. `External.fromJS` returns the cube in
  // 'raw' mode; the predicate operates on the split-mode result.
  function buildSplitExternal(applyExpr) {
    const main = makeMainWithUsoTipicoLookup();
    const split = plywood.SplitExpression.fromJS ? null : null; // not needed — use addExpression chain.
    const withSplit = main.addExpression(
      plywood.Expression.fromJS({ op: 'ref', name: 'main' }).split('$uso_tipico', 'uso_tipico'),
    );
    return withSplit.addExpression(applyExpr);
  }

  it('countDistinct + linked-only split returns nativeJoin shape (not jsJoin)', () => {
    const apply = plywood.Expression.fromJS({
      op: 'apply',
      operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
      expression: {
        op: 'countDistinct',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'userIdProduct' },
      },
      name: 'unique_products',
    });
    const splitExt = buildSplitExternal(apply);
    expect(splitExt, 'split-mode external built').to.exist;
    expect(splitExt.mode, 'mode is split').to.equal('split');
    const crossExt = splitExt.getCrossExternalDecomposition();
    // F3 (cycle-2): the gate MUST emit a nativeJoin shape — never null.
    // Null is not an acceptable "soft refusal" because the caller's
    // single-source fallback (`splitToDruid`) fails late with
    // "could not get attribute info for X" instead of the engine
    // surfacing the unsupported shape cleanly. We pin nativeJoin AND
    // a real INNER JOIN in the emitted SQL.
    expect(crossExt, 'gate must produce nativeJoin shape, never null').to.not.be.null;
    expect(crossExt.kind, 'kind discriminator').to.equal('nativeJoin');
    expect(crossExt.nativeJoin, 'nativeJoin payload').to.exist;
    expect(crossExt.nativeJoin.sql, 'SQL contains INNER JOIN').to.match(/INNER JOIN/i);
  });

  it('sum + linked-only split STILL takes the JS-join path (kind=jsJoin or absent)', () => {
    // Build with a NUMBER attribute on main so sum can apply.
    const mainNum = External.fromJS({
      engine: 'druidsql',
      source: 'main_ds',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'productName', type: 'STRING' },
        { name: 'price', type: 'NUMBER' },
      ],
      linkedSources: {
        lookup_uso_tipico: {
          source: 'lookup_uso_tipico_rev1',
          joinKeys: ['productName'],
          autoInjectJoinKeys: ['productName'],
          sharedDimensions: ['productName'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: 'productName', type: 'STRING' },
            { name: 'uso_tipico', type: 'STRING' },
          ],
        },
      },
      filter: timeFilter,
    });
    const split = mainNum.addExpression(
      plywood.Expression.fromJS({ op: 'ref', name: 'main' }).split('$uso_tipico', 'uso_tipico'),
    );
    const apply = plywood.Expression.fromJS({
      op: 'apply',
      operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
      expression: {
        op: 'sum',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'price' },
      },
      name: 'total_price',
    });
    const splitExt = split.addExpression(apply);
    expect(splitExt, 'split-mode external built').to.exist;
    const crossExt = splitExt.getCrossExternalDecomposition();
    expect(crossExt, 'gate returned a decomposition object').to.exist;
    // 'jsJoin' is the new discriminator value (or undefined if the
    // gate keeps the legacy shape untagged — accept both as long as
    // it's NOT 'nativeJoin').
    expect(crossExt.kind, 'kind must not be nativeJoin for sum').to.not.equal('nativeJoin');
  });
});
