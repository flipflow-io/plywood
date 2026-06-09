/*
 * RED spec — Phase F2 (cycle-2): average decomposes pre-gate.
 *
 * Contract: `average($x)` is mathematically `sum($x) / count()`. The
 * AverageExpression.decomposeAverage rewrite (baseExpression.ts:1805)
 * already exists and is invariant-preserving. Cross-source gate must
 * apply this rewrite BEFORE evaluating Expression.isMeasureDecomposable
 * on each main-side apply — otherwise avg's trait ('none') routes the
 * query to a needlessly heavy native-JOIN even though sum/count are
 * both 'sum'-trait and the JS-join path is perfectly safe.
 *
 * What this test asserts:
 *
 *   Given Shape 8 (avg($price) on main + linked-only split on a magic
 *   dim), `getCrossExternalDecomposition()` must return a JS-join
 *   shape (kind === 'jsJoin' OR absent kind discriminator with both
 *   `main` and `linkedExternals` populated), NOT a native-JOIN.
 *
 * Counterfactual: forget to call decomposeAverage pre-gate → avg's
 * 'none' trait fires the gate → return nativeJoin → this test red.
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
      { name: 'price', type: 'NUMBER', unsplitable: true },
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

describe('Cross-source decomposability — average is decomposed pre-gate (F2)', () => {
  function buildSplitExternal(applyExpr) {
    const main = makeMainWithUsoTipicoLookup();
    const withSplit = main.addExpression(
      plywood.Expression.fromJS({ op: 'ref', name: 'main' }).split('$uso_tipico', 'uso_tipico'),
    );
    return withSplit.addExpression(applyExpr);
  }

  it('average($price) + linked-only split routes to jsJoin (NOT nativeJoin)', () => {
    const apply = plywood.Expression.fromJS({
      op: 'apply',
      operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
      expression: {
        op: 'average',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'price' },
      },
      name: 'AvgPrice',
    });
    const splitExt = buildSplitExternal(apply);
    expect(splitExt.mode, 'mode is split').to.equal('split');
    const crossExt = splitExt.getCrossExternalDecomposition();
    // avg must decompose to sum/count before the gate so it routes
    // through the JS-join path (linkedExternals populated, no
    // nativeJoin discriminator).
    expect(crossExt, 'gate must produce a decomposition shape').to.not.be.null;
    expect(crossExt.kind, 'avg must NOT trigger nativeJoin path').to.not.equal('nativeJoin');
    expect(crossExt.linkedExternals, 'JS-join populates linkedExternals').to.exist;
    expect(crossExt.linkedExternals.length, 'one linked side').to.be.greaterThan(0);
  });

  it('avg-rewritten main apply produces a single linkedExternal entry for uso_tipico', () => {
    const apply = plywood.Expression.fromJS({
      op: 'apply',
      operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
      expression: {
        op: 'average',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'price' },
      },
      name: 'AvgPrice',
    });
    const splitExt = buildSplitExternal(apply);
    const crossExt = splitExt.getCrossExternalDecomposition();
    expect(crossExt).to.not.be.null;
    const linkedNames = (crossExt.linkedExternals || []).map(le => le.name);
    expect(linkedNames, 'linkedExternals carries lookup_uso_tipico').to.include(
      'lookup_uso_tipico',
    );
  });
});
