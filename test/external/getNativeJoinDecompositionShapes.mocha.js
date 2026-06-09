/*
 * RED spec — Phase F4 (cycle-2): getNativeJoinDecomposition throws
 * `PlywoodUnsupportedNativeJoinShape` on every unsupported shape
 * instead of returning null silently.
 *
 * Each test below constructs the minimal External + arguments that
 * trip a specific guard inside `getNativeJoinDecomposition` and
 * pins the throw with a regex on the specific reason string.
 *
 * Counterfactual: revert any single throw to `return null` → the
 * matching test goes red.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, PlywoodUnsupportedNativeJoinShape } = plywood;

function makeMainDruidSql() {
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
  });
}

function makeMainDruidNative() {
  return External.fromJS({
    engine: 'druid',
    source: 'main_ds',
    timeAttribute: '__time',
    allowSelectQueries: true,
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
  });
}

function buildSplit(main, splitExprJS, splitAlias) {
  const withSplit = main.addExpression(
    plywood.Expression.fromJS({ op: 'ref', name: 'main' }).split(
      splitExprJS,
      splitAlias || 'uso_tipico',
    ),
  );
  return withSplit.addExpression(
    plywood.Expression.fromJS({
      op: 'apply',
      operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
      expression: {
        op: 'countDistinct',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'userIdProduct' },
      },
      name: 'unique_products',
    }),
  );
}

describe('getNativeJoinDecomposition — fail-loud on unsupported shapes (F4)', () => {
  it('throws when linkedOnlySplitAliases is empty (count=0)', () => {
    const splitExt = buildSplit(makeMainDruidSql(), '$uso_tipico');
    expect(() => splitExt.getNativeJoinDecomposition([], [], { lookup_uso_tipico: true })).to.throw(
      PlywoodUnsupportedNativeJoinShape,
      /multi-alias linked-only split/,
    );
  });

  it('throws when linkedOnlySplitAliases has >1 alias', () => {
    const splitExt = buildSplit(makeMainDruidSql(), '$uso_tipico');
    expect(() =>
      splitExt.getNativeJoinDecomposition(['a', 'b'], [], { lookup_uso_tipico: true }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /multi-alias linked-only split.*count=2/);
  });

  it('throws when multiple linkedSources are involved', () => {
    const splitExt = buildSplit(makeMainDruidSql(), '$uso_tipico');
    expect(() =>
      splitExt.getNativeJoinDecomposition(['uso_tipico'], [], {
        lookup_uso_tipico: true,
        another_lookup: true,
      }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /multiple linkedSources involved/);
  });

  it('throws when the linkedSource name is missing from linkedSources map', () => {
    const splitExt = buildSplit(makeMainDruidSql(), '$uso_tipico');
    expect(() =>
      splitExt.getNativeJoinDecomposition(['uso_tipico'], [], { does_not_exist: true }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /missing in linkedSources map/);
  });

  it('throws when joinMode is undefined on the linkedSource', () => {
    const main = External.fromJS({
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
          // joinMode INTENTIONALLY OMITTED
          timeAlignment: 'eternal',
          attributes: [
            { name: 'productName', type: 'STRING' },
            { name: 'uso_tipico', type: 'STRING' },
          ],
        },
      },
    });
    const splitExt = buildSplit(main, '$uso_tipico');
    expect(() =>
      splitExt.getNativeJoinDecomposition(['uso_tipico'], [], { lookup_uso_tipico: true }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /missing joinMode/);
  });

  it('throws when joinKeys is empty', () => {
    const main = External.fromJS({
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
          joinKeys: [],
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
    });
    const splitExt = buildSplit(main, '$uso_tipico');
    expect(() =>
      splitExt.getNativeJoinDecomposition(['uso_tipico'], [], { lookup_uso_tipico: true }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /empty joinKeys/);
  });

  it('throws when engine is native Druid (no SQLDialect)', () => {
    const splitExt = buildSplit(makeMainDruidNative(), '$uso_tipico');
    expect(() =>
      splitExt.getNativeJoinDecomposition(['uso_tipico'], [], { lookup_uso_tipico: true }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /native Druid engine has no SQLDialect/);
  });

  it('throws when the split alias is not present in this.split.splits', () => {
    const splitExt = buildSplit(makeMainDruidSql(), '$uso_tipico');
    expect(() =>
      splitExt.getNativeJoinDecomposition(['not_in_splits'], [], { lookup_uso_tipico: true }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /split alias "not_in_splits" not found/);
  });

  it('throws when the split expression is not a bare RefExpression (TIME_FLOOR/SUBSTR etc.)', () => {
    // Build the split with a substr() wrapper — not a bare ref.
    const main = makeMainDruidSql();
    const withSplit = main.addExpression(
      plywood.Expression.fromJS({ op: 'ref', name: 'main' }).split(
        { op: 'substr', operand: { op: 'ref', name: 'uso_tipico' }, position: 0, len: 3 },
        'uso_tipico_3',
      ),
    );
    const splitExt = withSplit.addExpression(
      plywood.Expression.fromJS({
        op: 'apply',
        operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
        expression: {
          op: 'countDistinct',
          operand: { op: 'ref', name: 'main' },
          expression: { op: 'ref', name: 'userIdProduct' },
        },
        name: 'unique_products',
      }),
    );
    expect(() =>
      splitExt.getNativeJoinDecomposition(['uso_tipico_3'], [], { lookup_uso_tipico: true }),
    ).to.throw(PlywoodUnsupportedNativeJoinShape, /not a bare RefExpression/);
  });
});
