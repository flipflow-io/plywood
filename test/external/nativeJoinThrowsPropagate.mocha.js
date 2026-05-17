/*
 * RED spec — Phase F5 (cycle-2): unsupported native-JOIN shapes
 * propagate `PlywoodUnsupportedNativeJoinShape` through the caller
 * (`getCrossExternalDecomposition`) without being swallowed.
 *
 * Contract: when the gate decides native-JOIN is needed but the
 * concrete shape isn't emittable (e.g. native Druid engine has no
 * SQLDialect), the caller MUST NOT silently fall back to the single-
 * source path. It must let the throw surface so the consumer sees a
 * specific, debuggable reason — never a cryptic Druid inflater error.
 *
 * Counterfactual: wrap the call in `try { ... } catch (e) { return ... }`
 * → this test red.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, PlywoodUnsupportedNativeJoinShape, $ } = plywood;

describe('Cross-source — native-JOIN throws propagate through caller (F5)', () => {
  it('native Druid + countDistinct + linked-only split throws PlywoodUnsupportedNativeJoinShape', () => {
    // engine: 'druid' (NATIVE, no SQLDialect) → site 3362 in
    // getNativeJoinDecomposition; gate fires for countDistinct on a
    // linked-only split; throw must propagate through
    // getCrossExternalDecomposition.
    const main = External.fromJS({
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
    const split = main.addExpression(
      plywood.Expression.fromJS({ op: 'ref', name: 'main' }).split('$uso_tipico', 'uso_tipico'),
    );
    const splitExt = split.addExpression(
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
    expect(() => splitExt.getCrossExternalDecomposition()).to.throw(
      PlywoodUnsupportedNativeJoinShape,
      /native Druid engine has no SQLDialect/,
    );
  });

  it('substr() split on a linked-only ref throws "not a bare RefExpression"', () => {
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
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: 'productName', type: 'STRING' },
            { name: 'uso_tipico', type: 'STRING' },
          ],
        },
      },
    });
    const split = main.addExpression(
      plywood.Expression.fromJS({ op: 'ref', name: 'main' }).split(
        { op: 'substr', operand: { op: 'ref', name: 'uso_tipico' }, position: 0, len: 3 },
        'uso_tipico_3',
      ),
    );
    const splitExt = split.addExpression(
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
    expect(() => splitExt.getCrossExternalDecomposition()).to.throw(
      PlywoodUnsupportedNativeJoinShape,
      /not a bare RefExpression/,
    );
  });
});
