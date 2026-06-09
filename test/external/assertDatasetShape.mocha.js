/*
 * RED spec — External.assertDatasetShape (INV-1, INV-4).
 *
 * The transparent net inserted right before `return joined` in
 * queryBasicValueStream's cross-source path. Catches "post-join row
 * count > distinct(keys)" — the engine-level symptom of the
 * cross-source decomposition bug pinned by
 * simulateDruidCrossSourceLinkedOnlySplitWithMainMeasure.mocha.js.
 *
 * Three cases:
 *   (a) clean dataset (distinct key tuples) — passes silently.
 *   (b) duplicated key tuple — throws PlywoodCardinalityViolation
 *       synchronously, with the duplicate tuple + delta in the
 *       message (fault-localization, not just fault-presence).
 *   (c) keys-empty totals dataset — skipped, no throw.
 *
 * Counterfactuals:
 *   - If the assertion is a no-op `return joined`, (b) fails.
 *   - If it throws on empty-keys totals, (c) fails.
 *   - If the message omits the duplicate tuple, the regex in (b) fails.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { Dataset, External, PlywoodCardinalityViolation } = plywood;

describe('External.assertDatasetShape', () => {
  it('passes a clean dataset where every key tuple is distinct', () => {
    const ds = Dataset.fromJS({
      keys: ['k'],
      attributes: [
        { name: 'k', type: 'STRING' },
        { name: 'v', type: 'NUMBER' },
      ],
      data: [
        { k: 'A', v: 1 },
        { k: 'B', v: 2 },
      ],
    });
    expect(() => External.assertDatasetShape(ds)).to.not.throw();
  });

  it('throws PlywoodCardinalityViolation when keys are not unique', () => {
    const ds = Dataset.fromJS({
      keys: ['k'],
      attributes: [
        { name: 'k', type: 'STRING' },
        { name: 'v', type: 'NUMBER' },
      ],
      data: [
        { k: 'A', v: 1 },
        { k: 'A', v: 2 },
      ],
    });
    expect(() => External.assertDatasetShape(ds))
      .to.throw(PlywoodCardinalityViolation)
      .with.property('message')
      .that.matches(/duplicate key tuple/)
      .and.that.matches(/A/)
      .and.that.matches(/delta 1/);
  });

  it('skips totals datasets (keys empty or undefined)', () => {
    const ds = Dataset.fromJS({
      attributes: [{ name: 'v', type: 'NUMBER' }],
      data: [{ v: 42 }, { v: 43 }],
    });
    // No `keys` declared — totals datasets carry one row of aggregates
    // but the engine still produces them; the net must not false-alarm.
    expect(() => External.assertDatasetShape(ds)).to.not.throw();
  });

  it('passes a multi-key dataset where the tuple is distinct even if individual keys repeat', () => {
    const ds = Dataset.fromJS({
      keys: ['k1', 'k2'],
      attributes: [
        { name: 'k1', type: 'STRING' },
        { name: 'k2', type: 'STRING' },
        { name: 'v', type: 'NUMBER' },
      ],
      data: [
        { k1: 'A', k2: 'X', v: 1 },
        { k1: 'A', k2: 'Y', v: 2 },
        { k1: 'B', k2: 'X', v: 3 },
      ],
    });
    expect(() => External.assertDatasetShape(ds)).to.not.throw();
  });

  it('reports the duplicated tuple including all key fields', () => {
    const ds = Dataset.fromJS({
      keys: ['k1', 'k2'],
      attributes: [
        { name: 'k1', type: 'STRING' },
        { name: 'k2', type: 'STRING' },
        { name: 'v', type: 'NUMBER' },
      ],
      data: [
        { k1: 'A', k2: 'X', v: 1 },
        { k1: 'A', k2: 'X', v: 2 },
        { k1: 'B', k2: 'Y', v: 3 },
      ],
    });
    expect(() => External.assertDatasetShape(ds))
      .to.throw(PlywoodCardinalityViolation)
      .with.property('message')
      .that.matches(/A/)
      .and.that.matches(/X/);
  });
});
