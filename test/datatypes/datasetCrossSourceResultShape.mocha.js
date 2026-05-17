/*
 * Phase 6 — result-shape pins for the cross-source path.
 *
 * Per the audit's section 4 item 2 (Dataset-level behaviour): build
 * a synthetic main + linked dataset, run them through
 * `External.assertDatasetShape` to confirm the INV-1 net catches
 * the historic bug shape (one main row per productName fanning out
 * into multiple linked rows per uso_tipico bucket).
 *
 * Plus a positive pin: the post-fix Dataset shape (already
 * re-aggregated to one row per bucket) passes the net.
 *
 * These tests are additive to Phase 6 — they don't extend the
 * canonical matrix file (Shape 8/10/11c result siblings) because
 * after Phase 4 the matrix's SQL-shape assumptions (1 main + 1
 * linked query) no longer hold for non-sum measures — those shapes
 * now emit a single native-JOIN query. The canonical matrix
 * extension is filed as a follow-up (see plywood-aggregation-fix-
 * status.md sorpresas).
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { Dataset, External, PlywoodCardinalityViolation } = plywood;

describe('Cross-source result-shape pins (Phase 6)', () => {
  it('historic buggy shape (one row per productName×uso_tipico) FAILS the net', () => {
    // Recreate the post-`dropColumns` shape pinned by the audit:
    // (P1→Industrial, P2→Industrial, P3→Retail) joined with main
    // (P1→10, P2→20, P3→30), then dropping __join_productName.
    // Result: 3 rows, keys=['uso_tipico'], two rows labeled
    // 'Industrial'. INV-1 must catch this.
    const buggy = Dataset.fromJS({
      keys: ['uso_tipico'],
      attributes: [
        { name: 'uso_tipico', type: 'STRING' },
        { name: 'unique_products', type: 'NUMBER' },
      ],
      data: [
        { uso_tipico: 'Industrial', unique_products: 10 },
        { uso_tipico: 'Industrial', unique_products: 20 },
        { uso_tipico: 'Retail', unique_products: 30 },
      ],
    });
    expect(() => External.assertDatasetShape(buggy))
      .to.throw(PlywoodCardinalityViolation)
      .with.property('message')
      .that.matches(/Industrial/);
  });

  it('post-fix shape (one row per uso_tipico bucket) PASSES the net', () => {
    // Same fixture, but already re-aggregated:
    //   (Industrial → 30 distinct, Retail → 1 distinct).
    const fixed = Dataset.fromJS({
      keys: ['uso_tipico'],
      attributes: [
        { name: 'uso_tipico', type: 'STRING' },
        { name: 'unique_products', type: 'NUMBER' },
      ],
      data: [
        { uso_tipico: 'Industrial', unique_products: 2 },
        { uso_tipico: 'Retail', unique_products: 1 },
      ],
    });
    expect(() => External.assertDatasetShape(fixed)).to.not.throw();
  });

  it('row count exactly equals distinct(keys) — section 6 P3 invariant', () => {
    // P3: every result-shape test asserts data.length === distinct(keys).
    // Pin this directly: any dataset that violates it must throw.
    const ds = Dataset.fromJS({
      keys: ['bucket'],
      attributes: [
        { name: 'bucket', type: 'STRING' },
        { name: 'v', type: 'NUMBER' },
      ],
      data: [
        { bucket: 'A', v: 1 },
        { bucket: 'B', v: 2 },
        { bucket: 'A', v: 3 }, // duplicate
      ],
    });
    // INV-1 + P3 together: a row count > distinct(keys) is fail-loud.
    const distinct = new Set(ds.data.map(r => r.bucket)).size;
    expect(ds.data.length).to.be.above(distinct);
    expect(() => External.assertDatasetShape(ds)).to.throw(PlywoodCardinalityViolation);
  });
});
