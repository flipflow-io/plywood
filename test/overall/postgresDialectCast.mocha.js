/*
 * Pin: PostgresDialect.castExpression supports the identity/wildcard
 * casts that the cross-source auto-inject joinKey path emits.
 *
 * Regression context (2026-06-02): a staging magic-attr linkedSource
 * (PostgresExternal over a magic_staging_* view) failed at SQL render
 * with "unsupported cast from STRING to STRING in Postgres dialect"
 * because the auto-inject path casts join keys to STRING and the
 * Postgres CAST_TO_FUNCTION table had no identity/wildcard entry —
 * unlike DruidDialect, which has `_` fallbacks per target.
 */

const { expect } = require('chai');
const { PostgresDialect } = require('../plywood');

describe('PostgresDialect.castExpression', () => {
  const dialect = new PostgresDialect();

  it('STRING → STRING (identity, the auto-inject joinKey shape) renders ::text', () => {
    expect(dialect.castExpression('STRING', '"brand"', 'STRING')).to.equal('"brand"::text');
  });

  it('NUMBER → STRING keeps the explicit entry', () => {
    expect(dialect.castExpression('NUMBER', '"price"', 'STRING')).to.equal('"price"::text');
  });

  it('NUMBER → NUMBER falls back to the wildcard', () => {
    expect(dialect.castExpression('NUMBER', '"price"', 'NUMBER')).to.equal('"price"::float');
  });

  it('TIME wildcard renders CAST AS TIMESTAMP', () => {
    expect(dialect.castExpression('STRING', '"t"', 'TIME')).to.equal('CAST("t" AS TIMESTAMP)');
  });

  it('unknown target type still throws loud', () => {
    expect(() => dialect.castExpression('STRING', '"x"', 'GEO')).to.throw(
      /unsupported cast from STRING to GEO in Postgres dialect/,
    );
  });
});
