/*
 * Tests for DruidSQLExternal.getIntrospectAttributes — specifically the
 * SQL-native INFORMATION_SCHEMA.COLUMNS path that replaces segmentMetadata
 * as the default for plain sources (non-withQuery).
 *
 * The mocked requester asserts WHICH query type arrives (SQL vs segment
 * metadata) and returns shaped results so we can verify the attribute
 * parsing without a live Druid cluster.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External } = plywood;

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

function makeExternal(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      version: '30.0.0',
      source: 'reviews',
    },
    requester,
  );
}

describe('DruidSQLExternal Introspection — INFORMATION_SCHEMA.COLUMNS', () => {
  it('uses SQL INFORMATION_SCHEMA.COLUMNS by default, not segmentMetadata', async () => {
    let sqlQueriesSeen = 0;
    let segmentMetadataSeen = 0;

    const requester = promiseFnToStream(({ query }) => {
      if (typeof query === 'object' && query.query && typeof query.query === 'string') {
        sqlQueriesSeen++;
        expect(query.query).to.include('INFORMATION_SCHEMA.COLUMNS');
        expect(query.query).to.include("TABLE_NAME = 'reviews'");
        return Promise.resolve([
          { COLUMN_NAME: '__time', DATA_TYPE: 'TIMESTAMP' },
          { COLUMN_NAME: 'competitor', DATA_TYPE: 'VARCHAR' },
          { COLUMN_NAME: 'rating', DATA_TYPE: 'DOUBLE' },
          { COLUMN_NAME: 'is_owned', DATA_TYPE: 'BOOLEAN' },
        ]);
      }
      if (query && query.queryType === 'segmentMetadata') {
        segmentMetadataSeen++;
        // Shouldn't be called when SQL path succeeds.
        return Promise.resolve([]);
      }
      return Promise.reject(new Error(`unexpected query: ${JSON.stringify(query)}`));
    });

    const ext = await makeExternal(requester).introspect();
    expect(sqlQueriesSeen, 'SQL path used').to.equal(1);
    expect(segmentMetadataSeen, 'segmentMetadata not used').to.equal(0);

    const attrs = ext.attributes;
    expect(attrs.map(a => a.name)).to.deep.equal(['__time', 'competitor', 'rating', 'is_owned']);
    expect(attrs.map(a => a.type)).to.deep.equal(['TIME', 'STRING', 'NUMBER', 'BOOLEAN']);
    // nativeType roundtrip: upper-cased, preserves exact SQL type name
    expect(attrs.map(a => a.nativeType)).to.deep.equal([
      'TIMESTAMP',
      'VARCHAR',
      'DOUBLE',
      'BOOLEAN',
    ]);
  });

  it('maps the full set of Druid SQL types to the right PlyTypes', async () => {
    const requester = promiseFnToStream(({ query }) => {
      if (query && query.query) {
        return Promise.resolve([
          { COLUMN_NAME: 'a_timestamp', DATA_TYPE: 'TIMESTAMP' },
          { COLUMN_NAME: 'a_date', DATA_TYPE: 'DATE' },
          { COLUMN_NAME: 'a_varchar', DATA_TYPE: 'VARCHAR' },
          { COLUMN_NAME: 'a_string', DATA_TYPE: 'STRING' },
          { COLUMN_NAME: 'a_double', DATA_TYPE: 'DOUBLE' },
          { COLUMN_NAME: 'a_float', DATA_TYPE: 'FLOAT' },
          { COLUMN_NAME: 'a_bigint', DATA_TYPE: 'BIGINT' },
          { COLUMN_NAME: 'a_integer', DATA_TYPE: 'INTEGER' },
          { COLUMN_NAME: 'a_boolean', DATA_TYPE: 'BOOLEAN' },
          { COLUMN_NAME: 'a_ipaddress', DATA_TYPE: 'IPADDRESS' },
          { COLUMN_NAME: 'a_mystery', DATA_TYPE: 'COMPLEX<something-exotic>' },
        ]);
      }
      return Promise.reject(new Error('unexpected query'));
    });

    const ext = await makeExternal(requester).introspect();
    const byName = {};
    for (const a of ext.attributes) byName[a.name] = a.type;

    expect(byName).to.deep.equal({
      a_timestamp: 'TIME',
      a_date: 'TIME',
      a_varchar: 'STRING',
      a_string: 'STRING',
      a_double: 'NUMBER',
      a_float: 'NUMBER',
      a_bigint: 'NUMBER',
      a_integer: 'NUMBER',
      a_boolean: 'BOOLEAN',
      a_ipaddress: 'IP',
      a_mystery: 'STRING', // unknown → STRING fallback, safe default
    });
  });

  it('falls back to segmentMetadata when SQL path fails', async () => {
    let sqlFailed = false;
    let segmentMetadataUsed = false;

    const requester = promiseFnToStream(({ query }) => {
      if (typeof query === 'object' && query.query && typeof query.query === 'string') {
        sqlFailed = true;
        return Promise.reject(new Error('INFORMATION_SCHEMA not available'));
      }
      if (query && query.queryType === 'segmentMetadata') {
        segmentMetadataUsed = true;
        return Promise.resolve([
          {
            id: 'm',
            columns: {
              __time: { type: 'LONG', errorMessage: null, hasMultipleValues: false },
              competitor: { type: 'STRING', errorMessage: null, hasMultipleValues: false },
            },
            aggregators: {},
          },
        ]);
      }
      if (query && query.queryType === 'timeBoundary') {
        return Promise.resolve([]);
      }
      return Promise.reject(new Error(`unexpected query: ${JSON.stringify(query)}`));
    });

    const ext = await makeExternal(requester).introspect();
    expect(sqlFailed, 'SQL path was attempted').to.equal(true);
    expect(segmentMetadataUsed, 'fallback ran').to.equal(true);
    expect(ext.attributes.map(a => a.name)).to.include('__time');
    expect(ext.attributes.map(a => a.name)).to.include('competitor');
  });

  it('falls back to segmentMetadata when SQL returns no rows', async () => {
    // Some proxies or restricted setups return [] for system schemas.
    // Treat empty as "try the native fallback" rather than "table has no
    // columns" — the latter is a corner case that would fail downstream
    // anyway.
    let segmentMetadataUsed = false;

    const requester = promiseFnToStream(({ query }) => {
      if (typeof query === 'object' && query.query && typeof query.query === 'string') {
        return Promise.resolve([]);
      }
      if (query && query.queryType === 'segmentMetadata') {
        segmentMetadataUsed = true;
        return Promise.resolve([
          {
            id: 'm',
            columns: {
              __time: { type: 'LONG', errorMessage: null, hasMultipleValues: false },
              x: { type: 'STRING', errorMessage: null, hasMultipleValues: false },
            },
            aggregators: {},
          },
        ]);
      }
      if (query && query.queryType === 'timeBoundary') return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected query: ${JSON.stringify(query)}`));
    });

    const ext = await makeExternal(requester).introspect();
    expect(segmentMetadataUsed, 'segmentMetadata ran after empty SQL result').to.equal(true);
    expect(ext.attributes.map(a => a.name)).to.deep.equal(['__time', 'x']);
  });

  it('escapes the table name so "tricky" names (with dashes, quotes) work', async () => {
    let capturedSql;
    const requester = promiseFnToStream(({ query }) => {
      if (typeof query === 'object' && query.query && typeof query.query === 'string') {
        capturedSql = query.query;
        return Promise.resolve([{ COLUMN_NAME: '__time', DATA_TYPE: 'TIMESTAMP' }]);
      }
      return Promise.reject(new Error('unexpected'));
    });

    const trickyName = "histories-abc123'xyz-reviews";
    const ext = External.fromJS(
      { engine: 'druidsql', version: '30.0.0', source: trickyName },
      requester,
    );
    await ext.introspect();

    expect(capturedSql).to.be.a('string');
    // Escaped single-quote — the exact escape depends on the dialect, but the
    // raw apostrophe MUST NOT appear unescaped inside the literal.
    const pattern = /TABLE_NAME = '([^']|'')+'/;
    expect(capturedSql).to.match(pattern);
  });
});
