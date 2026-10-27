/*
 * Copyright 2015-2020 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require('chai');
const { Duration } = require('chronoshift');
const { sane } = require('../utils');

const { Compute } = require('google-auth-library');

const plywood = require('../plywood');

const {
  External,
  BigQueryExternal,
  TimeRange,
  NumberRange,
  $,
  s$,
  ply,
  r,
  basicExecutorFactory,
  verboseRequesterFactory,
  Expression,
} = plywood;

const info = require('../info');

// The requester is not published yet: point BIGQUERY_REQUESTER at a build of plywood-bigquery-requester
const { bigQueryRequesterFactory } = require(info.bigQueryRequester);

const bigQueryRequester = bigQueryRequesterFactory({
  projectId: info.bigQueryProject,
  datasetId: info.bigQueryDataset,
  location: info.bigQueryLocation,
  authClient: new Compute(),
});

// bigQueryRequester = verboseRequesterFactory({
//   requester: bigQueryRequester,
// });

// Doubles summed in another order differ in the last digits; groups tied on the sort value at a
// LIMIT may be any of the tied ones on either engine
function expectSameRows(actual, expected, { tiedOn } = {}) {
  expect(actual).to.have.length(expected.length);
  actual.forEach((row, i) => {
    for (const key of Object.keys(expected[i])) {
      if (tiedOn && !tiedOn.includes(key)) continue;
      const want = expected[i][key];
      if (typeof want === 'number' && !Number.isInteger(want)) {
        expect(row[key]).to.be.closeTo(want, Math.abs(want) * 1e-12);
      } else {
        expect(row[key]).to.deep.equal(want);
      }
    }
  });
}

const context = {
  maximumBytesBilled: '500000000',
  labels: { suite: 'plywood-functional' },
  lookupTables: { 'channel-lookup': info.bigQueryChannelLookup },
};

describe('BigQuery Functional', function () {
  this.timeout(10000);

  const wikiAttributes = [
    {
      name: '__time',
      nativeType: '__time',
      range: {
        bounds: '[]',
        end: new Date('2015-09-12T23:59:00.000Z'),
        start: new Date('2015-09-12T00:46:00.000Z'),
      },
      type: 'TIME',
    },
    {
      name: 'sometimeLater',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'sometimeLaterMs',
      nativeType: 'LONG',
      type: 'NUMBER',
    },
    {
      name: 'channel',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'cityName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'comment',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'commentTerms',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'commentLength',
      nativeType: 'LONG',
      type: 'NUMBER',
    },
    {
      name: 'commentLengthStr',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'countryIsoCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'countryName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'deltaBucket100',
      nativeType: 'FLOAT',
      type: 'NUMBER',
    },
    {
      name: 'isAnonymous',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isMinor',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isNew',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isRobot',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isUnpatrolled',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'metroCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'namespace',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'page',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'regionIsoCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'regionName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'geohash',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'user',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'userChars',
      nativeType: 'ARRAY<STRUCT<v STRING>>',
      type: 'SET/STRING',
    },
    {
      maker: {
        expression: {
          name: 'added',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'added',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      maker: {
        op: 'count',
      },
      name: 'count',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      maker: {
        expression: {
          name: 'deleted',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'deleted',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      maker: {
        expression: {
          name: 'delta',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'delta',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      maker: {
        expression: {
          name: 'deltaByTen',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'deltaByTen',
      nativeType: 'DOUBLE',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      name: 'delta_hist',
      nativeType: 'approximateHistogram',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'delta_quantilesDoublesSketch',
      nativeType: 'quantilesDoublesSketch',
      type: 'NULL',
      unsplitable: true,
    },
    {
      maker: {
        expression: {
          name: 'max_delta',
          op: 'ref',
        },
        op: 'max',
      },
      name: 'max_delta',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      maker: {
        expression: {
          name: 'min_delta',
          op: 'ref',
        },
        op: 'min',
      },
      name: 'min_delta',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      name: 'page_unique',
      nativeType: 'hyperUnique',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'user_hll',
      nativeType: 'HLLSketch',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'user_theta',
      nativeType: 'thetaSketch',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'user_unique',
      nativeType: 'hyperUnique',
      type: 'NULL',
      unsplitable: true,
    },
  ];

  const wikiDerivedAttributes = {
    pageInBrackets: "'[' ++ $page ++ ']'",
  };

  describe('source list', () => {
    it('does a source list', async () => {
      expect(await BigQueryExternal.getSourceList(bigQueryRequester)).to.include(
        info.bigQueryWikipedia.split('.').pop(),
      );
    });
  });

  describe('custom SQL', () => {
    const basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'bigquery',
            source: info.bigQueryWikipedia,
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
          },
          bigQueryRequester,
        ),
      },
    });

    it('works in simple aggregate case', () => {
      const ex = $('wiki')
        .split(s$(`CONCAT(channel, '~')`), 'Channel')
        .apply('Count', $('wiki').sqlAggregate(r(`SUM(t."count")`)))
        .apply('Fancy', $('wiki').sqlAggregate(r(`SQRT(SUM(t."added" * t."added"))`)))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en~',
            Count: 114711,
            Fancy: 717274.3671253002,
          },
          {
            Channel: 'vi~',
            Count: 99010,
            Fancy: 70972.1877005352,
          },
          {
            Channel: 'de~',
            Count: 25103,
            Fancy: 284404.77477356105,
          },
        ]);
      });
    });

    it('works with negation', () => {
      const ex = $('wiki')
        .filter($('cityName').isnt('Tokyo'))
        .split($('cityName'), 'City')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS()).to.deep.equal({
          attributes: [
            {
              name: 'City',
              type: 'STRING',
            },
            {
              name: 'Count',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              City: null,
              Count: 371151,
            },
            {
              City: 'Central District',
              Count: 425,
            },
            {
              City: 'Moscow',
              Count: 330,
            },
          ],
          keys: ['City'],
        });
      });
    });

    it('can do compare column', () => {
      const prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T12:00:00Z'),
      });
      const mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      const ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply(
          'CountPrev',
          $('wiki').filter($('__time').overlap(prevRange)).sqlAggregate(r(`SUM(t."count")`)),
        )
        .apply(
          'CountMain',
          $('wiki').filter($('__time').overlap(mainRange)).sqlAggregate(r(`SUM(t."count")`)),
        )
        .sort($('CountMain'), 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            CountMain: 68606,
            CountPrev: 46105,
          },
          {
            Channel: 'vi',
            CountMain: 48521,
            CountPrev: 50489,
          },
          {
            Channel: 'de',
            CountMain: 15857,
            CountPrev: 9246,
          },
          {
            Channel: 'fr',
            CountMain: 14779,
            CountPrev: 6506,
          },
          {
            Channel: 'uz',
            CountMain: 10064,
            CountPrev: 8,
          },
        ]);
      });
    });
  });

  describe('custom SQL with WITH', () => {
    const basicExecutor = basicExecutorFactory({
      datasets: {
        wikiWith: External.fromJS(
          {
            engine: 'bigquery',
            source: 'wikipedia_zzz',
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
            withQuery: `SELECT *, CONCAT(channel, '-lol') AS channelLol FROM \`${info.bigQueryWikipedia}\` WHERE channel = 'en'`,
          },
          bigQueryRequester,
        ),
      },
    });

    it('works in simple aggregate case', () => {
      const ex = $('wikiWith')
        .split(s$(`t.channelLol`), 'ChannelLol')
        .apply('Count', $('wikiWith').sqlAggregate(r(`SUM(t."count")`)))
        .apply('Fancy', $('wikiWith').sqlAggregate(r(`SQRT(SUM(t."added" * t."added"))`)))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            ChannelLol: 'en-lol',
            Count: 114711,
            Fancy: 717274.3671253002,
          },
        ]);
      });
    });
  });

  describe('defined attributes in datasource', () => {
    const basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'bigquery',
            source: info.bigQueryWikipedia,
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
          },
          bigQueryRequester,
        ),
      },
    });

    describe('decomposition shortcuts', () => {
      it('can do compare column', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T12:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split($('channel'), 'Channel')
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .sort($('CountMain'), 'descending')
          .limit(5);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              Channel: 'en',
              CountMain: 68606,
              CountPrev: 46105,
            },
            {
              Channel: 'vi',
              CountMain: 48521,
              CountPrev: 50489,
            },
            {
              Channel: 'de',
              CountMain: 15857,
              CountPrev: 9246,
            },
            {
              Channel: 'fr',
              CountMain: 14779,
              CountPrev: 6506,
            },
            {
              Channel: 'uz',
              CountMain: 10064,
              CountPrev: 8,
            },
          ]);
        });
      });

      it('can do compare column with having', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T12:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split($('channel'), 'Channel')
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .sort($('CountMain'), 'descending')
          .filter($('CountMain').greaterThan(48520))
          .limit(5);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              Channel: 'en',
              CountMain: 68606,
              CountPrev: 46105,
            },
            {
              Channel: 'vi',
              CountMain: 48521,
              CountPrev: 50489,
            },
          ]);
        });
      });

      it('can timeBucket on joined column', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T12:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT12H')))
              .timeBucket('PT2H'),
            'TimeJoin',
          )
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'));

        return basicExecutor(ex).then(result => {
          expect(result.toJS()).to.deep.equal({
            attributes: [
              {
                name: 'TimeJoin',
                type: 'TIME_RANGE',
              },
              {
                name: 'CountPrev',
                type: 'NUMBER',
              },
              {
                name: 'CountMain',
                type: 'NUMBER',
              },
            ],
            data: [
              {
                CountMain: 37816,
                CountPrev: 14123,
                TimeJoin: {
                  end: new Date('2015-09-12T14:00:00.000Z'),
                  start: new Date('2015-09-12T12:00:00.000Z'),
                },
              },
              {
                CountMain: 38388,
                CountPrev: 19168,
                TimeJoin: {
                  end: new Date('2015-09-12T16:00:00.000Z'),
                  start: new Date('2015-09-12T14:00:00.000Z'),
                },
              },
              {
                CountMain: 42589,
                CountPrev: 20848,
                TimeJoin: {
                  end: new Date('2015-09-12T18:00:00.000Z'),
                  start: new Date('2015-09-12T16:00:00.000Z'),
                },
              },
              {
                CountMain: 41828,
                CountPrev: 43567,
                TimeJoin: {
                  end: new Date('2015-09-12T20:00:00.000Z'),
                  start: new Date('2015-09-12T18:00:00.000Z'),
                },
              },
              {
                CountMain: 35977,
                CountPrev: 33259,
                TimeJoin: {
                  end: new Date('2015-09-12T22:00:00.000Z'),
                  start: new Date('2015-09-12T20:00:00.000Z'),
                },
              },
              {
                CountMain: 30720,
                CountPrev: 34160,
                TimeJoin: {
                  end: new Date('2015-09-13T00:00:00.000Z'),
                  start: new Date('2015-09-12T22:00:00.000Z'),
                },
              },
            ],
            keys: ['TimeJoin'],
          });
        });
      });

      it('can timeBucket on joined column with limit', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T12:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT12H')))
              .timeBucket('PT2H'),
            'TimeJoin',
          )
          .apply('CountAll', $('wiki').sum('$count'))
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .limit(3);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              CountAll: 51939,
              CountMain: 37816,
              CountPrev: 14123,
              TimeJoin: {
                end: new Date('2015-09-12T14:00:00.000Z'),
                start: new Date('2015-09-12T12:00:00.000Z'),
              },
            },
            {
              CountAll: 57556,
              CountMain: 38388,
              CountPrev: 19168,
              TimeJoin: {
                end: new Date('2015-09-12T16:00:00.000Z'),
                start: new Date('2015-09-12T14:00:00.000Z'),
              },
            },
            {
              CountAll: 63437,
              CountMain: 42589,
              CountPrev: 20848,
              TimeJoin: {
                end: new Date('2015-09-12T18:00:00.000Z'),
                start: new Date('2015-09-12T16:00:00.000Z'),
              },
            },
          ]);
        });
      });

      it('can timeBucket on joined and overlapping column with limit', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T22:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T02:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT2H')))
              .timeBucket('PT1H'),
            'TimeJoin',
          )
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .limit(6);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              CountMain: 11020,
              CountPrev: 2681,
              TimeJoin: {
                end: new Date('2015-09-12T03:00:00.000Z'),
                start: new Date('2015-09-12T02:00:00.000Z'),
              },
            },
            {
              CountMain: 8148,
              CountPrev: 11442,
              TimeJoin: {
                end: new Date('2015-09-12T04:00:00.000Z'),
                start: new Date('2015-09-12T03:00:00.000Z'),
              },
            },
            {
              CountMain: 8240,
              CountPrev: 11020,
              TimeJoin: {
                end: new Date('2015-09-12T05:00:00.000Z'),
                start: new Date('2015-09-12T04:00:00.000Z'),
              },
            },
            {
              CountMain: 12608,
              CountPrev: 8148,
              TimeJoin: {
                end: new Date('2015-09-12T06:00:00.000Z'),
                start: new Date('2015-09-12T05:00:00.000Z'),
              },
            },
            {
              CountMain: 21194,
              CountPrev: 8240,
              TimeJoin: {
                end: new Date('2015-09-12T07:00:00.000Z'),
                start: new Date('2015-09-12T06:00:00.000Z'),
              },
            },
            {
              CountMain: 22373,
              CountPrev: 12608,
              TimeJoin: {
                end: new Date('2015-09-12T08:00:00.000Z'),
                start: new Date('2015-09-12T07:00:00.000Z'),
              },
            },
          ]);
        });
      });

      it('can timeBucket on joined and overlapping column with multiple splits and limit', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T22:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T02:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split({
            TimeJoin: $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT2H')))
              .timeBucket('PT1H'),
            isRobot: $('isRobot').is(r('true')),
          })
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .limit(6);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              CountMain: 5982,
              CountPrev: 1557,
              TimeJoin: {
                end: new Date('2015-09-12T03:00:00.000Z'),
                start: new Date('2015-09-12T02:00:00.000Z'),
              },
              isRobot: false,
            },
            {
              CountMain: 5038,
              CountPrev: 1124,
              TimeJoin: {
                end: new Date('2015-09-12T03:00:00.000Z'),
                start: new Date('2015-09-12T02:00:00.000Z'),
              },
              isRobot: true,
            },
            {
              CountMain: 5915,
              CountPrev: 6566,
              TimeJoin: {
                end: new Date('2015-09-12T04:00:00.000Z'),
                start: new Date('2015-09-12T03:00:00.000Z'),
              },
              isRobot: false,
            },
            {
              CountMain: 2233,
              CountPrev: 4876,
              TimeJoin: {
                end: new Date('2015-09-12T04:00:00.000Z'),
                start: new Date('2015-09-12T03:00:00.000Z'),
              },
              isRobot: true,
            },
            {
              CountMain: 6098,
              CountPrev: 5982,
              TimeJoin: {
                end: new Date('2015-09-12T05:00:00.000Z'),
                start: new Date('2015-09-12T04:00:00.000Z'),
              },
              isRobot: false,
            },
            {
              CountMain: 2142,
              CountPrev: 5038,
              TimeJoin: {
                end: new Date('2015-09-12T05:00:00.000Z'),
                start: new Date('2015-09-12T04:00:00.000Z'),
              },
              isRobot: true,
            },
          ]);
        });
      });

      it('can timeBucket on joined and overlapping column with limit and sort', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T22:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T02:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT2H')))
              .timeBucket('PT1H'),
            'TimeJoin',
          )
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .sort('$CountMain', 'descending')
          .limit(6);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              CountMain: 23001,
              CountPrev: 19655,
              TimeJoin: {
                end: new Date('2015-09-12T18:00:00.000Z'),
                start: new Date('2015-09-12T17:00:00.000Z'),
              },
            },
            {
              CountMain: 22373,
              CountPrev: 12608,
              TimeJoin: {
                end: new Date('2015-09-12T08:00:00.000Z'),
                start: new Date('2015-09-12T07:00:00.000Z'),
              },
            },
            {
              CountMain: 21699,
              CountPrev: 19588,
              TimeJoin: {
                end: new Date('2015-09-12T19:00:00.000Z'),
                start: new Date('2015-09-12T18:00:00.000Z'),
              },
            },
            {
              CountMain: 21194,
              CountPrev: 8240,
              TimeJoin: {
                end: new Date('2015-09-12T07:00:00.000Z'),
                start: new Date('2015-09-12T06:00:00.000Z'),
              },
            },
            {
              CountMain: 20725,
              CountPrev: 16240,
              TimeJoin: {
                end: new Date('2015-09-12T14:00:00.000Z'),
                start: new Date('2015-09-12T13:00:00.000Z'),
              },
            },
            {
              CountMain: 20129,
              CountPrev: 23001,
              TimeJoin: {
                end: new Date('2015-09-12T20:00:00.000Z'),
                start: new Date('2015-09-12T19:00:00.000Z'),
              },
            },
          ]);
        });
      });

      it('works with sql nested aggregate', () => {
        const ex = $('wiki')
          .split('$channel', 'channel')
          .apply(
            'qq',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(TIME_FLOOR(t.__time, \'PT1H\'),COUNT(t.channel) AS "V", SUM(t."V"))`,
              ),
            ),
          )
          .sort('$channel', 'descending')
          .limit(5);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              channel: 'zh',
              qq: 10776,
            },
            {
              channel: 'war',
              qq: 11,
            },
            {
              channel: 'vi',
              qq: 99002,
            },
            {
              channel: 'uz',
              qq: 10072,
            },
            {
              channel: 'uk',
              qq: 2280,
            },
          ]);
        });
      });

      it('can timeBucket on joined column with sub-split', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T12:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT12H')))
              .timeBucket('PT2H'),
            'TimeJoin',
          )
          .apply('CountAll', $('wiki').sum('$count'))
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .limit(2)
          .apply(
            'Channels',
            $('wiki')
              .split('$channel', 'Channel')
              .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
              .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
              .sort('$CountMain', 'descending')
              .limit(2),
          );

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              Channels: {
                attributes: [
                  {
                    name: 'Channel',
                    type: 'STRING',
                  },
                  {
                    name: 'CountPrev',
                    type: 'NUMBER',
                  },
                  {
                    name: 'CountMain',
                    type: 'NUMBER',
                  },
                ],
                data: [
                  {
                    Channel: 'en',
                    CountMain: 10698,
                    CountPrev: 5906,
                  },
                  {
                    Channel: 'vi',
                    CountMain: 7650,
                    CountPrev: 3771,
                  },
                ],
                keys: ['Channel'],
              },
              CountAll: 51939,
              CountMain: 37816,
              CountPrev: 14123,
              TimeJoin: {
                end: new Date('2015-09-12T14:00:00.000Z'),
                start: new Date('2015-09-12T12:00:00.000Z'),
              },
            },
            {
              Channels: {
                attributes: [
                  {
                    name: 'Channel',
                    type: 'STRING',
                  },
                  {
                    name: 'CountPrev',
                    type: 'NUMBER',
                  },
                  {
                    name: 'CountMain',
                    type: 'NUMBER',
                  },
                ],
                data: [
                  {
                    Channel: 'en',
                    CountMain: 10844,
                    CountPrev: 9439,
                  },
                  {
                    Channel: 'vi',
                    CountMain: 9258,
                    CountPrev: 2969,
                  },
                ],
                keys: ['Channel'],
              },
              CountAll: 57556,
              CountMain: 38388,
              CountPrev: 19168,
              TimeJoin: {
                end: new Date('2015-09-12T16:00:00.000Z'),
                start: new Date('2015-09-12T14:00:00.000Z'),
              },
            },
          ]);
        });
      });

      it('can timeBucket on joined column (sort by delta)', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T00:00:00Z'),
          end: new Date('2015-09-12T12:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT12H')))
              .timeBucket('PT2H'),
            'TimeJoin',
          )
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'))
          .apply('Delta', '$CountMain - $CountPrev')
          .sort('$Delta', 'descending');

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              CountMain: 37816,
              CountPrev: 14123,
              Delta: 23693,
              TimeJoin: {
                end: new Date('2015-09-12T14:00:00.000Z'),
                start: new Date('2015-09-12T12:00:00.000Z'),
              },
            },
            {
              CountMain: 42589,
              CountPrev: 20848,
              Delta: 21741,
              TimeJoin: {
                end: new Date('2015-09-12T18:00:00.000Z'),
                start: new Date('2015-09-12T16:00:00.000Z'),
              },
            },
            {
              CountMain: 38388,
              CountPrev: 19168,
              Delta: 19220,
              TimeJoin: {
                end: new Date('2015-09-12T16:00:00.000Z'),
                start: new Date('2015-09-12T14:00:00.000Z'),
              },
            },
            {
              CountMain: 35977,
              CountPrev: 33259,
              Delta: 2718,
              TimeJoin: {
                end: new Date('2015-09-12T22:00:00.000Z'),
                start: new Date('2015-09-12T20:00:00.000Z'),
              },
            },
            {
              CountMain: 41828,
              CountPrev: 43567,
              Delta: -1739,
              TimeJoin: {
                end: new Date('2015-09-12T20:00:00.000Z'),
                start: new Date('2015-09-12T18:00:00.000Z'),
              },
            },
            {
              CountMain: 30720,
              CountPrev: 34160,
              Delta: -3440,
              TimeJoin: {
                end: new Date('2015-09-13T00:00:00.000Z'),
                start: new Date('2015-09-12T22:00:00.000Z'),
              },
            },
          ]);
        });
      });

      it('can timeBucket on joined column (overlap time)', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T06:00:00Z'),
          end: new Date('2015-09-12T18:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            $('__time')
              .overlap(mainRange)
              .then($('__time'))
              .fallback($('__time').timeShift(Duration.fromJS('PT6H')))
              .timeBucket('PT2H'),
            'TimeJoin',
          )
          .apply('CountPrev', $('wiki').filter($('__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter($('__time').overlap(mainRange)).sum('$count'));

        return basicExecutor(ex).then(result => {
          expect(result.toJS()).to.deep.equal({
            attributes: [
              {
                name: 'TimeJoin',
                type: 'TIME_RANGE',
              },
              {
                name: 'CountPrev',
                type: 'NUMBER',
              },
              {
                name: 'CountMain',
                type: 'NUMBER',
              },
            ],
            data: [
              {
                CountMain: 37816,
                CountPrev: 43567,
                TimeJoin: {
                  end: new Date('2015-09-12T14:00:00.000Z'),
                  start: new Date('2015-09-12T12:00:00.000Z'),
                },
              },
              {
                CountMain: 38388,
                CountPrev: 33259,
                TimeJoin: {
                  end: new Date('2015-09-12T16:00:00.000Z'),
                  start: new Date('2015-09-12T14:00:00.000Z'),
                },
              },
              {
                CountMain: 42589,
                CountPrev: 34160,
                TimeJoin: {
                  end: new Date('2015-09-12T18:00:00.000Z'),
                  start: new Date('2015-09-12T16:00:00.000Z'),
                },
              },
              {
                CountMain: 41828,
                CountPrev: 37816,
                TimeJoin: {
                  end: new Date('2015-09-12T20:00:00.000Z'),
                  start: new Date('2015-09-12T18:00:00.000Z'),
                },
              },
              {
                CountMain: 35977,
                CountPrev: 38388,
                TimeJoin: {
                  end: new Date('2015-09-12T22:00:00.000Z'),
                  start: new Date('2015-09-12T20:00:00.000Z'),
                },
              },
              {
                CountMain: 30720,
                CountPrev: 42589,
                TimeJoin: {
                  end: new Date('2015-09-13T00:00:00.000Z'),
                  start: new Date('2015-09-12T22:00:00.000Z'),
                },
              },
            ],
            keys: ['TimeJoin'],
          });
        });
      });

      it('can timeBucket on joined column (overlap time, SQL)', () => {
        const prevRange = TimeRange.fromJS({
          start: new Date('2015-09-12T06:00:00Z'),
          end: new Date('2015-09-12T18:00:00Z'),
        });
        const mainRange = TimeRange.fromJS({
          start: new Date('2015-09-12T12:00:00Z'),
          end: new Date('2015-09-13T00:00:00Z'),
        });
        const ex = $('wiki')
          .split(
            s$('t.__time')
              .overlap(mainRange)
              .then(s$('t.__time'))
              .fallback(s$('t.__time').timeShift(Duration.fromJS('PT6H')))
              .timeBucket('PT2H'),
            'TimeJoin',
          )
          .apply('CountPrev', $('wiki').filter(s$('t.__time').overlap(prevRange)).sum('$count'))
          .apply('CountMain', $('wiki').filter(s$('t.__time').overlap(mainRange)).sum('$count'));

        return basicExecutor(ex).then(result => {
          expect(result.toJS()).to.deep.equal({
            attributes: [
              {
                name: 'TimeJoin',
                type: 'TIME_RANGE',
              },
              {
                name: 'CountPrev',
                type: 'NUMBER',
              },
              {
                name: 'CountMain',
                type: 'NUMBER',
              },
            ],
            data: [
              {
                CountMain: 37816,
                CountPrev: 43567,
                TimeJoin: {
                  end: new Date('2015-09-12T14:00:00.000Z'),
                  start: new Date('2015-09-12T12:00:00.000Z'),
                },
              },
              {
                CountMain: 38388,
                CountPrev: 33259,
                TimeJoin: {
                  end: new Date('2015-09-12T16:00:00.000Z'),
                  start: new Date('2015-09-12T14:00:00.000Z'),
                },
              },
              {
                CountMain: 42589,
                CountPrev: 34160,
                TimeJoin: {
                  end: new Date('2015-09-12T18:00:00.000Z'),
                  start: new Date('2015-09-12T16:00:00.000Z'),
                },
              },
              {
                CountMain: 41828,
                CountPrev: 37816,
                TimeJoin: {
                  end: new Date('2015-09-12T20:00:00.000Z'),
                  start: new Date('2015-09-12T18:00:00.000Z'),
                },
              },
              {
                CountMain: 35977,
                CountPrev: 38388,
                TimeJoin: {
                  end: new Date('2015-09-12T22:00:00.000Z'),
                  start: new Date('2015-09-12T20:00:00.000Z'),
                },
              },
              {
                CountMain: 30720,
                CountPrev: 42589,
                TimeJoin: {
                  end: new Date('2015-09-13T00:00:00.000Z'),
                  start: new Date('2015-09-12T22:00:00.000Z'),
                },
              },
            ],
            keys: ['TimeJoin'],
          });
        });
      });
    });

    it('works in simple case', () => {
      const ex = $('wiki')
        .split('$channel', 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            Count: 114711,
          },
          {
            Channel: 'vi',
            Count: 99010,
          },
          {
            Channel: 'de',
            Count: 25103,
          },
        ]);
      });
    });

    it('works in advanced case', () => {
      const ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Namespaces',
          $('wiki')
            .split('$namespace', 'Namespace')
            .apply('Added', '$wiki.sum($added)')
            .sort('$Added', 'descending')
            .limit(2)
            .apply(
              'Time',
              $('wiki')
                .split($('__time').timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(3),
            ),
        );
      // .apply(
      //   'PagesHaving',
      //   $("wiki").split("$page", 'Page')
      //     .apply('Count', '$wiki.sum($count)')
      //     .sort('$Count', 'descending')
      //     .filter($('Count').lessThan(30))
      //     .limit(3)
      // );

      const rawQueries = [];
      return basicExecutor(ex, { rawQueries }).then(result => {
        expect(rawQueries).to.deep.equal([
          {
            engine: 'bigquery',
            query: {
              labels: { suite: 'plywood-functional' },
              maximumBytesBilled: '500000000',
              query: `SELECT\nSUM(\`count\`) AS \`Count\`,\nSUM(\`added\`) AS \`TotalAdded\`\nFROM \`${info.bigQueryWikipedia}\` AS t\nWHERE (\`channel\`='en')\nGROUP BY ()`,
            },
          },
          {
            engine: 'bigquery',
            query: {
              labels: { suite: 'plywood-functional' },
              maximumBytesBilled: '500000000',
              query: `SELECT\n\`namespace\` AS \`Namespace\`,\nSUM(\`added\`) AS \`Added\`\nFROM \`${info.bigQueryWikipedia}\` AS t\nWHERE (\`channel\`='en')\nGROUP BY 1\nORDER BY IS_NAN(\`Added\`) IS TRUE DESC, \`Added\` DESC\nLIMIT 2`,
            },
          },
          {
            engine: 'bigquery',
            query: {
              labels: { suite: 'plywood-functional' },
              maximumBytesBilled: '500000000',
              query: `SELECT\nTIMESTAMP_TRUNC(\`__time\`, HOUR) AS \`Timestamp\`,\nSUM(\`added\`) AS \`TotalAdded\`\nFROM \`${info.bigQueryWikipedia}\` AS t\nWHERE ((\`channel\`='en') AND (\`namespace\`='Main'))\nGROUP BY 1\nORDER BY IS_NAN(\`TotalAdded\`) IS TRUE DESC, \`TotalAdded\` DESC\nLIMIT 3`,
            },
          },
          {
            engine: 'bigquery',
            query: {
              labels: { suite: 'plywood-functional' },
              maximumBytesBilled: '500000000',
              query: `SELECT\nTIMESTAMP_TRUNC(\`__time\`, HOUR) AS \`Timestamp\`,\nSUM(\`added\`) AS \`TotalAdded\`\nFROM \`${info.bigQueryWikipedia}\` AS t\nWHERE ((\`channel\`='en') AND (\`namespace\`='User talk'))\nGROUP BY 1\nORDER BY IS_NAN(\`TotalAdded\`) IS TRUE DESC, \`TotalAdded\` DESC\nLIMIT 3`,
            },
          },
        ]);

        expect(result.toJS().data).to.deep.equal([
          {
            Count: 114711,
            Namespaces: {
              attributes: [
                {
                  name: 'Namespace',
                  type: 'STRING',
                },
                {
                  name: 'Added',
                  type: 'NUMBER',
                },
                {
                  name: 'Time',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Added: 11594002,
                  Namespace: 'Main',
                  Time: {
                    attributes: [
                      {
                        name: 'Timestamp',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'TotalAdded',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T15:00:00.000Z'),
                          start: new Date('2015-09-12T14:00:00.000Z'),
                        },
                        TotalAdded: 740968,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T19:00:00.000Z'),
                          start: new Date('2015-09-12T18:00:00.000Z'),
                        },
                        TotalAdded: 739956,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T23:00:00.000Z'),
                          start: new Date('2015-09-12T22:00:00.000Z'),
                        },
                        TotalAdded: 708543,
                      },
                    ],
                    keys: ['Timestamp'],
                  },
                },
                {
                  Added: 9210976,
                  Namespace: 'User talk',
                  Time: {
                    attributes: [
                      {
                        name: 'Timestamp',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'TotalAdded',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T13:00:00.000Z'),
                          start: new Date('2015-09-12T12:00:00.000Z'),
                        },
                        TotalAdded: 693571,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T18:00:00.000Z'),
                          start: new Date('2015-09-12T17:00:00.000Z'),
                        },
                        TotalAdded: 634804,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T03:00:00.000Z'),
                          start: new Date('2015-09-12T02:00:00.000Z'),
                        },
                        TotalAdded: 573768,
                      },
                    ],
                    keys: ['Timestamp'],
                  },
                },
              ],
              keys: ['Namespace'],
            },
            TotalAdded: 32553107,
          },
        ]);
      });
    });

    it('works with all kinds of cool aggregates on totals level', () => {
      const ex = ply()
        .apply('NumPages', $('wiki').countDistinct('$page'))
        .apply('NumEnPages', $('wiki').filter($('channel').is('en')).countDistinct('$page'))
        .apply('ChannelAdded', $('wiki').sum('$added'))
        .apply('ChannelENAdded', $('wiki').filter($('channel').is('en')).sum('$added'))
        .apply('ChannelENishAdded', $('wiki').filter($('channel').contains('en')).sum('$added'))
        .apply('Count', $('wiki').sum('$count'))
        .apply('CountSquareRoot', $('wiki').sum('$count').power(0.5))
        .apply('CountSquared', $('wiki').sum('$count').power(2))
        .apply('One', $('wiki').sum('$count').power(0))
        .apply('AddedByDeleted', $('wiki').sum('$added').divide($('wiki').sum('$deleted')))
        .apply('Delta95th', $('wiki').quantile('$delta', 0.95))
        .apply('Delta99thX2', $('wiki').quantile('$delta', 0.99).multiply(2));
      // .apply(
      //   'Delta98thEn',
      //   $('wiki')
      //     .filter($('channel').is('en'))
      //     .quantile('$delta_hist', 0.98),
      // )
      // .apply(
      //   'Delta98thDe',
      //   $('wiki')
      //     .filter($('channel').is('de'))
      //     .quantile('$delta_hist', 0.98),
      // );

      return basicExecutor(ex).then(result => {
        const data = result.toJS().data;
        // Quantiles are approximate: compared with the exact percentiles of the same rows (956 and 4207;
        // Druid's APPROX_QUANTILE_DS gives 945 and 4483)
        expect(data[0].Delta95th).to.be.closeTo(956, 956 * 0.02);
        expect(data[0].Delta99thX2).to.be.closeTo(2 * 4207, 2 * 4207 * 0.02);
        delete data[0].Delta95th;
        delete data[0].Delta99thX2;
        // Distinct counts are exact; the Druid figures are its exact counts (GROUP BY) on the same rows
        expect(data).to.deep.equal([
          {
            AddedByDeleted: 24.909643797343193,
            ChannelAdded: 97393743,
            ChannelENAdded: 32553107,
            ChannelENishAdded: 32553107,
            Count: 392443,
            CountSquareRoot: 626.4527117029664,
            CountSquared: 154011508249,
            NumEnPages: 65762,
            NumPages: 279893,
            One: 1,
          },
        ]);
      });
    });

    it.skip('works with boolean GROUP BYs', () => {
      const ex = $('wiki')
        .split($('channel').is('en'), 'ChannelIsEn')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            ChannelIsEn: false,
            Count: 277732,
          },
          {
            ChannelIsEn: true,
            Count: 114711,
          },
        ]);
      });
    });

    it('works string range', () => {
      const ex = $('wiki')
        .filter($('cityName').greaterThan('Eagleton'))
        .split('$cityName', 'CityName')
        .sort('$CityName', 'descending')
        .limit(10);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CityName: 'Ōita',
          },
          {
            CityName: 'Łódź',
          },
          {
            CityName: 'İzmit',
          },
          {
            CityName: 'České Budějovice',
          },
          {
            CityName: 'Ürümqi',
          },
          {
            CityName: 'Ústí nad Labem',
          },
          {
            CityName: 'Évry',
          },
          {
            CityName: 'Épinay-sur-Seine',
          },
          {
            CityName: 'Épernay',
          },
          {
            CityName: 'Élancourt',
          },
        ]);
      });
    });

    it('works with fancy lookup filter', () => {
      const ex = ply()
        .apply(
          'wiki',
          $('wiki').filter(
            $('channel')
              .lookup('channel-lookup')
              .fallback('"???"')
              .concat(r(' ('), '$channel', r(')'))
              .in(['English (en)', 'German (de)']),
          ),
        )
        .apply('Count', '$wiki.sum($count)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 114711,
          },
        ]);
      });
    });

    it('works with non __time time expression', () => {
      const ex = $('wiki')
        .split(s$('MILLIS_TO_TIMESTAMP(t.added)').timeBucket('PT1S', 'Etc/UTC'), 'addedAsTime')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(2);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 373662,
            addedAsTime: {
              end: new Date('1970-01-01T00:00:01.000Z'),
              start: new Date('1970-01-01T00:00:00.000Z'),
            },
          },
          {
            Count: 9744,
            addedAsTime: {
              end: new Date('1970-01-01T00:00:02.000Z'),
              start: new Date('1970-01-01T00:00:01.000Z'),
            },
          },
        ]);
      });
    });

    it('works with cast', () => {
      const ex = $('wiki')
        .split(s$('commentLengthStr').cast('NUMBER'), 'CommentLength')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CommentLength: 13,
            Count: 42013,
          },
          {
            CommentLength: 34,
            Count: 24469,
          },
          {
            CommentLength: 29,
            Count: 22526,
          },
        ]);
      });
    });
  });

  describe('incorrect commentLength and comment', () => {
    const wikiUserCharAsNumber = External.fromJS(
      {
        engine: 'bigquery',
        source: info.bigQueryWikipedia,
        timeAttribute: 'time',
        allowEternity: true,
        context,
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'comment', type: 'STRING' },
          { name: 'page', type: 'NUMBER' }, // This is incorrect
          { name: 'count', type: 'NUMBER', unsplitable: true },
        ],
      },
      bigQueryRequester,
    );
  });

  describe('introspection', () => {
    const basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'bigquery',
            source: info.bigQueryWikipedia,
            context,
          },
          bigQueryRequester,
        ),
      },
    });

    it('introspects table', async () => {
      const external = await External.fromJS(
        {
          engine: 'bigquery',
          source: info.bigQueryWikipedia,
          context,
        },
        bigQueryRequester,
      ).introspect();

      expect(external.version).to.equal('2.0.0');
      expect(external.toJS().attributes).to.deep.equal([
        {
          name: '__time',
          type: 'TIME',
          nativeType: 'TIMESTAMP',
        },
        {
          name: 'sometimeLater',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'sometimeLaterMs',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'channel',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'cityName',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'comment',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'commentTerms',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'commentLength',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'commentLengthStr',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'countryIsoCode',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'countryName',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'deltaBucket100',
          type: 'NUMBER',
          nativeType: 'FLOAT64',
        },
        {
          name: 'isAnonymous',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'isMinor',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'isNew',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'isRobot',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'isUnpatrolled',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'metroCode',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'namespace',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'page',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'regionIsoCode',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'regionName',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'geohash',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'user',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'userChars',
          type: 'STRING',
          nativeType: 'ARRAY<STRUCT<v STRING>>',
        },
        {
          name: 'added',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'count',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'deleted',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'delta',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'deltaByTen',
          type: 'NUMBER',
          nativeType: 'FLOAT64',
        },
        {
          name: 'max_delta',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'min_delta',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
      ]);
    });

    it('introspects withQuery (no star)', async () => {
      const external = await External.fromJS(
        {
          engine: 'bigquery',
          source: info.bigQueryWikipedia,
          withQuery: `SELECT __time, page, user = 'vad' AS is_vad, COUNT(*) AS cnt FROM \`${info.bigQueryWikipedia}\` GROUP BY 1, 2, 3`,
          context,
        },
        bigQueryRequester,
      ).introspect();

      expect(external.toJS().attributes).to.deep.equal([
        {
          name: '__time',
          type: 'TIME',
          nativeType: 'TIMESTAMP',
        },
        {
          name: 'page',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'is_vad',
          type: 'BOOLEAN',
          nativeType: 'BOOL',
        },
        {
          name: 'cnt',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
      ]);
    });

    it('introspects withQuery (with star)', async () => {
      const external = await External.fromJS(
        {
          engine: 'bigquery',
          source: info.bigQueryWikipedia,
          withQuery: `SELECT CONCAT(page, 'lol') AS pageLol, added + 1 AS addedPlusOne, * FROM \`${info.bigQueryWikipedia}\``,
          context,
        },
        bigQueryRequester,
      ).introspect();

      expect(external.toJS().attributes.slice(0, 4)).to.deep.equal([
        {
          name: 'pageLol',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'addedPlusOne',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: '__time',
          type: 'TIME',
          nativeType: 'TIMESTAMP',
        },
        {
          name: 'sometimeLater',
          type: 'STRING',
          nativeType: 'STRING',
        },
      ]);
    });

    it('introspects withQuery (with group by, without __time)', async () => {
      const external = await External.fromJS(
        {
          engine: 'bigquery',
          source: info.bigQueryWikipedia,
          timeAttribute: '__time',
          withQuery: sane`
            SELECT
            channel,
               cityName,
               COUNT(*) AS \`Count\`
            FROM \`${info.bigQueryWikipedia}\`
            GROUP BY 1, 2
            ORDER BY 2 DESC
          `,
          context,
        },
        bigQueryRequester,
      ).introspect();

      expect(external.toJS().attributes).to.deep.equal([
        {
          name: 'channel',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'cityName',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'Count',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
      ]);
    });

    it('introspects withQuery (with join)', async () => {
      const external = await External.fromJS(
        {
          engine: 'bigquery',
          source: info.bigQueryWikipedia,
          withQuery: sane`
            SELECT
              __time,
              added,
              channel,
              lk.v AS channelName,
              user
            FROM \`${info.bigQueryWikipedia}\` AS wikipedia
            LEFT JOIN \`${info.bigQueryChannelLookup}\` AS lk ON lk.k = wikipedia.channel
          `,
          context,
        },
        bigQueryRequester,
      ).introspect();

      expect(external.toJS().attributes).to.deep.equal([
        {
          name: '__time',
          type: 'TIME',
          nativeType: 'TIMESTAMP',
        },
        {
          name: 'added',
          type: 'NUMBER',
          nativeType: 'INT64',
        },
        {
          name: 'channel',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'channelName',
          type: 'STRING',
          nativeType: 'STRING',
        },
        {
          name: 'user',
          type: 'STRING',
          nativeType: 'STRING',
        },
      ]);
    });

    // BigQuery has no sketch columns (hyperUnique, HLLSketch, thetaSketch): distinct counts are exact.
    // The expected figures are Druid's exact counts (GROUP BY) on the same rows.
    it('works with exact uniques', () => {
      const ex = ply()
        .apply('UniqueIsRobot', $('wiki').countDistinct('$isRobot'))
        .apply('UniquePages', $('wiki').countDistinct('$page'))
        .apply('UniqueUsers', $('wiki').countDistinct('$user'))
        .apply('UniqueChannels', $('wiki').countDistinct('$channel'));

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            UniqueChannels: 52,
            UniqueIsRobot: 2,
            UniquePages: 279893,
            UniqueUsers: 38234,
          },
        ]);
      });
    });

    // BigQuery has no histogram or quantile sketch columns: quantiles are APPROX_QUANTILES of the base
    // column. The reference figures are the exact percentiles (PERCENTILE_DISC) of the same rows; Druid's
    // APPROX_QUANTILE_DS gives 945, 18, 147, 29, 0, 2214 and 1972 on them (2026-10-27).
    it('works with quantiles (plain and filtered)', () => {
      const ex = ply()
        .apply('delta95', $('wiki').quantile($('delta'), 0.95))
        .apply('deltaMedian', $('wiki').quantile($('delta'), 0.5))
        .apply('commentLength95', $('wiki').quantile($('commentLength'), 0.95))
        .apply('commentLengthMedian', $('wiki').quantile($('commentLength'), 0.5))
        .apply('deltaBucketMedian', $('wiki').quantile($('deltaBucket100'), 0.5))
        .apply(
          'Delta98thEn',
          $('wiki')
            .filter($('channel').is(r('en')))
            .quantile('$delta', 0.98),
        )
        .apply(
          'Delta98thDe',
          $('wiki')
            .filter($('channel').is(r('de')))
            .quantile('$delta', 0.98),
        );

      return basicExecutor(ex).then(result => {
        const datum = result.toJS().data[0];
        const near = (k, druid, tolerance) =>
          expect(datum[k]).to.be.closeTo(druid, Math.max(1, Math.abs(druid) * tolerance));
        near('delta95', 956, 0.02);
        near('deltaMedian', 18, 0.02);
        near('commentLength95', 147, 0.02);
        near('commentLengthMedian', 29, 0.02);
        near('deltaBucketMedian', 0, 0);
        near('Delta98thEn', 2230, 0.02);
        near('Delta98thDe', 2006, 0.02);
      });
    });

    it('works with introspection', () => {
      // ToDo: needs null check correction
      const ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Time',
          $('wiki')
            .split($('__time').timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3)
            .apply(
              'Pages',
              $('wiki')
                .split('$regionName', 'RegionName')
                .apply('Deleted', '$wiki.sum($deleted)')
                .sort('$Deleted', 'descending')
                .limit(2),
            ),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Time: {
              attributes: [
                {
                  name: 'Timestamp',
                  type: 'TIME_RANGE',
                },
                {
                  name: 'TotalAdded',
                  type: 'NUMBER',
                },
                {
                  name: 'Pages',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Pages: {
                    attributes: [
                      {
                        name: 'RegionName',
                        type: 'STRING',
                      },
                      {
                        name: 'Deleted',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Deleted: 11807,
                        RegionName: null,
                      },
                      {
                        Deleted: 848,
                        RegionName: 'Ontario',
                      },
                    ],
                    keys: ['RegionName'],
                  },
                  Timestamp: {
                    end: new Date('2015-09-12T01:00:00.000Z'),
                    start: new Date('2015-09-12T00:00:00.000Z'),
                  },
                  TotalAdded: 331925,
                },
                {
                  Pages: {
                    attributes: [
                      {
                        name: 'RegionName',
                        type: 'STRING',
                      },
                      {
                        name: 'Deleted',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Deleted: 109934,
                        RegionName: null,
                      },
                      {
                        Deleted: 474,
                        RegionName: 'Indiana',
                      },
                    ],
                    keys: ['RegionName'],
                  },
                  Timestamp: {
                    end: new Date('2015-09-12T02:00:00.000Z'),
                    start: new Date('2015-09-12T01:00:00.000Z'),
                  },
                  TotalAdded: 1418072,
                },
                {
                  Pages: {
                    attributes: [
                      {
                        name: 'RegionName',
                        type: 'STRING',
                      },
                      {
                        name: 'Deleted',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Deleted: 124999,
                        RegionName: null,
                      },
                      {
                        Deleted: 449,
                        RegionName: 'Georgia',
                      },
                    ],
                    keys: ['RegionName'],
                  },
                  Timestamp: {
                    end: new Date('2015-09-12T03:00:00.000Z'),
                    start: new Date('2015-09-12T02:00:00.000Z'),
                  },
                  TotalAdded: 3045966,
                },
              ],
              keys: ['Timestamp'],
            },
            TotalAdded: 32553107,
          },
        ]);
      });
    });
  });

  // ============================================================
  // MODE Expression Functional Tests
  // Tests the decomposition approach: split queries produce
  // separate normal + mode queries joined in memory.
  // ============================================================
  describe('MODE expression', function () {
    this.timeout(10000);

    const basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'bigquery',
            source: info.bigQueryWikipedia,
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
          },
          bigQueryRequester,
        ),
      },
    });

    it('split + mode + count (most frequent page per channel)', () => {
      const ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .apply('ModePage', $('wiki').mode('$page'))
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            Count: 114711,
            ModePage: 'User:Cyde/List of candidates for speedy deletion/Subpage',
          },
          {
            Channel: 'vi',
            Count: 99010,
            ModePage: 'Giang Hoa (diễn viên)',
          },
          {
            Channel: 'de',
            Count: 25103,
            ModePage: 'Wikipedia:Vandalismusmeldung',
          },
          {
            Channel: 'fr',
            Count: 21285,
            ModePage: 'Wikipédia:Le Bistro/12 septembre 2015',
          },
          {
            Channel: 'ru',
            Count: 14031,
            ModePage: 'Гомосексуальный образ жизни',
          },
        ]);
      });
    });

    it('total mode + count (most frequent channel overall)', () => {
      const ex = ply()
        .apply('Count', $('wiki').sum('$count'))
        .apply('ModeChannel', $('wiki').mode('$channel'));

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 392443,
            ModeChannel: 'en',
          },
        ]);
      });
    });

    it('value mode (single most frequent page)', () => {
      const ex = ply().apply('MostFrequentPage', $('wiki').mode('$page'));

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            MostFrequentPage: 'Jeremy Corbyn',
          },
        ]);
      });
    });

    it('multiple modes in same split', () => {
      const ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .apply('ModePage', $('wiki').mode('$page'))
        .apply('ModeUser', $('wiki').mode('$user'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            Count: 114711,
            ModePage: 'User:Cyde/List of candidates for speedy deletion/Subpage',
            ModeUser: 'The Quixotic Potato',
          },
          {
            Channel: 'vi',
            Count: 99010,
            ModePage: 'Giang Hoa (diễn viên)',
            ModeUser: 'TuanminhBot',
          },
          {
            Channel: 'de',
            Count: 25103,
            ModePage: 'Wikipedia:Vandalismusmeldung',
            ModeUser: 'Krdbot',
          },
        ]);
      });
    });

    it('mode + sum + count', () => {
      const ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .apply('TotalAdded', $('wiki').sum('$added'))
        .apply('ModePage', $('wiki').mode('$page'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            Count: 114711,
            TotalAdded: 32553107,
            ModePage: 'User:Cyde/List of candidates for speedy deletion/Subpage',
          },
          {
            Channel: 'vi',
            Count: 99010,
            TotalAdded: 3225313,
            ModePage: 'Giang Hoa (diễn viên)',
          },
          {
            Channel: 'de',
            Count: 25103,
            TotalAdded: 5743850,
            ModePage: 'Wikipedia:Vandalismusmeldung',
          },
        ]);
      });
    });

    it('mode as only aggregate in split', () => {
      const ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('ModePage', $('wiki').mode('$page'))
        .sort('$Channel', 'ascending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'ar',
            ModePage: 'قبيلة حارم المشحكة والمعربة',
          },
          {
            Channel: 'be',
            ModePage: 'Парк імя Міхаіла Паўлава',
          },
          {
            Channel: 'bg',
            ModePage: 'Уикипедия:Разговори',
          },
        ]);
      });
    });

    it('mode with time split', () => {
      const ex = $('wiki')
        .split($('__time').timeBucket('PT12H', 'Etc/UTC'), 'TimeHalf')
        .apply('Count', $('wiki').sum('$count'))
        .apply('ModeChannel', $('wiki').mode('$channel'))
        .sort('$TimeHalf', 'ascending');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            TimeHalf: {
              start: new Date('2015-09-12T00:00:00.000Z'),
              end: new Date('2015-09-12T12:00:00.000Z'),
            },
            Count: 165125,
            ModeChannel: 'vi',
          },
          {
            TimeHalf: {
              start: new Date('2015-09-12T12:00:00.000Z'),
              end: new Date('2015-09-13T00:00:00.000Z'),
            },
            Count: 227318,
            ModeChannel: 'en',
          },
        ]);
      });
    });

    it('mode with filter on the mode aggregate', () => {
      const ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .apply('ModeNamespace', $('wiki').filter($('namespace').isnt('Main')).mode('$namespace'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            Count: 114711,
            ModeNamespace: 'User talk',
          },
          {
            Channel: 'vi',
            Count: 99010,
            ModeNamespace: 'Wikipedia',
          },
          {
            Channel: 'de',
            Count: 25103,
            ModeNamespace: 'Wikipedia',
          },
        ]);
      });
    });
  });

  describe('BigQuery Functional', function () {
    this.timeout(10000);

    const basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'bigquery',
            source: info.bigQueryWikipedia,
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
          },
          bigQueryRequester,
        ),
      },
    });

    describe('PIVOT_NESTED_AGG Tests', () => {
      it('funciona con SUM como agregado interno', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'TotalEdits',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(TIME_FLOOR(t.__time, 'PT1H'), COUNT(*) AS "EditsPerHour", SUM("EditsPerHour"))`,
              ),
            ),
          )
          .sort('$TotalEdits', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              Channel: 'en',
              TotalEdits: 113591,
            },
            {
              Channel: 'vi',
              TotalEdits: 99002,
            },
            {
              Channel: 'de',
              TotalEdits: 24987,
            },
          ]);
        });
      });

      it('funciona con AVG como agregado interno', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'AvgEditsPerHour',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(TIME_FLOOR(t.__time, 'PT1H'), COUNT(*) AS "EditsPerHour", AVG("EditsPerHour"))`,
              ),
            ),
          )
          .sort('$AvgEditsPerHour', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          expectSameRows(result.toJS().data, [
            {
              Channel: 'en',
              AvgEditsPerHour: 4732.958333333333,
            },
            {
              Channel: 'vi',
              AvgEditsPerHour: 4125.083333333333,
            },
            {
              Channel: 'de',
              AvgEditsPerHour: 1041.125,
            },
          ]);
        });
      });

      it('funciona con MIN como agregado interno', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'MinEditsPerHour',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(TIME_FLOOR(t.__time, 'PT1H'), COUNT(*) AS "EditsPerHour", MIN("EditsPerHour"))`,
              ),
            ),
          )
          .sort('$MinEditsPerHour', 'ascending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          // Every listed group is tied on the sort value: which channels fill the LIMIT is arbitrary
          expectSameRows(
            result.toJS().data,
            [
              {
                Channel: 'be',
                MinEditsPerHour: 1,
              },
              {
                Channel: 'bg',
                MinEditsPerHour: 1,
              },
              {
                Channel: 'ce',
                MinEditsPerHour: 1,
              },
            ],
            { tiedOn: ['MinEditsPerHour'] },
          );
        });
      });

      it('funciona con MAX como agregado interno', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'MaxEditsPerHour',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(TIME_FLOOR(t.__time, 'PT1H'), COUNT(*) AS "EditsPerHour", MAX("EditsPerHour"))`,
              ),
            ),
          )
          .sort('$MaxEditsPerHour', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              Channel: 'vi',
              MaxEditsPerHour: 12442,
            },
            {
              Channel: 'en',
              MaxEditsPerHour: 6577,
            },
            {
              Channel: 'de',
              MaxEditsPerHour: 1996,
            },
          ]);
        });
      });

      it('funciona con COUNT como agregado interno', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'CountHoursWithEdits',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(TIME_FLOOR(t.__time, 'PT1H'), COUNT(*) AS "EditsPerHour", COUNT("EditsPerHour"))`,
              ),
            ),
          )
          .sort('$CountHoursWithEdits', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          // Every listed group is tied on the sort value: which channels fill the LIMIT is arbitrary
          expectSameRows(
            result.toJS().data,
            [
              {
                Channel: 'ar',
                CountHoursWithEdits: 24,
              },
              {
                Channel: 'ca',
                CountHoursWithEdits: 24,
              },
              {
                Channel: 'cs',
                CountHoursWithEdits: 24,
              },
            ],
            { tiedOn: ['CountHoursWithEdits'] },
          );
        });
      });

      it('funciona con diferentes tipos de agregados internos', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'CombinedAgg',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(
                TIME_FLOOR(t.__time, 'PT1H'),
                SUM("added") AS "AddedPerHour",
                AVG("AddedPerHour") + MAX("AddedPerHour")
              )`,
              ),
            ),
          )
          .sort('$CombinedAgg', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              Channel: 'it',
              CombinedAgg: 4656312.166666667,
            },
            {
              Channel: 'en',
              CombinedAgg: 4402345.458333333,
            },
            {
              Channel: 'hu',
              CombinedAgg: 1172694.5833333333,
            },
          ]);
        });
      });

      it('funciona con agregación interna que incluye una dimensión (usando CASE)', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'TotalAnonEdits',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(
                TIME_FLOOR(t.__time, 'PT1H'),
                SUM(CASE WHEN t.isAnonymous = 'false' THEN 1 ELSE 0 END) AS "AnonEditsPerHourFalse",
                SUM(t.AnonEditsPerHourFalse)
              )`,
              ),
            ),
          )
          .apply(
            'SumAnonEdits',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(
                TIME_FLOOR(t.__time, 'PT1H'),
                AVG(CASE WHEN t.isAnonymous = 'true' THEN 1 ELSE 0 END) AS "AnonEditsPerHour",
                AVG(t.AnonEditsPerHour)
              )`,
              ),
            ),
          )
          .limit(3);

        return basicExecutor(ex).then(result => {
          expectSameRows(result.toJS().data, [
            {
              Channel: 'ar',
              SumAnonEdits: 0.06187466151429335,
              TotalAnonEdits: 4182,
            },
            {
              Channel: 'be',
              SumAnonEdits: 0.03535714285714286,
              TotalAnonEdits: 256,
            },
            {
              Channel: 'bg',
              SumAnonEdits: 0.11940971693867407,
              TotalAnonEdits: 655,
            },
          ]);
        });
      });

      it('funciona con agregado interno que incluye una condición', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'TotalAnonAdded',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(
                TIME_FLOOR(t.__time, 'PT1H'),
                SUM(CASE WHEN "isAnonymous" = 'true' THEN "added" ELSE 0 END) AS "AnonAddedPerHour",
                SUM("AnonAddedPerHour")
              )`,
              ),
            ),
          )
          .sort('$TotalAnonAdded', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              Channel: 'en',
              TotalAnonAdded: 1444691,
            },
            {
              Channel: 'ru',
              TotalAnonAdded: 567625,
            },
            {
              Channel: 'it',
              TotalAnonAdded: 487206,
            },
          ]);
        });
      });

      it('funciona con agregado interno complejo', () => {
        const ex = $('wiki')
          .split('$channel', 'Channel')
          .apply(
            'AvgUniquePagesPerHour',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(
                TIME_FLOOR(t.__time, 'PT1H'),
                COUNT(DISTINCT "page") AS "UniquePagesPerHour",
                AVG("UniquePagesPerHour")
              )`,
              ),
            ),
          )
          .sort('$AvgUniquePagesPerHour', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          // Druid's figures with exact COUNT DISTINCT (its default is approximate: 4112.71, 3339.92, 798.79)
          expectSameRows(result.toJS().data, [
            {
              Channel: 'vi',
              AvgUniquePagesPerHour: 4111.166666666667,
            },
            {
              Channel: 'en',
              AvgUniquePagesPerHour: 3343.2916666666665,
            },
            {
              Channel: 'de',
              AvgUniquePagesPerHour: 801.75,
            },
          ]);
        });
      });

      it('maneja correctamente casos sin datos (edge cases)', () => {
        const ex = $('wiki')
          .filter($('channel').is('nonexistent_channel'))
          .split('$channel', 'Channel')
          .apply(
            'NoDataAgg',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(
                TIME_FLOOR(t.__time, 'PT1H'),
                COUNT(*) AS "EditsPerHour",
                SUM("EditsPerHour")
              )`,
              ),
            ),
          )
          .sort('$NoDataAgg', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([]);
        });
      });

      it.skip('funciona con subexpresiones en la división (split) [BUG: TIME_EXTRACT AS alias in CTE]', () => {
        const ex = $('wiki')
          .split($('channel').concat('_test'), 'ChannelTest')
          .apply(
            'MaxAddedPerHour',
            $('wiki').sqlAggregate(
              r(
                `PIVOT_NESTED_AGG(
                TIME_EXTRACT(t.__time, 'HOUR') AS "Hour",
                SUM("added") AS "AddedPerHour",
                MAX("AddedPerHour")
              )`,
              ),
            ),
          )
          .sort('$MaxAddedPerHour', 'descending')
          .limit(3);

        return basicExecutor(ex).then(result => {
          // Reemplazar con los valores esperados reales
          expect(result.toJS().data).to.deep.equal([
            {
              ChannelTest: 'en_test',
              MaxAddedPerHour: 1,
            },
            // Otros canales
          ]);
        });
      });
    });
  });

  // One or more queries per expression op, the expected rows being what Druid SQL returns on the same
  // wikipedia rows (useApproximateCountDistinct and useApproximateTopN off). Not covered:
  // - cardinality: Druid SQL has no CARDINALITY either ("No match found for function signature")
  // - ipMatch, ipSearch, ipStringify: Druid IP types, no BigQuery column holds one
  // - customAggregate, customTransform: Druid native JSON, not SQL
  // - timeRange, join, select: no SQL form in SQLExternal on any engine
  // - a multi-value column inside a measure other than in a filter: refused with an error
  // - a sum filtered on a multi-value column: on client 507 Druid returns NULL for it (its
  //   SUM(CASE … ELSE 0 END) over a multi-value column), so no Druid answer is frozen; BigQuery returns the sum
  // - a filter on a collect measure: dropped by SQLExternal on master, on Druid SQL as well
  // - a filtered sum of a column over a group with no matching row: Druid gives NULL on `wikipedia`
  //   and 0 on the client datasources (same filtered longSum plan); BigQuery gives 0
  describe('expression ops', () => {
    const opsAttributes = [
      { name: '__time', type: 'TIME' },
      { name: 'sometimeLater', type: 'STRING' },
      { name: 'sometimeLaterMs', type: 'NUMBER' },
      { name: 'channel', type: 'STRING' },
      { name: 'cityName', type: 'STRING' },
      { name: 'comment', type: 'STRING' },
      { name: 'commentLength', type: 'NUMBER' },
      { name: 'commentLengthStr', type: 'STRING' },
      { name: 'countryIsoCode', type: 'STRING' },
      { name: 'deltaBucket100', type: 'NUMBER' },
      { name: 'isRobot', type: 'STRING' },
      { name: 'namespace', type: 'STRING' },
      { name: 'page', type: 'STRING' },
      { name: 'user', type: 'STRING' },
      { name: 'userChars', type: 'STRING' },
      { name: 'added', type: 'NUMBER' },
      { name: 'count', type: 'NUMBER' },
      { name: 'deleted', type: 'NUMBER' },
      { name: 'delta', type: 'NUMBER' },
      { name: 'deltaByTen', type: 'NUMBER' },
    ];

    const wiki = External.fromJS(
      {
        engine: 'bigquery',
        source: info.bigQueryWikipedia,
        attributes: opsAttributes.map(a =>
          a.name === 'userChars' ? { ...a, nativeType: 'ARRAY<STRUCT<v STRING>>' } : a,
        ),
        context,
        timeAttribute: '__time',
      },
      bigQueryRequester,
    );

    const top = (split, measure = $('wiki').sum('$count'), n = 5) =>
      $('wiki').split(split, 'K').apply('V', measure).sort('$V', 'descending').limit(n);
    const byKey = (split, measure = $('wiki').sum('$count'), n = 10) =>
      $('wiki').split(split, 'K').apply('V', measure).sort('$K', 'ascending').limit(n);
    const total = measure => ply().apply('V', measure);
    const countWhere = filter => total($('wiki').filter(filter).sum('$count'));

    const day = TimeRange.fromJS({
      start: new Date('2015-09-12T00:00:00Z'),
      end: new Date('2015-09-13T00:00:00Z'),
    });

    const CASES = {
      Absolute: total($('wiki').sum($('delta').absolute())),
      Add: total($('wiki').sum($('added').add($('deleted')))),
      Subtract: total($('wiki').sum($('added').subtract($('deleted')))),
      Multiply: total($('wiki').sum($('added').multiply(2))),
      Divide: total($('wiki').sum('$added').divide($('wiki').sum('$deleted'))),
      DivideByZero: $('wiki')
        .filter($('channel').in(['de', 'en', 'fr']))
        .split('$channel', 'K')
        .apply(
          'V',
          $('wiki')
            .sum('$added')
            .divide(
              $('wiki')
                .filter($('channel').is(r('en')))
                .count(),
            ),
        )
        .sort('$K', 'ascending'),
      And: countWhere(
        $('channel')
          .is(r('en'))
          .and($('isRobot').is(r('true'))),
      ),
      Or: countWhere(
        $('channel')
          .is(r('en'))
          .or($('namespace').is(r('User'))),
      ),
      Not: countWhere($('channel').is(r('en')).not()),
      Average: total($('wiki').average('$added')),
      CastToString: top($('commentLength').cast('STRING')),
      CastToNumber: total($('wiki').sum($('commentLengthStr').cast('NUMBER'))),
      CastToTime: byKey($('sometimeLaterMs').cast('TIME').timeBucket('P1D', 'Etc/UTC')),
      // A filter on the collected measure is dropped by SQLExternal on master (both engines), so none here
      Collect: byKey('$channel', $('wiki').collect('$isRobot'), 5),
      Concat: top($('channel').concat(r('-'), $('namespace'))),
      Contains: countWhere($('page').contains(r('Israel'))),
      ContainsIgnoreCase: countWhere($('page').contains(r('israel'), 'ignoreCase')),
      Count: total($('wiki').count()),
      CountDistinct: total($('wiki').countDistinct('$user')),
      CountDistinctFiltered: total(
        $('wiki')
          .filter($('channel').is(r('en')))
          .countDistinct('$page'),
      ),
      Extract: top($('page').extract('^([A-Z][a-z]+)')),
      Fallback: top($('cityName').fallback(r('(none)'))),
      GreaterThan: countWhere($('added').greaterThan(100)),
      GreaterThanOrEqual: countWhere($('added').greaterThanOrEqual(100)),
      LessThan: countWhere($('added').lessThan(100)),
      LessThanOrEqual: countWhere($('added').lessThanOrEqual(100)),
      InSet: countWhere($('channel').in(['en', 'de', 'zzz'])),
      InNumberRange: countWhere($('commentLength').in(NumberRange.fromJS({ start: 10, end: 20 }))),
      IndexOf: top($('page').indexOf(r('a'))),
      Is: countWhere($('channel').is(r('fr'))),
      IsNull: countWhere($('cityName').is(r(null))),
      Length: total($('wiki').sum($('page').length())),
      LengthUnicode: top('$channel', $('wiki').sum($('page').length()), 5),
      Log: total($('wiki').sum('$added').log(10)),
      LogOfZero: top('$isRobot', $('wiki').sum('$added').log(), 3),
      LogOfZeroRows: byKey('$channel', $('wiki').filter($('added').is(0)).sum('$added').log(), 3),
      LogOfNegative: byKey(
        '$channel',
        $('wiki').filter($('delta').lessThan(0)).sum('$delta').log(),
        3,
      ),
      Lookup: top($('channel').lookup('channel-lookup').fallback(r('???'))),
      Match: countWhere($('page').match('^Isr')),
      Max: total($('wiki').max('$added')),
      Min: total($('wiki').min('$deleted')),
      Mode: top(
        '$isRobot',
        $('wiki')
          .filter($('namespace').is(r('Main')))
          .mode('$channel'),
        2,
      ),
      MvContains: countWhere($('userChars').mvContains(['A', 'B'])),
      MvOverlap: countWhere($('userChars').mvOverlap(['A', 'Z'])),
      MvFilterOnly: byKey($('userChars').mvFilterOnly(['A', 'B'])),
      MvSplit: byKey('$userChars', $('wiki').count(), 8),
      // A filter on a multi-value column inside a measure, next to a plain count (else it moves to WHERE)
      MvMeasureTotal: ply()
        .apply('N', $('wiki').count())
        .apply(
          'V',
          $('wiki')
            .filter($('userChars').is(r('A')))
            .count(),
        ),
      MvMeasureNot: ply()
        .apply('N', $('wiki').count())
        .apply(
          'V',
          $('wiki')
            .filter($('userChars').is(r('A')).not())
            .count(),
        ),
      MvMeasureSplit: $('wiki')
        .split('$channel', 'K')
        .apply('N', $('wiki').count())
        .apply(
          'V',
          $('wiki')
            .filter($('userChars').is(r('A')))
            .average('$added'),
        )
        .sort('$K', 'ascending')
        .limit(5),
      MvMeasureSplitOnIt: $('wiki')
        .split('$userChars', 'K')
        .apply('N', $('wiki').count())
        .apply(
          'V',
          $('wiki')
            .filter($('userChars').in(['A', 'B']))
            .count(),
        )
        .sort('$K', 'ascending')
        .limit(8),
      NumberBucket: byKey($('commentLength').numberBucket(10)),
      Overlap: countWhere(
        $('__time').overlap(
          TimeRange.fromJS({
            start: new Date('2015-09-12T06:00:00Z'),
            end: new Date('2015-09-12T07:30:00Z'),
          }),
        ),
      ),
      Power: total($('wiki').sum('$count').power(0.5)),
      Quantile: total($('wiki').quantile('$delta', 0.5)),
      SqlRef: top(s$(`CONCAT(channel, '!')`)),
      SqlAggregate: total($('wiki').sqlAggregate(r('SUM(t.added) - SUM(t.deleted)'))),
      Substr: top($('countryIsoCode').substr(0, 1)),
      SubstrPastEnd: top($('channel').substr(5, 3)),
      Then: countWhere($('channel').is(r('en')).then(r(true)).fallback(r(false))),
      TimeBucketHour: byKey($('__time').timeBucket('PT1H', 'Etc/UTC'), $('wiki').sum('$count'), 24),
      TimeBucket5MinLA: byKey(
        $('__time').timeBucket('PT5M', 'America/Los_Angeles'),
        $('wiki').sum('$count'),
        6,
      ),
      TimeBucketWeek: byKey($('__time').timeBucket('P1W', 'Etc/UTC')),
      TimeFloorDayLA: byKey($('__time').timeFloor('P1D', 'America/Los_Angeles')),
      TimePartHour: byKey(
        $('__time').timePart('HOUR_OF_DAY', 'Etc/UTC'),
        $('wiki').sum('$count'),
        24,
      ),
      TimePartDayOfWeek: byKey($('__time').timePart('DAY_OF_WEEK', 'America/Los_Angeles')),
      TimePartWeek: byKey($('__time').timePart('WEEK_OF_YEAR', 'Etc/UTC')),
      TimeShift: countWhere(
        $('__time').timeShift(Duration.fromJS('PT6H'), 1, 'Etc/UTC').overlap(day),
      ),
      TransformCase: top($('channel').transformCase('upperCase')),
    };

    const EXPECTED = {
      Absolute: [{ V: 101291014 }],
      Add: [{ V: 101303624 }],
      Subtract: [{ V: 93483862 }],
      Multiply: [{ V: 194787486 }],
      Divide: [{ V: 24.909643797343193 }],
      DivideByZero: [
        { K: 'de', V: 'Infinity' },
        { K: 'en', V: 286.581745032617 },
        { K: 'fr', V: 'Infinity' },
      ],
      And: [{ V: 10849 }],
      Or: [{ V: 114889 }],
      Not: [{ V: 277732 }],
      Average: [{ V: 249.6417718014836 }],
      CastToString: [
        { K: '13', V: 42013 },
        { K: '34', V: 24469 },
        { K: '29', V: 22526 },
        { K: '23', V: 11630 },
        { K: '90', V: 10919 },
      ],
      CastToNumber: [{ V: 18220473 }],
      CastToTime: [
        { K: { start: '2016-09-12T00:00:00.000Z', end: '2016-09-13T00:00:00.000Z' }, V: 392443 },
      ],
      Collect: [
        { K: 'ar', V: 'false, true' },
        { K: 'be', V: 'false, true' },
        { K: 'bg', V: 'false, true' },
        { K: 'ca', V: 'false, true' },
        { K: 'ce', V: 'false, true' },
      ],
      Concat: [
        { K: 'vi-Main', V: 98678 },
        { K: 'en-Main', V: 78233 },
        { K: 'de-Main', V: 17537 },
        { K: 'fr-Main', V: 14519 },
        { K: 'it-Main', V: 10733 },
      ],
      Contains: [{ V: 153 }],
      ContainsIgnoreCase: [{ V: 157 }],
      Count: [{ V: 390134 }],
      CountDistinct: [{ V: 38234 }],
      CountDistinctFiltered: [{ V: 65762 }],
      Extract: [
        { K: null, V: 74399 },
        { K: 'User', V: 18659 },
        { K: 'Wikipedia', V: 12250 },
        { K: 'Talk', V: 5807 },
        { K: 'List', V: 4345 },
      ],
      Fallback: [
        { K: '(none)', V: 371151 },
        { K: 'Tokyo', V: 460 },
        { K: 'Central District', V: 425 },
        { K: 'Moscow', V: 330 },
        { K: 'Milan', V: 267 },
      ],
      GreaterThan: [{ V: 73542 }],
      GreaterThanOrEqual: [{ V: 73831 }],
      LessThan: [{ V: 318612 }],
      LessThanOrEqual: [{ V: 318901 }],
      InSet: [{ V: 139814 }],
      InNumberRange: [{ V: 108066 }],
      IndexOf: [
        { K: -1, V: 106359 },
        { K: 1, V: 70191 },
        { K: 6, V: 29546 },
        { K: 8, V: 25550 },
        { K: 2, V: 21725 },
      ],
      Is: [{ V: 21285 }],
      IsNull: [{ V: 371151 }],
      Length: [{ V: 8186864 }],
      LengthUnicode: [
        { K: 'en', V: 2725647 },
        { K: 'vi', V: 1778218 },
        { K: 'de', V: 607086 },
        { K: 'fr', V: 531701 },
        { K: 'ru', V: 339948 },
      ],
      Log: [{ V: 7.988531056797921 }],
      LogOfZero: [
        { K: 'false', V: 18.124181621974813 },
        { K: 'true', V: 16.9532680802474 },
      ],
      LogOfZeroRows: [
        { K: 'ar', V: '-Infinity' },
        { K: 'be', V: '-Infinity' },
        { K: 'bg', V: '-Infinity' },
      ],
      LogOfNegative: [
        { K: 'ar', V: 'NaN' },
        { K: 'be', V: 'NaN' },
        { K: 'bg', V: 'NaN' },
      ],
      Lookup: [
        { K: '???', V: 227040 },
        { K: 'English', V: 114711 },
        { K: 'French', V: 21285 },
        { K: 'Russian', V: 14031 },
        { K: 'Spanish', V: 12824 },
      ],
      Match: [{ V: 63 }],
      Max: [{ V: 242198 }],
      Min: [{ V: 0 }],
      Mode: [
        { K: 'true', V: 'vi' },
        { K: 'false', V: 'en' },
      ],
      MvContains: [{ V: 116323 }],
      MvOverlap: [{ V: 232791 }],
      MvFilterOnly: [
        { K: null, V: 109104 },
        { K: 'A', V: 222676 },
        { K: 'B', V: 176986 },
      ],
      MvSplit: [
        { K: ' ', V: 39371 },
        { K: '!', V: 26628 },
        { K: '"', V: 5 },
        { K: '$', V: 2 },
        { K: '&', V: 402 },
        { K: "'", V: 316 },
        { K: '(', V: 251 },
        { K: ')', V: 252 },
      ],
      MvMeasureTotal: [{ N: 390134, V: 221790 }],
      MvMeasureNot: [{ N: 390134, V: 168344 }],
      MvMeasureSplit: [
        { K: 'ar', N: 4467, V: 640.4052741151978 },
        { K: 'be', N: 269, V: 956.5793103448276 },
        { K: 'bg', N: 778, V: 1556.1198156682028 },
        { K: 'ca', N: 4448, V: 212.1478715459298 },
        { K: 'ce', N: 665, V: 1301.9830508474577 },
      ],
      MvMeasureSplitOnIt: [
        { K: ' ', N: 39371, V: 30798 },
        { K: '!', N: 26628, V: 25951 },
        { K: '"', N: 5, V: 3 },
        { K: '$', N: 2, V: 1 },
        { K: '&', N: 402, V: 349 },
        { K: "'", N: 316, V: 250 },
        { K: '(', N: 251, V: 186 },
        { K: ')', N: 252, V: 187 },
      ],
      NumberBucket: [
        { K: { start: 0, end: 10 }, V: 21525 },
        { K: { start: 10, end: 20 }, V: 108066 },
        { K: { start: 20, end: 30 }, V: 68273 },
        { K: { start: 30, end: 40 }, V: 52496 },
        { K: { start: 40, end: 50 }, V: 23868 },
        { K: { start: 50, end: 60 }, V: 19998 },
        { K: { start: 60, end: 70 }, V: 13574 },
        { K: { start: 70, end: 80 }, V: 12577 },
        { K: { start: 80, end: 90 }, V: 10238 },
        { K: { start: 90, end: 100 }, V: 18430 },
      ],
      Overlap: [{ V: 32905 }],
      Power: [{ V: 626.4527117029664 }],
      Quantile: [{ V: 18 }],
      SqlRef: [
        { K: 'en!', V: 114711 },
        { K: 'vi!', V: 99010 },
        { K: 'de!', V: 25103 },
        { K: 'fr!', V: 21285 },
        { K: 'ru!', V: 14031 },
      ],
      SqlAggregate: [{ V: 93483862 }],
      Substr: [
        { K: null, V: 354930 },
        { K: 'U', V: 5829 },
        { K: 'I', V: 4899 },
        { K: 'G', V: 2717 },
        { K: 'C', V: 2519 },
      ],
      SubstrPastEnd: [
        { K: null, V: 392025 },
        { K: 'e', V: 418 },
      ],
      Then: [{ V: 114711 }],
      TimeBucketHour: [
        { K: { start: '2015-09-12T00:00:00.000Z', end: '2015-09-12T01:00:00.000Z' }, V: 2681 },
        { K: { start: '2015-09-12T01:00:00.000Z', end: '2015-09-12T02:00:00.000Z' }, V: 11442 },
        { K: { start: '2015-09-12T02:00:00.000Z', end: '2015-09-12T03:00:00.000Z' }, V: 11020 },
        { K: { start: '2015-09-12T03:00:00.000Z', end: '2015-09-12T04:00:00.000Z' }, V: 8148 },
        { K: { start: '2015-09-12T04:00:00.000Z', end: '2015-09-12T05:00:00.000Z' }, V: 8240 },
        { K: { start: '2015-09-12T05:00:00.000Z', end: '2015-09-12T06:00:00.000Z' }, V: 12608 },
        { K: { start: '2015-09-12T06:00:00.000Z', end: '2015-09-12T07:00:00.000Z' }, V: 21194 },
        { K: { start: '2015-09-12T07:00:00.000Z', end: '2015-09-12T08:00:00.000Z' }, V: 22373 },
        { K: { start: '2015-09-12T08:00:00.000Z', end: '2015-09-12T09:00:00.000Z' }, V: 16221 },
        { K: { start: '2015-09-12T09:00:00.000Z', end: '2015-09-12T10:00:00.000Z' }, V: 17038 },
        { K: { start: '2015-09-12T10:00:00.000Z', end: '2015-09-12T11:00:00.000Z' }, V: 17920 },
        { K: { start: '2015-09-12T11:00:00.000Z', end: '2015-09-12T12:00:00.000Z' }, V: 16240 },
        { K: { start: '2015-09-12T12:00:00.000Z', end: '2015-09-12T13:00:00.000Z' }, V: 17091 },
        { K: { start: '2015-09-12T13:00:00.000Z', end: '2015-09-12T14:00:00.000Z' }, V: 20725 },
        { K: { start: '2015-09-12T14:00:00.000Z', end: '2015-09-12T15:00:00.000Z' }, V: 18733 },
        { K: { start: '2015-09-12T15:00:00.000Z', end: '2015-09-12T16:00:00.000Z' }, V: 19655 },
        { K: { start: '2015-09-12T16:00:00.000Z', end: '2015-09-12T17:00:00.000Z' }, V: 19588 },
        { K: { start: '2015-09-12T17:00:00.000Z', end: '2015-09-12T18:00:00.000Z' }, V: 23001 },
        { K: { start: '2015-09-12T18:00:00.000Z', end: '2015-09-12T19:00:00.000Z' }, V: 21699 },
        { K: { start: '2015-09-12T19:00:00.000Z', end: '2015-09-12T20:00:00.000Z' }, V: 20129 },
        { K: { start: '2015-09-12T20:00:00.000Z', end: '2015-09-12T21:00:00.000Z' }, V: 18317 },
        { K: { start: '2015-09-12T21:00:00.000Z', end: '2015-09-12T22:00:00.000Z' }, V: 17660 },
        { K: { start: '2015-09-12T22:00:00.000Z', end: '2015-09-12T23:00:00.000Z' }, V: 15906 },
        { K: { start: '2015-09-12T23:00:00.000Z', end: '2015-09-13T00:00:00.000Z' }, V: 14814 },
      ],
      TimeBucket5MinLA: [
        { K: { start: '2015-09-12T00:45:00.000Z', end: '2015-09-12T00:50:00.000Z' }, V: 618 },
        { K: { start: '2015-09-12T00:50:00.000Z', end: '2015-09-12T00:55:00.000Z' }, V: 998 },
        { K: { start: '2015-09-12T00:55:00.000Z', end: '2015-09-12T01:00:00.000Z' }, V: 1065 },
        { K: { start: '2015-09-12T01:00:00.000Z', end: '2015-09-12T01:05:00.000Z' }, V: 1127 },
        { K: { start: '2015-09-12T01:05:00.000Z', end: '2015-09-12T01:10:00.000Z' }, V: 854 },
        { K: { start: '2015-09-12T01:10:00.000Z', end: '2015-09-12T01:15:00.000Z' }, V: 906 },
      ],
      TimeBucketWeek: [
        { K: { start: '2015-09-07T00:00:00.000Z', end: '2015-09-14T00:00:00.000Z' }, V: 392443 },
      ],
      TimeFloorDayLA: [
        { K: '2015-09-11T07:00:00.000Z', V: 75333 },
        { K: '2015-09-12T07:00:00.000Z', V: 317110 },
      ],
      TimePartHour: [
        { K: 0, V: 2681 },
        { K: 1, V: 11442 },
        { K: 2, V: 11020 },
        { K: 3, V: 8148 },
        { K: 4, V: 8240 },
        { K: 5, V: 12608 },
        { K: 6, V: 21194 },
        { K: 7, V: 22373 },
        { K: 8, V: 16221 },
        { K: 9, V: 17038 },
        { K: 10, V: 17920 },
        { K: 11, V: 16240 },
        { K: 12, V: 17091 },
        { K: 13, V: 20725 },
        { K: 14, V: 18733 },
        { K: 15, V: 19655 },
        { K: 16, V: 19588 },
        { K: 17, V: 23001 },
        { K: 18, V: 21699 },
        { K: 19, V: 20129 },
        { K: 20, V: 18317 },
        { K: 21, V: 17660 },
        { K: 22, V: 15906 },
        { K: 23, V: 14814 },
      ],
      TimePartDayOfWeek: [
        { K: 5, V: 75333 },
        { K: 6, V: 317110 },
      ],
      TimePartWeek: [{ K: 37, V: 392443 }],
      TimeShift: [{ V: 283918 }],
      TransformCase: [
        { K: 'EN', V: 114711 },
        { K: 'VI', V: 99010 },
        { K: 'DE', V: 25103 },
        { K: 'FR', V: 21285 },
        { K: 'RU', V: 14031 },
      ],
    };

    for (const [name, ex] of Object.entries(CASES)) {
      it(`works with ${name}`, () => {
        return ex.compute({ wiki }).then(result => {
          // Through JSON as Druid's rows were: dates as ISO strings, Infinity and NaN as text
          expectSameRows(JSON.parse(JSON.stringify(result.toJS().data)), EXPECTED[name]);
        });
      });
    }
  });
});
