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

import { Column, Introspect, QueryResult, SqlColumn, SqlQuery } from 'druid-query-toolkit';
import { PlywoodRequester } from 'plywood-base-api';
import * as toArray from 'stream-to-array';

import { AttributeInfo, Attributes } from '../datatypes';
import { DruidDialect } from '../dialect';
import { Expression, RefExpression, SqlRefExpression } from '../expressions';
import { dictEqual } from '../helper';
import { PlyType } from '../types';

import { External, ExternalJS, ExternalValue, IntrospectionDepth } from './baseExternal';
import { DruidExternal } from './druidExternal';
import { SQLExternal } from './sqlExternal';

export interface DruidSQLDescribeRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
}

export class DruidSQLExternal extends SQLExternal {
  static engine = 'druidsql';
  static type = 'DATASET';

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): DruidSQLExternal {
    const value: ExternalValue = SQLExternal.jsToValue(parameters, requester);
    value.context = parameters.context;
    return new DruidSQLExternal(value);
  }

  static postProcessIntrospect(columns: Column[]): Attributes {
    return columns.map(column => {
      const name = column.name;
      const effectiveType =
        column.sqlType === 'TIMESTAMP' || column.sqlType === 'BOOLEAN'
          ? column.sqlType
          : column.nativeType;

      let type: PlyType;
      switch (String(effectiveType).toUpperCase()) {
        case 'TIMESTAMP':
        case 'DATE':
          type = 'TIME';
          break;

        case 'IPADDRESS':
        case 'IPPREFIX':
          type = 'IP';
          break;

        case 'VARCHAR':
        case 'STRING':
          type = 'STRING';
          break;

        case 'DOUBLE':
        case 'FLOAT':
        case 'BIGINT':
        case 'LONG':
          type = 'NUMBER';
          break;

        case 'BOOLEAN':
          type = 'BOOLEAN';
          break;

        case 'imply-ts':
          type = 'TIME_SERIES';
          break;

        default:
          // OTHER
          type = 'NULL';
          break;
      }

      return new AttributeInfo({
        name,
        type,
        nativeType: effectiveType,
      });
    });
  }

  static async getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    const sources = await toArray(
      requester({
        query: {
          query: Introspect.getTableIntrospectionQuery(),
        },
      }),
    );

    return Introspect.decodeTableIntrospectionResult(QueryResult.fromRawResult(sources))
      .map(s => s.name)
      .sort();
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(
      requester({
        query: {
          queryType: 'status',
        },
      }),
    ).then(res => {
      return res[0].version;
    });
  }

  public context: Record<string, any>;

  constructor(parameters: ExternalValue) {
    super(
      parameters,
      new DruidDialect({ attributes: parameters.rawAttributes || parameters.attributes }),
    );
    this._ensureEngine('druidsql');
    this.context = parameters.context;
  }

  public valueOf(): ExternalValue {
    const value: ExternalValue = super.valueOf();
    value.context = this.context;
    return value;
  }

  public toJS(): ExternalJS {
    const js: ExternalJS = super.toJS();
    if (this.context) js.context = this.context;
    return js;
  }

  public equals(other: DruidSQLExternal | undefined): boolean {
    return super.equals(other) && dictEqual(this.context, other.context);
  }

  // -----------------

  public getTimeAttribute(): string | undefined {
    return '__time';
  }

  public isTimeRef(ex: Expression): ex is RefExpression {
    if (ex instanceof SqlRefExpression) {
      if (ex.parsedSql instanceof SqlColumn) {
        return ex.parsedSql.getName() === '__time';
      } else {
        return false;
      }
    } else {
      return super.isTimeRef(ex);
    }
  }

  protected async getIntrospectAttributes(depth: IntrospectionDepth): Promise<Attributes> {
    const { source, withQuery } = this;

    if (withQuery) {
      let withQueryParsed: SqlQuery;
      try {
        withQueryParsed = SqlQuery.parse(withQuery);
      } catch (e) {
        throw new Error(`could not parse withQuery: ${e.message}`);
      }

      const queryPayload = Introspect.getQueryColumnIntrospectionPayload(withQueryParsed);

      // Query for sample also
      const rawResult = await toArray(
        this.requester({
          query: {
            ...queryPayload,
            resultFormat: 'array',
            context: this.context,
          },
        }),
      );

      const queryResult = QueryResult.fromRawResult(
        rawResult,
        true,
        queryPayload.header,
        queryPayload.typesHeader,
        queryPayload.sqlTypesHeader,
      );

      return DruidSQLExternal.postProcessIntrospect(
        Introspect.decodeQueryColumnIntrospectionResult(queryResult),
      );
    }

    let table: string;
    if (Array.isArray(source)) {
      table = source[0];
    } else {
      table = source;
    }

    // SQL-native introspection via INFORMATION_SCHEMA.COLUMNS. On clusters
    // with many segments, Druid's native segmentMetadata query can take
    // minutes or time out entirely; INFORMATION_SCHEMA is sub-second and
    // returns everything we need for an External's attribute list. Since
    // we're already a druidsql External we can use the same endpoint.
    // We fall back to segmentMetadata only if the SQL path fails — that
    // way existing clusters that somehow lack INFORMATION_SCHEMA continue
    // to work.
    try {
      const rawResult = await toArray(
        this.requester({
          query: {
            query: `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ${this.dialect.escapeLiteral(
              table,
            )} ORDER BY ORDINAL_POSITION`,
            context: this.context,
          },
        }),
      );
      if (rawResult.length > 0) {
        return DruidSQLExternal.postProcessInformationSchemaIntrospect(
          rawResult as DruidSQLDescribeRow[],
        );
      }
    } catch (e) {
      // Fall through to segmentMetadata. Not logged — the fallback is the
      // safety net; noise would drown the real failure if segmentMetadata
      // itself fails too.
    }

    return DruidExternal.introspectAttributesWithSegmentMetadata(
      table,
      this.requester,
      '__time',
      this.context,
      depth,
    );
  }

  static postProcessInformationSchemaIntrospect(columns: DruidSQLDescribeRow[]): Attributes {
    return columns.map(column => {
      const name = column.COLUMN_NAME;
      const nativeType = String(column.DATA_TYPE || '').toUpperCase();

      let type: PlyType;
      switch (nativeType) {
        case 'TIMESTAMP':
        case 'DATE':
          type = 'TIME';
          break;

        case 'IPADDRESS':
        case 'IPPREFIX':
          type = 'IP';
          break;

        case 'VARCHAR':
        case 'STRING':
        case 'CHAR':
          type = 'STRING';
          break;

        case 'DOUBLE':
        case 'FLOAT':
        case 'REAL':
        case 'BIGINT':
        case 'INTEGER':
        case 'SMALLINT':
        case 'TINYINT':
        case 'LONG':
          type = 'NUMBER';
          break;

        case 'BOOLEAN':
          type = 'BOOLEAN';
          break;

        default:
          // Unknown native types fall back to STRING — plywood can still
          // reference and filter them, and an explicit attributeOverrides
          // on the External can correct a specific column if needed.
          type = 'STRING';
          break;
      }

      return new AttributeInfo({
        name,
        type,
        nativeType,
      });
    });
  }

  protected sqlToQuery(sql: string): any {
    const payload: any = {
      query: sql,
    };

    payload.context = { ...(this.context || {}), sqlTimeZone: 'Etc/UTC' };

    return payload;
  }

  protected capability(cap: string): boolean {
    if (cap === 'filter-on-attribute') return false;
    return super.capability(cap);
  }
}

External.register(DruidSQLExternal);
