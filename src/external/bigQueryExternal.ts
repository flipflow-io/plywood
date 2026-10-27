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

import { SqlColumn } from 'druid-query-toolkit';
import { PlywoodRequester } from 'plywood-base-api';
import * as toArray from 'stream-to-array';

import { AttributeInfo, Attributes } from '../datatypes';
import { BigQueryDialect } from '../dialect';
import {
  $,
  AndExpression,
  Expression,
  IsExpression,
  LiteralExpression,
  MvContainsExpression,
  MvFilterOnlyExpression,
  MvOverlapExpression,
  NotExpression,
  OrExpression,
  RefExpression,
  SplitExpression,
  SqlRefExpression,
} from '../expressions';
import { dictEqual } from '../helper';
import { PlyType } from '../types';

import { External, ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';

export interface BigQueryField {
  name: string;
  type: string;
  mode?: string;
  fields?: BigQueryField[];
}

/**
 * How the multi-value columns used by a query are read. Druid evaluates a filter on a multi-value
 * column per row (true when any element matches) and a split on one per element, except that a
 * top level `= 'x'` or `IS NULL` filter turns a split on the same column into that single value.
 */
interface MultiValuePlan {
  filter: Expression;
  split: SplitExpression | undefined;
  columns: string[];
  elementColumns: { name: string; array: string }[];
  predicates: string[];
  constants: Record<string, string>;
}

export class BigQueryExternal extends SQLExternal {
  static engine = 'bigquery';
  static type = 'DATASET';

  static MULTI_VALUE_NATIVE_TYPE = 'ARRAY<STRUCT<v STRING>>';

  static STANDARD_TYPE_NAME: Record<string, string> = {
    INTEGER: 'INT64',
    FLOAT: 'FLOAT64',
    BOOLEAN: 'BOOL',
  };

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): BigQueryExternal {
    const value: ExternalValue = SQLExternal.jsToValue(parameters, requester);
    value.context = parameters.context;
    return new BigQueryExternal(value);
  }

  static postProcessIntrospect(fields: BigQueryField[]): Attributes {
    return fields
      .map(field => {
        const name = field.name;
        const repeated = field.mode === 'REPEATED';
        let nativeType = String(field.type).toUpperCase();
        let type: PlyType;
        if (repeated) {
          // Druid's multi-value columns are copied as ARRAY<STRUCT<v STRING>> (NULL elements kept).
          // Druid SQL presents them as VARCHAR, so the cube sees a STRING.
          const subFields = field.fields || [];
          if (
            nativeType === 'RECORD' &&
            subFields.length === 1 &&
            subFields[0].name === 'v' &&
            subFields[0].type === 'STRING'
          ) {
            return new AttributeInfo({
              name,
              type: 'STRING',
              nativeType: BigQueryExternal.MULTI_VALUE_NATIVE_TYPE,
            });
          }
          return null;
        }

        switch (nativeType) {
          case 'TIMESTAMP':
          case 'DATETIME':
          case 'DATE':
            type = 'TIME';
            break;

          case 'STRING':
            type = 'STRING';
            break;

          case 'JSON':
            // As Druid SQL's COMPLEX<json>: reachable through JSON_VALUE in formulas
            type = 'STRING';
            break;

          case 'INT64':
          case 'INTEGER':
          case 'FLOAT64':
          case 'FLOAT':
          case 'NUMERIC':
          case 'BIGNUMERIC':
            type = 'NUMBER';
            break;

          case 'BOOL':
          case 'BOOLEAN':
            type = 'BOOLEAN';
            break;

          default:
            return null;
        }
        // Standard SQL names for the legacy ones a dry-run schema reports
        nativeType = BigQueryExternal.STANDARD_TYPE_NAME[nativeType] || nativeType;

        return new AttributeInfo({
          name,
          type,
          nativeType,
        });
      })
      .filter(Boolean);
  }

  static async getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    const sources = await toArray(requester({ query: { listTables: true } }));
    return sources.map((s: { name: string }) => s.name).sort();
  }

  static getVersion(_requester: PlywoodRequester<any>): Promise<string> {
    // BigQuery has no server version; this is its REST API version (bigquery/v2)
    return Promise.resolve('2.0.0');
  }

  public context: Record<string, any>;

  constructor(parameters: ExternalValue) {
    super(
      parameters,
      new BigQueryDialect({
        attributes: parameters.rawAttributes || parameters.attributes,
        lookupTable: lookup => BigQueryExternal.lookupTableFor(parameters, lookup),
      }),
    );
    this._ensureEngine('bigquery');
    this.context = parameters.context;
  }

  /**
   * A Druid lookup becomes a (k, v) table. `context.lookupTables` names it; otherwise it is
   * `lookup__<name>` in the dataset of the source.
   */
  static lookupTableFor(parameters: ExternalValue, lookup: string): string {
    const { context, source } = parameters;
    const lookupTables = (context && context.lookupTables) || {};
    if (lookupTables[lookup]) return lookupTables[lookup];
    const table = Array.isArray(source) ? source[0] : source;
    const path = String(table).split('.');
    path[path.length - 1] = `lookup__${lookup}`;
    return path.join('.');
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

  public equals(other: BigQueryExternal | undefined): boolean {
    return super.equals(other) && dictEqual(this.context, other.context);
  }

  // -----------------

  public getTimeAttribute(): string | undefined {
    return this.timeAttribute || '__time';
  }

  public isTimeRef(ex: Expression): ex is RefExpression {
    if (ex instanceof SqlRefExpression) {
      return (
        ex.parsedSql instanceof SqlColumn && ex.parsedSql.getName() === this.getTimeAttribute()
      );
    }
    return super.isTimeRef(ex);
  }

  // Druid returns the groups of an unsorted split ordered by their keys
  protected getUnsortedSplitOrderSQL(split: SplitExpression): string | null {
    const { dialect } = this;
    return (
      'ORDER BY ' +
      split
        .mapSplits((name, expression) =>
          dialect.orderByExpression(dialect.escapeName(name), 'ASC', expression.type),
        )
        .join(', ')
    );
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    const { source, withQuery, dialect } = this;
    const table = Array.isArray(source) ? source[0] : source;
    // A dry run returns the schema and bills nothing
    return toArray(
      this.requester({
        query: {
          query: withQuery ? withQuery : `SELECT * FROM ${dialect.escapeName(table)}`,
          schemaOnly: true,
        },
      }),
    ).then(BigQueryExternal.postProcessIntrospect);
  }

  protected sqlToQuery(sql: string): any {
    const { context } = this;
    const payload: any = { query: sql };
    if (context) {
      for (const key of ['maximumBytesBilled', 'jobTimeoutMs', 'labels']) {
        if (context[key] != null) payload[key] = context[key];
      }
    }
    return payload;
  }

  // -----------------
  // Multi-value columns

  private getMultiValueColumns(): string[] {
    return (this.rawAttributes || this.attributes || [])
      .filter(a => a.nativeType === BigQueryExternal.MULTI_VALUE_NATIVE_TYPE)
      .map(a => a.name);
  }

  private getMultiValuePlan(): MultiValuePlan | null {
    const multiValueColumns = this.getMultiValueColumns();
    if (!multiValueColumns.length) return null;

    const isMultiValueRef = (ex: Expression) =>
      ex instanceof RefExpression && multiValueColumns.includes(ex.name);
    const multiValueRefsOf = (ex: Expression) =>
      ex.getFreeReferences().filter(name => multiValueColumns.includes(name));

    const { dialect } = this;
    const filter = super.getQueryFilter();
    const split = this.split ? super.getQuerySplit() : undefined;

    for (const apply of this.applies || []) {
      // Measure filters sit in a nested scope that getFreeReferences does not report
      const refs: string[] = [];
      this.inlineDerivedAttributes(apply.expression).substitute(ex => {
        if (isMultiValueRef(ex)) refs.push((ex as RefExpression).name);
        return null;
      });
      if (refs.length) {
        throw new Error(
          `multi-value column '${refs[0]}' in measure '${apply.name}' is not supported by the BigQuery external`,
        );
      }
    }

    // Each predicate on one multi-value column is computed per row next to the table
    const predicates: string[] = [];
    const newFilter = filter.substitute(ex => {
      if (
        ex instanceof AndExpression ||
        ex instanceof OrExpression ||
        ex instanceof NotExpression
      ) {
        return null;
      }
      if (ex.type !== 'BOOLEAN') return null;
      const refs = multiValueRefsOf(ex);
      if (!refs.length) return null;
      if (refs.length > 1) {
        throw new Error(`can not filter on two multi-value columns at once: ${ex}`);
      }
      const column = refs[0];
      let predicateSQL: string;
      if (
        (ex instanceof MvContainsExpression || ex instanceof MvOverlapExpression) &&
        isMultiValueRef(ex.operand)
      ) {
        predicateSQL = ex.getSQL(dialect);
      } else {
        const elementPredicate = ex.substitute(sub => (isMultiValueRef(sub) ? $('v') : null));
        predicateSQL = `EXISTS(SELECT 1 FROM ${this.multiValueElements(
          column,
        )} WHERE ${elementPredicate.getSQL(dialect)})`;
      }
      predicates.push(predicateSQL);
      return $(`__mvp_${predicates.length - 1}`, 'BOOLEAN');
    });

    // A split on a multi-value column reads an element column added next to the table
    const constants: Record<string, string> = {};
    const elementColumns: { name: string; array: string }[] = [];
    let newSplit = split;
    if (split) {
      const topLevel = filter instanceof AndExpression ? filter.getExpressionList() : [filter];
      newSplit = split.transformExpressions(ex => {
        const refs = multiValueRefsOf(ex);
        if (!refs.length) return ex;
        if (refs.length > 1) throw new Error(`can not split on two multi-value columns: ${ex}`);
        const column = refs[0];
        const columnSQL = `t.${dialect.escapeName(column)}`;
        if (ex instanceof MvFilterOnlyExpression && isMultiValueRef(ex.operand)) {
          // Only the listed values split; a row left with none is one NULL group, as in Druid
          const name = `__mv__${column}__only${elementColumns.length}`;
          elementColumns.push({
            name,
            array: dialect.mvFilterOnlyExpression(columnSQL, ex.mvArray),
          });
          return $(name, 'STRING');
        }
        const elementName = `__mv__${column}`;
        if (!(column in constants) && !elementColumns.some(e => e.name === elementName)) {
          const pinned = topLevel
            .map(part => BigQueryExternal.pinnedMultiValue(part, column))
            .find(v => v !== undefined);
          if (pinned !== undefined && isMultiValueRef(ex)) {
            constants[column] =
              pinned === null ? 'CAST(NULL AS STRING)' : dialect.escapeLiteral(pinned);
          } else {
            elementColumns.push({ name: elementName, array: columnSQL });
          }
        }
        return ex.substitute(sub => (isMultiValueRef(sub) ? $(`__mv__${column}`, 'STRING') : null));
      });
    }

    if (!predicates.length && !elementColumns.length && !Object.keys(constants).length) return null;
    return {
      filter: newFilter,
      split: newSplit,
      columns: multiValueColumns,
      elementColumns,
      predicates,
      constants,
    };
  }

  /**
   * The value a top level `= 'x'` (or one value IN) or `IS NULL` filter pins a multi-value column
   * to, undefined if the filter part does not pin it.
   */
  static pinnedMultiValue(part: Expression, column: string): string | null | undefined {
    if (!(part instanceof IsExpression)) return undefined;
    const { operand, expression } = part;
    if (!(operand instanceof RefExpression) || operand.name !== column) return undefined;
    if (!(expression instanceof LiteralExpression)) return undefined;
    const value = expression.value;
    if (value === null || typeof value === 'string') return value;
    if (value && Array.isArray(value.elements) && value.elements.length === 1) {
      const element = value.elements[0];
      if (element === null || typeof element === 'string') return element;
    }
    return undefined;
  }

  // Druid treats an empty multi-value row as NULL
  private multiValueElements(column: string): string {
    const name = this.dialect.escapeName(column);
    return `UNNEST(IF(ARRAY_LENGTH(${name})>0,${name},[STRUCT(CAST(NULL AS STRING) AS v)]))`;
  }

  public getQueryFilter(): Expression {
    const plan = this.getMultiValuePlan();
    return plan ? plan.filter : super.getQueryFilter();
  }

  public getQuerySplit(): SplitExpression {
    const plan = this.getMultiValuePlan();
    return plan && plan.split ? plan.split : super.getQuerySplit();
  }

  protected getFrom(): string {
    const { source, withQuery, dialect } = this;
    let relation: string;
    if (Array.isArray(source) && !withQuery) {
      if (!source.length) throw new Error('source array must not be empty');
      // As Druid's TABLE(APPEND(...)): a column missing from one table reads as NULL there
      relation = `(${source
        .map(s => `SELECT * FROM ${dialect.escapeName(s)}`)
        .join(' FULL UNION ALL BY NAME ')})`;
    } else {
      relation = super
        .getFrom()
        .replace(/^FROM /, '')
        .replace(/ AS t$/, '');
    }

    const plan = this.getMultiValuePlan();
    if (!plan) return `FROM ${relation} AS t`;

    const extraColumns = [
      ...plan.predicates.map((sql, i) => `${sql} AS ${dialect.escapeName(`__mvp_${i}`)}`),
      ...Object.keys(plan.constants).map(
        column => `${plan.constants[column]} AS ${dialect.escapeName(`__mv__${column}`)}`,
      ),
      ...plan.elementColumns.map(
        (element, i) => `__mvs_${i}.v AS ${dialect.escapeName(element.name)}`,
      ),
    ];
    const joins = plan.elementColumns.map(
      (element, i) => `LEFT JOIN UNNEST(${element.array}) AS __mvs_${i}`,
    );
    return `FROM (SELECT t.*, ${extraColumns.join(', ')} FROM ${relation} AS t${joins
      .map(join => ' ' + join)
      .join('')}) AS t`;
  }
}

External.register(BigQueryExternal);
