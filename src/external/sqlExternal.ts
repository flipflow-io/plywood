/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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

import { PlywoodRequester } from 'plywood-base-api';
import { Transform } from 'readable-stream';

import { Attributes } from '../datatypes/attributeInfo';
import { SQLDialect } from '../dialect/baseDialect';
import {
  ApplyExpression,
  Expression,
  FilterExpression,
  SortExpression,
  SplitExpression,
  SqlAggregateExpression,
} from '../expressions';

import {
  External,
  ExternalJS,
  ExternalValue,
  Inflater,
  IntrospectionDepth,
  QueryAndPostTransform,
} from './baseExternal';

function getSplitInflaters(split: SplitExpression): Inflater[] {
  return split.mapSplits((label, splitExpression) => {
    const simpleInflater = External.getIntelligentInflater(splitExpression, label);
    if (simpleInflater) return simpleInflater;
    return undefined;
  });
}

export abstract class SQLExternal extends External {
  static type = 'DATASET';

  static jsToValue(parameters: ExternalJS, requester: PlywoodRequester<any>): ExternalValue {
    const value: ExternalValue = External.jsToValue(parameters, requester);
    value.withQuery = parameters.withQuery;
    return value;
  }

  public withQuery?: string;

  public dialect: SQLDialect;

  constructor(parameters: ExternalValue, dialect: SQLDialect) {
    super(parameters, dummyObject);
    this.withQuery = parameters.withQuery;
    this.dialect = dialect;
  }

  public valueOf(): ExternalValue {
    const value: ExternalValue = super.valueOf();
    value.withQuery = this.withQuery;
    return value;
  }

  // -----------------

  public canHandleFilter(_filter: FilterExpression): boolean {
    return true;
  }

  public canHandleSort(_sort: SortExpression): boolean {
    return true;
  }

  protected capability(cap: string): boolean {
    if (cap === 'filter-on-attribute' || cap === 'shortcut-group-by') return true;
    return super.capability(cap);
  }

  // -----------------

  protected sqlToQuery(sql: string): any {
    return sql;
  }

  protected getFrom(): string {
    const { source, dialect, withQuery } = this;
    if (withQuery) {
      return `FROM __with__ AS t`;
    }

    return `FROM ${dialect.escapeName(source as string)} AS t`;
  }

  public getQueryAndPostTransform(): QueryAndPostTransform<string> {
    const { mode, applies, sort, limit, derivedAttributes, dialect, withQuery } = this;

    const queryParts = [];
    const cteDefinitions: string[] = [];

    if (withQuery) {
      queryParts.push(`WITH __with__ AS (${withQuery})`);
    }

    let postTransform: Transform = null;
    let inflaters: Inflater[] = [];
    let keys: string[] = null;
    let zeroTotalApplies: ApplyExpression[] = null;

    let selectExpressions: string[] = [];
    let fromClause: string = this.getFrom();
    let groupByExpressions: string[] = [];

    // dialect.setTable(null);
    fromClause = this.getFrom();
    // dialect.setTable(source as string);

    const filter = this.getQueryFilter();
    if (!filter.equals(Expression.TRUE)) {
      fromClause += '\nWHERE ' + filter.getSQL(dialect);
    }

    let selectedAttributes = this.getSelectedAttributes();
    switch (mode) {
      case 'raw':
        selectedAttributes = selectedAttributes.map(a => a.dropOriginInfo());

        inflaters = selectedAttributes
          .map(attribute => {
            const { name, type } = attribute;
            switch (type) {
              case 'BOOLEAN':
                return External.booleanInflaterFactory(name);

              case 'TIME':
                return External.timeInflaterFactory(name);

              case 'IP':
                return External.ipInflaterFactory(name);

              case 'SET/STRING':
                return External.setStringInflaterFactory(name);

              default:
                return null;
            }
          })
          .filter(Boolean);

        queryParts.push(
          selectedAttributes
            .map(a => {
              const name = a.name;
              if (derivedAttributes[name]) {
                return Expression._.apply(name, derivedAttributes[name]).getSQL(dialect);
              } else {
                return dialect.escapeName(name);
              }
            })
            .join(', '),
          fromClause,
        );
        if (sort) {
          queryParts.push(sort.getSQL(dialect));
        }
        if (limit) {
          queryParts.push(limit.getSQL(dialect));
        }
        break;

      case 'value':
        queryParts.push(this.toValueApply().getSQL(dialect), fromClause, dialect.emptyGroupBy());
        postTransform = External.valuePostTransformFactory();
        break;

      case 'total':
        zeroTotalApplies = applies;
        inflaters = applies
          .map(apply => {
            const { name, expression } = apply;
            return External.getSimpleInflater(expression.type, name);
          })
          .filter(Boolean);

        keys = [];
        queryParts.push(
          applies.map(apply => apply.getSQL(dialect)).join(',\n'),
          fromClause,
          dialect.emptyGroupBy(),
        );
        break;

      case 'split': {
        const split = this.getQuerySplit();
        keys = split.mapSplits(name => name);

        selectExpressions = split.getSelectSQL(dialect);
        groupByExpressions = this.capability('shortcut-group-by')
          ? split.getShortGroupBySQL()
          : split.getGroupBySQL(dialect);

        // Procesar aplicaciones que utilizan PIVOT_NESTED_AGG
        const pivotNestedAggApplies: {
          [subSplitExpression: string]: {
            apply: ApplyExpression;
            components: {
              subSplitExpression: string;
              innerAggregateExpr: string;
              innerAggregateValueAlias: string;
              outerAggregateExpr: string;
            };
          }[];
        } = {};

        const otherApplies: ApplyExpression[] = [];

        applies.forEach(apply => {
          if (apply.expression instanceof SqlAggregateExpression) {
            const sqlAggregateExpression = apply.expression;
            const pivotNestedAggComponents =
              sqlAggregateExpression.extractPivotNestedAggComponents();
            if (pivotNestedAggComponents) {
              const subSplitExpression = pivotNestedAggComponents.subSplitExpression.trim();

              if (!pivotNestedAggApplies[subSplitExpression]) {
                pivotNestedAggApplies[subSplitExpression] = [];
              }
              pivotNestedAggApplies[subSplitExpression].push({
                apply,
                components: pivotNestedAggComponents,
              });
            } else {
              // Si no es PIVOT_NESTED_AGG, procesar normalmente
              selectExpressions.push(apply.getSQL(dialect));
            }
          } else {
            selectExpressions.push(apply.getSQL(dialect));
          }
        });

        if (Object.keys(pivotNestedAggApplies).length > 1) {
          throw new Error(
            'Can not have more than one different re-split in a query, use JOIN for that',
          );
        }

        // Si hay aplicaciones con PIVOT_NESTED_AGG, generar la CTE
        if (Object.keys(pivotNestedAggApplies).length === 1) {
          const subSplitExpression = Object.keys(pivotNestedAggApplies)[0];
          const pnaApplies = pivotNestedAggApplies[subSplitExpression];

          // Construir las expresiones de selección y agrupación internas
          // @ts-expect-error
          let innerSelectExpressions = new Set({ setType: 'STRING', elements: [] });
          // @ts-expect-error
          let innerGroupByExpressions = new Set({ setType: 'STRING', elements: [] });

          // Añadir la expresión de subdivisión interna
          innerSelectExpressions = innerSelectExpressions.add(`${subSplitExpression} AS "sub_key"`);
          innerGroupByExpressions = innerGroupByExpressions.add(subSplitExpression);

          // Añadir las expresiones de selección del split
          split
            .getSelectSQLNoAliases(dialect)
            .forEach(expr => (innerSelectExpressions = innerSelectExpressions.add(expr)));
          split
            .getSelectSQLNoAliases(dialect)
            .forEach(expr => (innerGroupByExpressions = innerGroupByExpressions.add(expr)));

          // Añadir las expresiones agregadas internas
          pnaApplies.forEach(({ components }) => {
            innerSelectExpressions = innerSelectExpressions.add(
              `${components.innerAggregateExpr} AS "${components.innerAggregateValueAlias}"`,
            );
          });

          const innerWhereClause = !filter.equals(Expression.TRUE)
            ? 'WHERE ' + filter.getSQL(dialect)
            : '';

          // @ts-expect-error
          const items = innerSelectExpressions.elements;
          // @ts-expect-error
          const groupItems = innerGroupByExpressions.elements;
          const innerAggregateSQL = `
          SELECT
            ${items.join(',\n')}
            ${this.getFrom()}
            ${innerWhereClause}
          GROUP BY
            ${groupItems.join(', ')}
        `;

          // Agregar la definición de la CTE
          const cteName = `cte_pivot_nested_agg`;
          cteDefinitions.push(`${cteName} AS (${innerAggregateSQL})`);

          // Ajustar el FROM para usar la CTE
          fromClause = `FROM ${cteName} AS t`;

          // Construir las expresiones de selección externas
          pnaApplies.forEach(({ apply, components }) => {
            selectExpressions.push(
              `${components.outerAggregateExpr.replace(/t\./g, 't.')} AS ${dialect.escapeName(
                apply.name,
              )}`,
            );
          });
        }

        // Construir la cláusula WITH si hay CTEs
        if (cteDefinitions.length > 0) {
          queryParts.unshift(`WITH\n  ${cteDefinitions.join(',\n  ')}`);
        }

        // Construir las partes principales de la consulta
        queryParts.push(
          'SELECT',
          selectExpressions.join(',\n'),
          fromClause,
          groupByExpressions.length > 0 ? 'GROUP BY ' + groupByExpressions.join(', ') : '',
        );

        // Manejar HAVING, ORDER BY y LIMIT
        if (!this.havingFilter.equals(Expression.TRUE)) {
          queryParts.push('HAVING ' + this.havingFilter.getSQL(dialect));
        }
        if (sort) {
          queryParts.push(sort.getSQL(dialect));
        }
        if (limit) {
          queryParts.push(limit.getSQL(dialect));
        }
        inflaters = getSplitInflaters(split);
        break;
      }
      default:
        throw new Error(`can not get query for mode: ${mode}`);
    }

    return {
      query: this.sqlToQuery(queryParts.join('\n')),
      postTransform:
        postTransform ||
        External.postTransformFactory(inflaters, selectedAttributes, keys, zeroTotalApplies),
    };
  }

  protected abstract getIntrospectAttributes(depth: IntrospectionDepth): Promise<Attributes>;
}
