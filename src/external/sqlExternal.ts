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
  $,
  ApplyExpression,
  ChainableExpression,
  ChainableUnaryExpression,
  CountExpression,
  Expression,
  FilterExpression,
  NumberBucketExpression,
  RefExpression,
  SortExpression,
  SplitExpression,
  SqlAggregateExpression,
  TimeBucketExpression,
} from '../expressions';

import {
  External,
  ExternalJS,
  ExternalValue,
  Inflater,
  IntrospectionDepth,
  QueryAndPostTransform,
} from './baseExternal';
import { buildResplitAggregationSQL } from './utils/resplitAggregationSQLBuilder';
import { AttributeInfo } from '../datatypes/attributeInfo';

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
      cteDefinitions.push(`__with__ AS (${withQuery})`);
    }

    let postTransform: Transform = null;
    let inflaters: Inflater[] = [];
    let keys: string[] = null;
    let zeroTotalApplies: ApplyExpression[] = null;

    let selectExpressions: string[] = [];
    let fromClause: string = this.getFrom();
    let groupByExpressions: string[] = [];

    // Check for nested group by (resplit aggregation)
    const nestedGroupByResult = this.nestedGroupByIfNeeded();
    if (nestedGroupByResult) {
      cteDefinitions.push(...nestedGroupByResult.cteDefinitions);
      fromClause = nestedGroupByResult.fromClause;
      selectExpressions.push(...nestedGroupByResult.selectExpressions);
      groupByExpressions = nestedGroupByResult.groupByExpressions;
    }

    // dialect.setTable(null);
    if (!nestedGroupByResult) {
      fromClause = this.getFrom();
    }
    // dialect.setTable(source as string);

    const filter = this.getQueryFilter();
    const whereClause = !filter.equals(Expression.TRUE) ? 'WHERE ' + filter.getSQL(dialect) : '';
    if (!nestedGroupByResult && !filter.equals(Expression.TRUE)) {
      fromClause += '\n' + whereClause;
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

    // If nestedGroupByResult exists, it already built the query parts
    // Otherwise, build them now
    if (!nestedGroupByResult) {
      // Build WITH clause if there are CTEs
      if (cteDefinitions.length > 0) {
        queryParts.unshift(`WITH\n  ${cteDefinitions.join(',\n  ')}`);
      }

      // Build SELECT clause if not already built
      if (selectExpressions.length === 0) {
        selectExpressions.push('1');
      }

      queryParts.push('SELECT ' + selectExpressions.join(', '));
      queryParts.push(fromClause);
      if (groupByExpressions && groupByExpressions.length > 0) {
        queryParts.push('GROUP BY ' + groupByExpressions.join(', '));
      }

      // Add HAVING, ORDER BY, LIMIT
      if (!this.havingFilter.equals(Expression.TRUE)) {
        queryParts.push('HAVING ' + this.havingFilter.getSQL(dialect));
      }
      if (sort) {
        queryParts.push(sort.getSQL(dialect));
      }
      if (limit) {
        queryParts.push(limit.getSQL(dialect));
      }
    } else {
      // nestedGroupByResult already has SELECT, FROM, GROUP BY
      // Just add HAVING, ORDER BY, LIMIT if needed
      if (!this.havingFilter.equals(Expression.TRUE)) {
        queryParts.push('HAVING ' + this.havingFilter.getSQL(dialect));
      }
      if (sort) {
        queryParts.push(sort.getSQL(dialect));
      }
      if (limit) {
        queryParts.push(limit.getSQL(dialect));
      }
    }

    return {
      query: this.sqlToQuery(queryParts.join('\n')),
      postTransform:
        postTransform ||
        External.postTransformFactory(inflaters, selectedAttributes, keys, zeroTotalApplies),
    };
  }

  protected abstract getIntrospectAttributes(depth: IntrospectionDepth): Promise<Attributes>;

  // -----------------
  // Resplit Aggregation Support

  /**
   * Parses a resplit aggregation expression.
   * A resplit aggregation is an aggregate expression that contains a split inside it,
   * like: $main.split($url).apply('qty', ...).sum($qty)
   */
  static parseResplitAgg(ex: Expression): {
    resplitAgg: Expression;
    resplitApply: ApplyExpression;
    resplitSplit: SplitExpression;
  } | null {
    if (ex instanceof ChainableExpression) {
      if (ex.isAggregate()) {
        const operand = ex.operand;
        if (operand instanceof ApplyExpression) {
          const resplitApply = operand;
          if (resplitApply.operand instanceof SplitExpression) {
            const resplitSplit = resplitApply.operand;
            return {
              resplitAgg: ex.changeOperand(Expression._),
              resplitApply: resplitApply.changeOperand(Expression._),
              resplitSplit: resplitSplit.changeOperand(Expression._),
            };
          }
        }
      }
      const resplitInOperand = SQLExternal.parseResplitAgg(ex.operand);
      if (resplitInOperand) {
        return resplitInOperand;
      }
      if (ex instanceof ChainableUnaryExpression) {
        const resplitInExpression = SQLExternal.parseResplitAgg(ex.expression);
        if (resplitInExpression) {
          return resplitInExpression;
        }
      }
    }
    return null;
  }

  /**
   * Builds nested GROUP BY SQL if needed (when there are resplit aggregations).
   * This method handles derivationScope: inner and additional splits via lookup CTE.
   */
  public nestedGroupByIfNeeded(): {
    cteDefinitions: string[];
    selectExpressions: string[];
    fromClause: string;
    groupByExpressions: string[];
  } | null {
    const divvyUpNestedSplitExpression = (
      splitExpression: Expression,
      intermediateName: string,
    ): { inner: Expression; outer: Expression } => {
      if (
        splitExpression instanceof TimeBucketExpression ||
        splitExpression instanceof NumberBucketExpression
      ) {
        return {
          inner: splitExpression,
          outer: splitExpression.changeOperand($(intermediateName)),
        };
      } else {
        return {
          inner: splitExpression,
          outer: $(intermediateName),
        };
      }
    };

    const { applies, split, dialect } = this;
    const effectiveApplies = applies
      ? applies
      : [Expression._.apply('__VALUE__', this.valueExpression)];

    // Check for early exit condition - if there are no applies with splits in them then there is nothing to do.
    if (
      !effectiveApplies.some(apply => {
        return apply.expression.some(ex => (ex instanceof SplitExpression ? true : null));
      })
    )
      return null;

    // Split up applies
    let globalResplitSplit: SplitExpression = null;
    const outerAttributes: AttributeInfo[] = [];
    const innerApplies: ApplyExpression[] = [];
    const usedAliases = new Map<string, boolean>();

    const getMetricNameFromExpression = (expression: Expression): string => {
      if (expression instanceof ChainableUnaryExpression) {
        const operand = expression.operand;
        if (operand instanceof RefExpression) {
          return operand.name;
        }
        if (expression.expression instanceof RefExpression) {
          return expression.expression.name;
        }
      }
      const exprString = expression.toString();
      const match = exprString.match(/\$([a-zA-Z0-9]+)/);
      if (match) {
        return match[1];
      }
      return exprString.replace(/[^a-zA-Z0-9]/g, '').substring(0, 10);
    };

    const getUniqueAlias = (applyName: string, expression: Expression, index: number): string => {
      const metricName = getMetricNameFromExpression(expression);
      let baseAlias = `${applyName}_${metricName}_${index}`;
      if (baseAlias.length > 30) {
        baseAlias = `${applyName}_${metricName.substring(0, 10)}_${index}`;
      }
      let alias = baseAlias;
      let counter = 1;
      while (usedAliases.has(alias)) {
        alias = `${baseAlias}_${counter++}`;
      }
      usedAliases.set(alias, true);
      return alias;
    };

    const outerApplies = effectiveApplies.map((apply, i) => {
      let c = 0;
      return apply.changeExpression(
        apply.expression.substitute(ex => {
          if (ex.isAggregate()) {
            const resplit = SQLExternal.parseResplitAgg(ex);
            if (resplit) {
              if (globalResplitSplit) {
                if (!globalResplitSplit.equals(resplit.resplitSplit))
                  throw new Error('All resplit aggregators must have the same split');
              } else {
                globalResplitSplit = resplit.resplitSplit;
              }

              const resplitApply = resplit.resplitApply;
              const oldName = resplitApply.name;
              const newName = getUniqueAlias(oldName, resplitApply.expression, i);

              innerApplies.push(
                resplitApply.changeName(newName).changeExpression(resplitApply.expression),
              );
              outerAttributes.push(AttributeInfo.fromJS({ name: newName, type: 'NUMBER' }));

              const resplitAggWithUpdatedNames = resplit.resplitAgg.substitute(ex => {
                if (ex instanceof RefExpression && ex.name === oldName) {
                  return ex.changeName(newName);
                }
                return null;
              }) as ChainableExpression;

              return resplitAggWithUpdatedNames;
            } else {
              const tempName = getUniqueAlias(`a${i}`, ex, c++);
              innerApplies.push(Expression._.apply(tempName, ex));
              outerAttributes.push(
                AttributeInfo.fromJS({
                  name: tempName,
                  type: ex.type,
                }),
              );
              if (ex instanceof CountExpression) {
                return Expression._.sum($(tempName));
              } else if (ex instanceof SqlAggregateExpression) {
                return $(tempName);
              } else if (ex instanceof ChainableUnaryExpression) {
                return ex.changeOperand(Expression._).changeExpression($(tempName));
              } else {
                throw new Error(`Unsupported aggregate in custom expression: ${ex.op}`);
              }
            }
          }
          return null;
        }),
      );
    });

    if (!globalResplitSplit) return null;

    // Build resplit aggregation SQL using modular functions
    const filter = this.getQueryFilter();
    const whereClause = !filter.equals(Expression.TRUE) ? 'WHERE ' + filter.getSQL(dialect) : '';

    const result = buildResplitAggregationSQL(
      globalResplitSplit,
      split,
      innerApplies,
      outerApplies,
      () => this.getFrom(),
      whereClause,
      dialect,
      divvyUpNestedSplitExpression,
      (sql: string, cteColumnNames: string[], _dialect: SQLDialect) => {
        return this._wrapCTEReferencesWithAnyValue(sql, cteColumnNames, _dialect);
      },
    );

    // Update outerAttributes (needed for post-transform)
    outerAttributes.length = 0;
    outerAttributes.push(...result.outerAttributes);

    return {
      cteDefinitions: result.cteDefinitions,
      selectExpressions: result.selectExpressions,
      fromClause: result.fromClause,
      groupByExpressions: result.groupByExpressions,
    };
  }

  protected _getAnyValueFunction(): string {
    const dialectName = this.dialect.constructor.name;
    if (dialectName === 'DruidDialect') {
      return 'ANY_VALUE';
    } else if (dialectName === 'PostgresDialect') {
      return 'FIRST';
    } else if (dialectName === 'MySqlDialect') {
      return 'ANY_VALUE';
    } else {
      return 'ANY_VALUE';
    }
  }

  protected _wrapCTEReferencesWithAnyValue(
    sqlExpression: string,
    cteColumnNames: string[],
    _dialect: SQLDialect,
  ): string {
    if (!cteColumnNames || cteColumnNames.length === 0) {
      return sqlExpression;
    }
    const isCompleteAggregateExpression = /^(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|GROUP_CONCAT|LISTAGG|ARRAY_AGG|STRING_AGG|BIT_AND|BIT_OR|BIT_XOR|BOOL_AND|BOOL_OR|CORR|COVAR_POP|COVAR_SAMP|EVERY|REGR_|PERCENTILE_|MEDIAN|ANY_VALUE)\s*\(/i.test(
      sqlExpression,
    );
    if (isCompleteAggregateExpression) {
      return sqlExpression;
    }
    const sortedColumnNames = cteColumnNames.slice().sort((a, b) => b.length - a.length);
    let processedExpression = sqlExpression;
    for (const columnName of sortedColumnNames) {
      const quotedColumnPattern = new RegExp(`"${columnName.replace(/"/g, '""')}"`, 'g');
      if (quotedColumnPattern.test(processedExpression)) {
        const aggregatePattern = new RegExp(
          `\\b(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|GROUP_CONCAT|LISTAGG|ARRAY_AGG|STRING_AGG|BIT_AND|BIT_OR|BIT_XOR|BOOL_AND|BOOL_OR|CORR|COVAR_POP|COVAR_SAMP|EVERY|REGR_|PERCENTILE_|MEDIAN|ANY_VALUE)\\s*\\([^)]*"${columnName.replace(/"/g, '""')}"[^)]*\\)`,
          'i',
        );
        if (!aggregatePattern.test(processedExpression)) {
          const anyValueFn = this._getAnyValueFunction();
          processedExpression = processedExpression.replace(
            quotedColumnPattern,
            `${anyValueFn}("${columnName}")`,
          );
        }
      }
    }
    return processedExpression;
  }
}
