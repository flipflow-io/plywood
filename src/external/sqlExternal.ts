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

import { Duration, Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { PlywoodRequester } from 'plywood-base-api';
import { Transform } from 'readable-stream';

import { AttributeInfo, Attributes } from '../datatypes/attributeInfo';
import { TimeRange } from '../datatypes/timeRange';
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
  Splits,
  SqlAggregateExpression,
  TimeBucketExpression,
  TimeFloorExpression,
} from '../expressions';

import {
  External,
  ExternalJS,
  ExternalValue,
  Inflater,
  IntrospectionDepth,
  QueryAndPostTransform,
} from './baseExternal';
import { ParsedResplitAgg } from './druidExternal';

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

  private inlineDerivedAttributess(expression: Expression): Expression {
    const { derivedAttributes } = this;
    return expression.substitute(refEx => {
      if (refEx instanceof RefExpression) {
        return derivedAttributes[refEx.name] || null;
      } else {
        return null;
      }
    });
  }

  static timeRangeInflaterFactoryFromSQL(
    label: string,
    duration: Duration,
    timezone: Timezone,
  ): Inflater {
    return (d: any) => {
      const v = d[label];
      if ('' + v === 'null' || typeof v === 'undefined') {
        d[label] = null;
        return;
      }
      const start = new Date(v);
      d[label] = new TimeRange({
        start,
        end: duration.shift(start, timezone),
      });
    };
  }

  public getQueryAndPostTransform(): QueryAndPostTransform<string> {
    let groupByExpressions;
    const { mode, sort, limit, derivedAttributes, dialect, withQuery } = this;
    let { applies } = this;
    const queryParts: string[] = [];
    const cteDefinitions: string[] = [];
    const timeBucketExpressions: { label: string; duration: Duration; timezone: Timezone }[] = [];
    let postTransform: Transform = null;
    let inflaters: Inflater[] = [];
    let keys: string[] = null;
    let zeroTotalApplies: ApplyExpression[] = null;
    const selectExpressions: string[] = [];
    let fromClause: string = this.getFrom();
    const filter = this.getQueryFilter();

    // Handle the WITH clause if 'withQuery' is provided
    if (withQuery) {
      cteDefinitions.push(`__with__ AS (${withQuery})`);
      fromClause = `FROM __with__ AS t`;
    }

    // Handle nested group by if needed
    const nestedGroupByResult = this.nestedGroupByIfNeeded();
    if (nestedGroupByResult) {
      // Update 'cteDefinitions' and 'fromClause'
      cteDefinitions.push(...nestedGroupByResult.cteDefinitions);
      fromClause = nestedGroupByResult.fromClause;
      // Update 'selectExpressions'
      selectExpressions.push(...nestedGroupByResult.selectExpressions);
      // Use the 'groupByExpressions' from nestedGroupByResult if any
      groupByExpressions = nestedGroupByResult.groupByExpressions;
    }

    // Add WHERE clause if there's a filter
    const whereClause = !filter.equals(Expression.TRUE) ? 'WHERE ' + filter.getSQL(dialect) : '';

    // Proceed with the usual logic based on the mode
    const selectedAttributes = this.getSelectedAttributes();

    // Decompose averages in applies before processing them
    applies = !applies ? [Expression._.apply('__VALUE__', this.valueExpression)] : applies;

    const decomposedApplies = applies.map(apply => {
      let expression = apply.expression;
      // Inline derived attributes if necessary
      if (derivedAttributes) {
        expression = this.inlineDerivedAttributess(expression);
      }
      // Decompose average expressions into sum and count
      expression = expression.decomposeAverage();
      // Return a new apply with the decomposed expression
      return apply.changeExpression(expression);
    });

    switch (mode) {
      case 'raw': {
        if (!nestedGroupByResult) {
          // Build select expressions
          const attributeExpressions = selectedAttributes.map(a => {
            const name = a.name;
            if (derivedAttributes && derivedAttributes[name]) {
              return Expression._.apply(name, derivedAttributes[name]).getSQL(dialect);
            } else {
              return dialect.escapeName(name);
            }
          });
          selectExpressions.push(...attributeExpressions);
          inflaters = selectedAttributes
            .map(attribute => {
              const { name, type } = attribute;
              switch (type) {
                case 'BOOLEAN':
                  return External.booleanInflaterFactory(name);
                case 'TIME':
                  return External.timeInflaterFactory(name);
                case 'SET/STRING':
                  return External.setStringInflaterFactory(name);
                default:
                  return null;
              }
            })
            .filter(Boolean);
        }
        break;
      }
      case 'value': {
        if (!nestedGroupByResult) {
          if (applies.length > 0) {
            selectExpressions.push(this.toValueApply().getSQL(dialect));
          } else {
            // If no applies, select a constant value (e.g., 1) to ensure valid SQL
            selectExpressions.push('1');
          }
          postTransform = External.valuePostTransformFactory();
        }
        break;
      }
      case 'total': {
        keys = [];
        if (!nestedGroupByResult) {
          if (decomposedApplies.length > 0) {
            // Use decomposed applies
            decomposedApplies.forEach(apply => {
              const name = apply.name;
              const expression = apply.expression;
              const sql = apply.getSQL(dialect);
              selectExpressions.push(sql);
              inflaters.push(External.getSimpleInflater(expression.type, name));
            });
            zeroTotalApplies = decomposedApplies;
          } else {
            // If no applies, select a constant value (e.g., COUNT(*))
            selectExpressions.push('COUNT(*) AS "Count"');
            inflaters.push(External.getSimpleInflater('NUMBER', 'Count'));
          }
        }
        break;
      }
      case 'split': {
        const split = this.getQuerySplit();
        keys = split.mapSplits(name => name);
        split.mapSplits((label, splitExpression) => {
          if (
            splitExpression instanceof TimeBucketExpression ||
            splitExpression instanceof TimeFloorExpression
          ) {
            const duration = splitExpression.duration;
            const timezone = splitExpression.getTimezone();
            timeBucketExpressions.push({ label, duration, timezone });
          }
        });
        timeBucketExpressions.forEach(({ label, duration, timezone }) => {
          inflaters.push(SQLExternal.timeRangeInflaterFactoryFromSQL(label, duration, timezone));
        });
        if (!nestedGroupByResult) {
          // Build select expressions
          selectExpressions.push(...split.getSelectSQL(dialect));
          if (decomposedApplies.length > 0) {
            // Process decomposed applies
            decomposedApplies.forEach(apply => {
              const name = apply.name;
              const expression = apply.expression;
              const sql = apply.getSQL(dialect);
              selectExpressions.push(sql);
              inflaters.push(External.getSimpleInflater(expression.type, name));
            });
          } else {
            // If no applies, select a constant value
            selectExpressions.push('COUNT(*) AS "Count"');
            inflaters.push(External.getSimpleInflater('NUMBER', 'Count'));
          }
          // Add GROUP BY clause
          groupByExpressions = split.getShortGroupBySQL();
        }
        break;
      }
      default:
        throw new Error(`Cannot get query for mode: ${mode}`);
    }

    // Assemble the query parts in correct order
    if (cteDefinitions.length > 0) {
      queryParts.push(`WITH\n${cteDefinitions.join(',\n')}`);
    }
    if (selectExpressions.length === 0) {
      // Ensure there is at least one select expression
      selectExpressions.push('1');
    }
    queryParts.push('SELECT ' + selectExpressions.join(', '));
    queryParts.push(fromClause); // 'FROM' clause
    if (whereClause && !nestedGroupByResult) {
      queryParts.push(whereClause);
    }
    if (groupByExpressions && groupByExpressions.length > 0) {
      queryParts.push('GROUP BY ' + groupByExpressions.join(', '));
    }
    // Add HAVING clause after GROUP BY
    if (this.havingFilter && !this.havingFilter.equals(Expression.TRUE)) {
      queryParts.push('HAVING ' + this.havingFilter.getSQL(dialect));
    }
    if (sort) {
      queryParts.push(sort.getSQL(dialect));
    }
    if (limit) {
      queryParts.push(limit.getSQL(dialect));
    }

    // Combine the query parts
    const query = queryParts.join('\n');

    // Modify postTransform to handle averages
    if (!postTransform) {
      postTransform = External.postTransformFactory(
        inflaters,
        selectedAttributes,
        keys,
        zeroTotalApplies,
      );
    }

    return {
      query: this.sqlToQuery(query),
      postTransform,
    };
  }

  private nestedGroupByIfNeeded(): {
    selectExpressions: string[];
    fromClause: string;
    cteDefinitions: string[];
    groupByExpressions?: string[];
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

    // Early exit if there are no applies with splits in them
    if (
      !effectiveApplies.some(apply => {
        return apply.expression.some(ex => (ex instanceof SplitExpression ? true : null));
      })
    )
      return null;

    // Split up applies
    let globalResplitSplit: SplitExpression = null;
    const outerAttributes: Attributes = [];
    const innerApplies: ApplyExpression[] = [];
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
              const newName = `${oldName}_${i}`;

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
              const tempName = `a${i}_${c++}`;
              innerApplies.push(Expression._.apply(tempName, ex));
              outerAttributes.push(
                AttributeInfo.fromJS({
                  name: tempName,
                  type: ex.type,
                }),
              );

              if (ex instanceof CountExpression) {
                return Expression._.sum($(tempName));
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

    const outerSplits: Splits = {};
    const innerSplits: Splits = {};

    let splitCount = 0;
    globalResplitSplit.mapSplits((name, ex) => {
      let outerSplitName = null;
      if (split) {
        split.mapSplits((sname, sex) => {
          if (ex.equals(sex)) {
            outerSplitName = sname;
          }
        });
      }

      const intermediateName = `s${splitCount++}`;
      const divvy = divvyUpNestedSplitExpression(ex, intermediateName);
      outerAttributes.push(
        AttributeInfo.fromJS({ name: intermediateName, type: divvy.inner.type }),
      );
      innerSplits[intermediateName] = divvy.inner;
      if (outerSplitName) {
        outerSplits[outerSplitName] = divvy.outer;
      }
    });

    if (split) {
      split.mapSplits((name, ex) => {
        if (outerSplits[name]) return; // already taken care of
        const intermediateName = `s${splitCount++}`;
        const divvy = divvyUpNestedSplitExpression(ex, intermediateName);
        innerSplits[intermediateName] = divvy.inner;
        outerAttributes.push(
          AttributeInfo.fromJS({ name: intermediateName, type: divvy.inner.type }),
        );
        outerSplits[name] = divvy.outer;
      });
    }

    // Build inner query (CTE)
    const innerSelectExpressions: string[] = [];
    const innerGroupByExpressions: string[] = [];

    // Add inner splits to inner query
    for (const intermediateName in innerSplits) {
      if (!hasOwnProp(innerSplits, intermediateName)) continue;
      const expression = innerSplits[intermediateName];
      const sql = expression.getSQL(dialect);
      innerSelectExpressions.push(`${sql} AS ${dialect.escapeName(intermediateName)}`);
      innerGroupByExpressions.push(sql);
    }

    // Add inner applies to inner query
    innerApplies.forEach(apply => {
      const sql = apply.getSQL(dialect);
      innerSelectExpressions.push(sql);
    });

    // Build inner query
    const filter = this.getQueryFilter();
    const innerWhereClause = !filter.equals(Expression.TRUE)
      ? 'WHERE ' + filter.getSQL(dialect)
      : '';

    const innerQueryParts = [
      'SELECT ' + innerSelectExpressions.join(',\n'),
      this.getFrom(),
      innerWhereClause,
      'GROUP BY ' + innerGroupByExpressions.join(', '),
    ].filter(Boolean);

    const cteName = 'cte_subsplit';
    const innerQuery = innerQueryParts.join('\n');
    const cteDefinition = `${cteName} AS (\n${innerQuery}\n)`;

    // Adjust fromClause to use the CTE
    const fromClause = `FROM ${cteName} AS t`;

    // Build outer select expressions
    const selectExpressions: string[] = [];
    const groupByExpressions: string[] = [];

    for (const name in outerSplits) {
      if (!hasOwnProp(outerSplits, name)) continue;
      const expression = outerSplits[name];
      const sql = expression.getSQL(dialect);
      const alias = dialect.escapeName(name);
      selectExpressions.push(`${sql} AS ${alias}`);
      groupByExpressions.push(sql);
    }

    // Add outer applies
    outerApplies.forEach(apply => {
      const sql = apply.getSQL(dialect);
      selectExpressions.push(sql);
    });

    return {
      selectExpressions,
      fromClause,
      cteDefinitions: [cteDefinition],
      groupByExpressions,
    };
  }

  static parseResplitAgg(ex: Expression): ParsedResplitAgg | null {
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
      // Recursively search within operand
      const resplitInOperand = SQLExternal.parseResplitAgg(ex.operand);
      if (resplitInOperand) {
        return resplitInOperand;
      }
      // For ChainableUnaryExpression, recursively search within expression
      if (ex instanceof ChainableUnaryExpression) {
        const resplitInExpression = SQLExternal.parseResplitAgg(ex.expression);
        if (resplitInExpression) {
          return resplitInExpression;
        }
      }
    }
    return null;
  }

  private processPivotNestedAggApplies2(
    applies: ApplyExpression[],
    dialect: SQLDialect,
    filter: Expression,
    split?: SplitExpression,
  ): {
    selectExpressions: string[];
    fromClause: string;
    cteDefinitions: string[];
  } {
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

    const selectExpressions: string[] = [];
    const cteDefinitions: string[] = [];
    let fromClause = this.getFrom();

    applies.forEach(apply => {
      if (apply.expression instanceof SqlAggregateExpression) {
        const sqlAggregateExpression = apply.expression;
        const pivotNestedAggComponents = sqlAggregateExpression.extractPivotNestedAggComponents();
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
          // If not PIVOT_NESTED_AGG, process normally
          selectExpressions.push(apply.getSQL(dialect));
        }
      } else {
        selectExpressions.push(apply.getSQL(dialect));
      }
    });

    if (Object.keys(pivotNestedAggApplies).length > 1) {
      throw new Error('Cannot have more than one different re-split in a query, use JOIN for that');
    }

    // If there are applies with PIVOT_NESTED_AGG, generate the CTE
    if (Object.keys(pivotNestedAggApplies).length === 1) {
      const subSplitExpression = Object.keys(pivotNestedAggApplies)[0];
      const pnaApplies = pivotNestedAggApplies[subSplitExpression];

      // Use your custom Set class as before
      // Initialize innerSelectExpressions and innerGroupByExpressions as empty Sets
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-expect-error
      let innerSelectExpressions = new Set({ setType: 'STRING', elements: [] });
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-expect-error
      let innerGroupByExpressions = new Set({ setType: 'STRING', elements: [] });

      // Add the inner sub-split expression
      innerSelectExpressions = innerSelectExpressions.add(`${subSplitExpression} AS "sub_key"`);
      innerGroupByExpressions = innerGroupByExpressions.add(subSplitExpression);

      // If there is a split, add its select and group by expressions
      if (split) {
        split.getSelectSQL(dialect).forEach(expr => {
          innerSelectExpressions = innerSelectExpressions.add(expr);
        });
        split.getSelectSQLNoAliases(dialect).forEach(expr => {
          innerGroupByExpressions = innerGroupByExpressions.add(expr);
        });
      }

      // Add the inner aggregate expressions
      pnaApplies.forEach(({ components }) => {
        innerSelectExpressions = innerSelectExpressions.add(
          `${components.innerAggregateExpr} AS "${components.innerAggregateValueAlias}"`,
        );
      });

      const innerWhereClause = !filter.equals(Expression.TRUE)
        ? 'WHERE ' + filter.getSQL(dialect)
        : '';

      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-expect-error
      const items = innerSelectExpressions.elements;
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-expect-error
      const groupItems = innerGroupByExpressions.elements;

      const innerAggregateSQL = `
        SELECT ${items.join(',\n')}
        ${this.getFrom()}
        ${innerWhereClause}
        GROUP BY ${groupItems.join(', ')}
      `;

      // Add the CTE definition
      const cteName = `cte_pivot_nested_agg`;
      cteDefinitions.push(`${cteName} AS (${innerAggregateSQL})`);

      // Adjust the FROM clause to use the CTE
      fromClause = `FROM ${cteName} AS t`;

      // Build the external select expressions
      pnaApplies.forEach(({ apply, components }) => {
        selectExpressions.push(
          `${components.outerAggregateExpr.replace(/t\./g, 't.')} AS ${dialect.escapeName(
            apply.name,
          )}`,
        );
      });
    }

    return { selectExpressions, fromClause, cteDefinitions };
  }

  protected abstract getIntrospectAttributes(depth: IntrospectionDepth): Promise<Attributes>;
}
