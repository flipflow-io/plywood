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
import { ReadableStream, Transform } from 'readable-stream';

import { AttributeInfo, Attributes, Dataset, Datum, PlywoodValue } from '../datatypes';
import { Set } from '../datatypes/set';
import { SQLDialect } from '../dialect/baseDialect';
import {
  $,
  ApplyExpression,
  ChainableExpression,
  ChainableUnaryExpression,
  CollectExpression,
  CountExpression,
  Expression,
  FilterExpression,
  LimitExpression,
  ModeExpression,
  NumberBucketExpression,
  RefExpression,
  SortExpression,
  SplitExpression,
  SqlAggregateExpression,
  TimeBucketExpression,
} from '../expressions';
import { PlyType } from '../types';

import {
  External,
  ExternalJS,
  ExternalValue,
  Inflater,
  IntrospectionDepth,
  QueryAndPostTransform,
  TotalContainer,
} from './baseExternal';
import { buildResplitAggregationSQL } from './utils/resplitAggregationSQLBuilder';

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

  // -----------------
  // Mode Decomposition: splits mode applies from normal aggregates
  // in split mode, generating separate queries joined in memory.
  // -----------------

  static extractCollectExpression(ex: Expression): CollectExpression | null {
    if (ex instanceof CollectExpression) return ex;
    if (ex instanceof ChainableUnaryExpression && ex.operand instanceof CollectExpression) {
      return ex.operand;
    }
    return null;
  }

  protected buildModeDecomposition(): {
    normalExternal: External;
    modeQueries: { name: string; sql: string; type: PlyType }[];
    postJoinSort: SortExpression | null;
    postJoinLimit: LimitExpression | null;
  } | null {
    if (this.mode !== 'split') return null;

    const modeApplyInfos: { apply: ApplyExpression; modeExpression: ModeExpression }[] = [];
    const collectApplyInfos: { apply: ApplyExpression; collectExpression: CollectExpression }[] =
      [];
    const normalApplies: ApplyExpression[] = [];
    const specialApplyNames: Record<string, true> = {};

    for (const apply of this.applies) {
      const modeEx = SQLExternal.extractModeExpression(apply.expression);
      if (modeEx) {
        modeApplyInfos.push({ apply, modeExpression: modeEx });
        specialApplyNames[apply.name] = true;
      } else {
        const collectEx = SQLExternal.extractCollectExpression(apply.expression);
        if (collectEx) {
          collectApplyInfos.push({ apply, collectExpression: collectEx });
          specialApplyNames[apply.name] = true;
        } else {
          normalApplies.push(apply);
        }
      }
    }

    if (modeApplyInfos.length === 0 && collectApplyInfos.length === 0) return null;

    // If sort references a mode apply, strip it from normalExternal
    // and apply it post-join in memory
    let postJoinSort: SortExpression | null = null;
    let postJoinLimit: LimitExpression | null = null;
    const sortRefName =
      this.sort && this.sort.expression.isOp('ref')
        ? (this.sort.expression as RefExpression).name
        : null;
    const sortRefsMode = sortRefName && specialApplyNames[sortRefName];

    // Build normal external without mode applies
    const normalValue = this.valueOf();
    normalValue.applies = normalApplies;
    if (sortRefsMode) {
      postJoinSort = this.sort;
      postJoinLimit = this.limit;
      delete normalValue.sort;
      delete normalValue.limit;
    }
    const normalExternal = External.fromValue(normalValue);

    // Build mode query SQL for each mode apply
    const { dialect } = this;
    const split = this.getQuerySplit();
    const filter = this.getQueryFilter();
    const from = this.getFrom();

    const splitDimSQLs = split.getSelectSQLNoAliases(dialect);
    const splitDimAliasedSQLs = split.getSelectSQL(dialect);
    const splitKeyNames = split.mapSplits(name => name);
    // Escaped key names for referencing aliased columns in outer query
    const splitKeyEscaped = splitKeyNames.map(name => dialect.escapeName(name));

    const modeQueries = modeApplyInfos.map(({ apply, modeExpression }) => {
      const modeFieldSQL = modeExpression.expression.getSQL(dialect);
      const modeFilter = SQLExternal.extractModeFilter(apply.expression, dialect);

      // Build WHERE parts for the inner subquery
      const whereParts: string[] = [];
      if (!filter.equals(Expression.TRUE)) {
        whereParts.push(filter.getSQL(dialect));
      }
      whereParts.push(`${modeFieldSQL} IS NOT NULL`);
      if (modeFilter) {
        whereParts.push(modeFilter);
      }

      // Inner subquery: alias split dims with key names, GROUP BY raw expressions
      const innerSelect = [
        ...splitDimAliasedSQLs,
        `${modeFieldSQL} AS "__val"`,
        `ROW_NUMBER() OVER (PARTITION BY ${splitDimSQLs.join(
          ',',
        )} ORDER BY COUNT(*) DESC) AS "__rn"`,
      ].join(',\n');

      const innerGroupBy = [...splitDimSQLs, modeFieldSQL].join(',');
      const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';

      // Outer query: reference aliased key names from inner subquery
      const outerSelect = [...splitKeyEscaped, `"__val" AS ${dialect.escapeName(apply.name)}`].join(
        ',\n',
      );

      const sql = [
        'SELECT',
        outerSelect,
        `FROM (SELECT ${innerSelect} ${from} ${whereClause} GROUP BY ${innerGroupBy})`,
        'WHERE "__rn" = 1',
      ].join('\n');

      return {
        name: apply.name,
        sql,
        type: modeExpression.type,
      };
    });

    // Build collect queries — simpler than mode (no ROW_NUMBER, just ARRAY_AGG)
    // When groupByKeys is set, only use those keys for GROUP BY (not all split dims)
    const collectQueries = collectApplyInfos.map(({ apply, collectExpression }) => {
      const collectFieldSQL = collectExpression.expression.getSQL(dialect);
      const { groupByKeys } = collectExpression;

      const whereParts: string[] = [];
      if (!filter.equals(Expression.TRUE)) {
        whereParts.push(filter.getSQL(dialect));
      }
      whereParts.push(`${collectFieldSQL} IS NOT NULL`);
      const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';

      // If groupByKeys specified, use only those split dims
      let collectSplitDimSQLs: string[];
      let collectSplitAliasedSQLs: string[];
      let collectKeyNames: string[];

      if (groupByKeys && groupByKeys.length > 0) {
        const groupBySet: Record<string, true> = {};
        for (const k of groupByKeys) groupBySet[k] = true;

        collectSplitDimSQLs = [];
        collectSplitAliasedSQLs = [];
        collectKeyNames = [];

        split.mapSplits((name, expression) => {
          if (groupBySet[name]) {
            collectSplitDimSQLs.push(expression.getSQL(dialect));
            collectSplitAliasedSQLs.push(
              `${expression.getSQL(dialect)} AS ${dialect.escapeName(name)}`,
            );
            collectKeyNames.push(name);
          }
        });
      } else {
        collectSplitDimSQLs = splitDimSQLs;
        collectSplitAliasedSQLs = splitDimAliasedSQLs;
        collectKeyNames = splitKeyNames;
      }

      const selectParts = [
        ...collectSplitAliasedSQLs,
        `${dialect.collectExpression(collectFieldSQL)} AS ${dialect.escapeName(apply.name)}`,
      ];

      const sql = [
        'SELECT',
        selectParts.join(',\n'),
        from,
        whereClause,
        `GROUP BY ${collectSplitDimSQLs.join(',')}`,
      ].join('\n');

      return {
        name: apply.name,
        sql,
        type: 'STRING' as PlyType,
      };
    });

    const allDecompQueries = [...modeQueries, ...collectQueries];

    return { normalExternal, modeQueries: allDecompQueries, postJoinSort, postJoinLimit };
  }

  public simulateValue(
    lastNode: boolean,
    simulatedQueries: any[],
    externalForNext: External = null,
  ): PlywoodValue | TotalContainer {
    if (!externalForNext) externalForNext = this;

    if (this.mode === 'split') {
      const modeDecomp = this.buildModeDecomposition();
      if (modeDecomp) {
        const { normalExternal, modeQueries } = modeDecomp;

        // Push normal query (without mode applies)
        simulatedQueries.push(normalExternal.getQueryAndPostTransform().query);

        // Push each mode query
        for (const mq of modeQueries) {
          simulatedQueries.push(this.sqlToQuery(mq.sql));
        }

        // Build sample result with all columns (split keys + all applies)
        const datum: Datum = {};
        this.split.mapSplits((name, expression) => {
          datum[name] = this.getSampleValueForType(Set.unwrapSetType(expression.type));
        });
        for (const apply of this.applies) {
          datum[apply.name] = this.getSampleValueForType(apply.expression.type);
        }

        const keys = this.split.mapSplits(name => name);
        if (!lastNode) {
          externalForNext.addNextExternalToDatum(datum);
        }
        return new Dataset({ keys, data: [datum] });
      }
    }
    return super.simulateValue(lastNode, simulatedQueries, externalForNext);
  }

  private getSampleValueForType(type: string): any {
    switch (type) {
      case 'NUMBER':
        return 4;
      case 'STRING':
        return 'some_value';
      case 'BOOLEAN':
        return true;
      default:
        return null;
    }
  }

  protected queryBasicValueStream(rawQueries: any[] | null): ReadableStream {
    if (this.mode === 'split') {
      const modeDecomp = this.buildModeDecomposition();
      if (modeDecomp) {
        const { normalExternal, modeQueries } = modeDecomp;
        const { engine, requester } = this;

        // Execute normal external (standard aggregates)
        // Cast needed: External.fromValue returns External but runtime type preserves subclass
        const normalPromise = External.buildValueFromStream(
          (normalExternal as any).queryBasicValueStream(rawQueries),
        );

        // Execute each mode query via the requester
        const splitKeys = this.split.mapSplits(name => name);
        const splitAttrs = this.split.mapSplits(
          (name, expression) =>
            new AttributeInfo({ name, type: Set.unwrapSetType(expression.type) }),
        );

        const modePromises = modeQueries.map(mq => {
          const query = this.sqlToQuery(mq.sql);
          const modeAttr = new AttributeInfo({ name: mq.name, type: mq.type });
          const attrs = [...splitAttrs, modeAttr];
          const postTransform = External.postTransformFactory([], attrs, splitKeys, null);

          const stream = External.performQueryAndPostTransform(
            { query, postTransform },
            requester,
            engine,
            rawQueries,
          );
          return External.buildValueFromStream(stream);
        });

        const { postJoinSort, postJoinLimit } = modeDecomp;

        // Join all results in memory, then apply sort/limit if needed
        return External.valuePromiseToStream(
          Promise.all([normalPromise, ...modePromises]).then(([normalPV, ...modePVs]) => {
            let joined = normalPV as Dataset;
            for (const modePV of modePVs) {
              const other = modePV as Dataset;
              if (other.keys && joined.keys && other.keys.length < joined.keys.length) {
                joined = joined.broadcastJoin(other);
              } else {
                joined = joined.leftJoin(other);
              }
            }
            if (postJoinSort) {
              joined = joined.sort(postJoinSort.expression, postJoinSort.direction);
              if (postJoinLimit) {
                joined = joined.limit(postJoinLimit.value);
              }
            }
            return joined;
          }),
        );
      }
    }
    return super.queryBasicValueStream(rawQueries);
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
          'SELECT',
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

      case 'value': {
        const valueApply = this.toValueApply();
        const valueModeEx = SQLExternal.extractModeExpression(valueApply.expression);
        if (valueModeEx) {
          const modeFieldSQL = valueModeEx.expression.getSQL(dialect);
          const modeFilter = SQLExternal.extractModeFilter(valueApply.expression, dialect);
          const subWhereParts: string[] = [];
          if (!filter.equals(Expression.TRUE)) {
            subWhereParts.push(filter.getSQL(dialect));
          }
          subWhereParts.push(`${modeFieldSQL} IS NOT NULL`);
          if (modeFilter) {
            subWhereParts.push(modeFilter);
          }
          queryParts.push(
            'SELECT',
            `(SELECT ${modeFieldSQL} ${this.getFrom()} WHERE ${subWhereParts.join(
              ' AND ',
            )} GROUP BY ${modeFieldSQL} ORDER BY COUNT(*) DESC LIMIT 1) AS ${dialect.escapeName(
              valueApply.name,
            )}`,
            fromClause,
            dialect.emptyGroupBy(),
          );
        } else {
          queryParts.push('SELECT', valueApply.getSQL(dialect), fromClause, dialect.emptyGroupBy());
        }
        postTransform = External.valuePostTransformFactory();
        break;
      }

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
          'SELECT',
          applies
            .map(apply => {
              const modeEx = SQLExternal.extractModeExpression(apply.expression);
              if (modeEx) {
                // Generate scalar subquery for total mode
                const modeFieldSQL = modeEx.expression.getSQL(dialect);
                const modeFilter = SQLExternal.extractModeFilter(apply.expression, dialect);
                const subWhereParts: string[] = [];
                if (!filter.equals(Expression.TRUE)) {
                  subWhereParts.push(filter.getSQL(dialect));
                }
                subWhereParts.push(`${modeFieldSQL} IS NOT NULL`);
                if (modeFilter) {
                  subWhereParts.push(modeFilter);
                }
                return `(SELECT ${modeFieldSQL} ${this.getFrom()} WHERE ${subWhereParts.join(
                  ' AND ',
                )} GROUP BY ${modeFieldSQL} ORDER BY COUNT(*) DESC LIMIT 1) AS ${dialect.escapeName(
                  apply.name,
                )}`;
              }
              return apply.getSQL(dialect);
            })
            .join(',\n'),
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

        const _otherApplies: ApplyExpression[] = [];

        // Collect mode applies for CTE generation
        const modeApplies: { apply: ApplyExpression; modeExpression: ModeExpression }[] = [];

        applies.forEach(apply => {
          // Detect ModeExpression (possibly wrapped in FilterExpression)
          const modeEx = SQLExternal.extractModeExpression(apply.expression);
          if (modeEx) {
            modeApplies.push({ apply, modeExpression: modeEx });
            return;
          }

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
          let innerSelectExpressions = new Set({ setType: 'STRING', elements: [] });
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

          const items = innerSelectExpressions.elements;
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

        // Generate CTEs for MODE expressions
        const modeJoinClauses: string[] = [];
        modeApplies.forEach(({ apply, modeExpression }, idx) => {
          const cteName = `__mode_${idx}`;
          const cteAlias = `__m${idx}`;

          // Get the field being mode'd
          const modeFieldSQL = modeExpression.expression.getSQL(dialect);

          // Get split dimension expressions for PARTITION BY and JOIN
          const splitDimSQLs = split.getSelectSQLNoAliases(dialect);
          const _splitDimAliasedSQLs = split.getSelectSQL(dialect);

          // Extract filter from the mode's operand chain (if mode is filtered)
          const modeFilter = SQLExternal.extractModeFilter(apply.expression, dialect);

          // Build the CTE: GROUP BY split dims + mode field, then ROW_NUMBER
          const cteSelectParts = [
            ...splitDimSQLs,
            `${modeFieldSQL} AS __val`,
            `ROW_NUMBER() OVER (PARTITION BY ${splitDimSQLs.join(
              ',',
            )} ORDER BY COUNT(*) DESC) AS __rn`,
          ];

          const cteWhereparts: string[] = [];
          if (!filter.equals(Expression.TRUE)) {
            cteWhereparts.push(filter.getSQL(dialect));
          }
          cteWhereparts.push(`${modeFieldSQL} IS NOT NULL`);
          if (modeFilter) {
            cteWhereparts.push(modeFilter);
          }

          const cteGroupByParts = [...splitDimSQLs, modeFieldSQL];

          const cteSQL = [
            'SELECT',
            cteSelectParts.join(','),
            this.getFrom(),
            `WHERE ${cteWhereparts.join(' AND ')}`,
            `GROUP BY ${cteGroupByParts.join(',')}`,
          ].join('\n');

          cteDefinitions.push(`${cteName} AS (${cteSQL})`);

          // Build JOIN clause
          const joinConditions = split
            .mapSplits((_name, expression) => {
              const dimSQL = expression.getSQL(dialect);
              return `(t.${dimSQL} IS NOT DISTINCT FROM ${cteAlias}.${dimSQL})`;
            })
            .concat([`${cteAlias}.__rn = 1`]);

          modeJoinClauses.push(
            `LEFT JOIN ${cteName} ${cteAlias} ON (${joinConditions.join(' AND ')})`,
          );

          // Add mode column to SELECT
          selectExpressions.push(`${cteAlias}.__val AS ${dialect.escapeName(apply.name)}`);

          // Add mode column to GROUP BY to satisfy SQL semantics
          groupByExpressions.push(`${cteAlias}.__val`);
        });

        // Insert JOIN clauses between FROM and WHERE
        if (modeJoinClauses.length > 0) {
          // fromClause may contain "FROM table AS t\nWHERE ..."
          // We need to insert JOINs between FROM and WHERE
          const whereIdx = fromClause.indexOf('\nWHERE ');
          if (whereIdx !== -1) {
            const fromPart = fromClause.substring(0, whereIdx);
            const wherePart = fromClause.substring(whereIdx);
            fromClause = fromPart + '\n' + modeJoinClauses.join('\n') + wherePart;
          } else {
            fromClause += '\n' + modeJoinClauses.join('\n');
          }
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
          groupByExpressions.length > 0 ? 'GROUP BY ' + groupByExpressions.join(',') : '',
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

  // -----------------
  // Mode Expression Support

  /**
   * Extracts a ModeExpression from an apply's expression chain.
   * Mode may be direct: $data.mode($field)
   * Or filtered: $data.filter(...).mode($field)
   */
  static extractModeExpression(ex: Expression): ModeExpression | null {
    if (ex instanceof ModeExpression) return ex;
    // Check if it's a mode wrapped in a filter chain
    if (ex instanceof ChainableUnaryExpression && ex.operand instanceof ModeExpression) {
      return ex.operand;
    }
    return null;
  }

  /**
   * Extracts the filter SQL from a filtered mode expression.
   * E.g., $data.filter($color != "null").mode($color) → the filter condition SQL
   */
  static extractModeFilter(ex: Expression, _dialect: SQLDialect): string | null {
    if (ex instanceof ModeExpression) {
      // Check if the operand of the mode is a FilterExpression
      if (ex.operand instanceof FilterExpression) {
        const filterEx = ex.operand;
        return filterEx.expression.getSQL(_dialect);
      }
    }
    return null;
  }

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
    if (!applies && !this.valueExpression) return null;
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
    const isCompleteAggregateExpression =
      /^(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|GROUP_CONCAT|LISTAGG|ARRAY_AGG|STRING_AGG|BIT_AND|BIT_OR|BIT_XOR|BOOL_AND|BOOL_OR|CORR|COVAR_POP|COVAR_SAMP|EVERY|REGR_|PERCENTILE_|MEDIAN|ANY_VALUE)\s*\(/i.test(
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
          `\\b(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|GROUP_CONCAT|LISTAGG|ARRAY_AGG|STRING_AGG|BIT_AND|BIT_OR|BIT_XOR|BOOL_AND|BOOL_OR|CORR|COVAR_POP|COVAR_SAMP|EVERY|REGR_|PERCENTILE_|MEDIAN|ANY_VALUE)\\s*\\([^)]*"${columnName.replace(
            /"/g,
            '""',
          )}"[^)]*\\)`,
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
