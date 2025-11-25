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

import { Expression } from '../../expressions';
import { SQLDialect } from '../../dialect/baseDialect';

export interface QualifiedExpressions {
  selectExpressions: string[];
  groupByExpressions: string[];
}

/**
 * Qualifies column references in SELECT and GROUP BY when there's a JOIN
 * to avoid ambiguity (e.g., "s0" -> "t.s0").
 *
 * When there's a JOIN between cte_subsplit and cte_split_lookup, both CTEs
 * may have columns with the same name (e.g., "s0"). To avoid ambiguity,
 * we need to qualify all column references with the table alias.
 *
 * @param outerSplits - Map of outer split names to expressions
 * @param outerSplitNameToIntermediateName - Mapping from outer names to intermediate names
 * @param needsQualification - Whether qualification is needed (has JOIN)
 * @param cteAlias - Alias for the main CTE (usually "t")
 * @param dialect - SQL dialect for escaping names
 * @returns Qualified SELECT and GROUP BY expressions
 */
export function qualifyColumns(
  outerSplits: Record<string, Expression>,
  outerSplitNameToIntermediateName: Record<string, string>,
  needsQualification: boolean,
  cteAlias: string,
  dialect: SQLDialect,
): QualifiedExpressions {
  const selectExpressions: string[] = [];
  const groupByExpressions: string[] = [];

  for (const name in outerSplits) {
    if (!outerSplits.hasOwnProperty(name)) continue;

    const expression = outerSplits[name];
    let sql = expression.getSQL(dialect);
    const alias = dialect.escapeName(name);
    const intermediateName = outerSplitNameToIntermediateName[name];

    // Qualify column references if there's a JOIN
    if (needsQualification && intermediateName) {
      // Use qualified column reference: t."s0" instead of just "s0"
      sql = `${cteAlias}.${dialect.escapeName(intermediateName)}`;
    }

    selectExpressions.push(`${sql} AS ${alias}`);

    // Also qualify GROUP BY expressions
    let groupBySql = sql;
    if (needsQualification && intermediateName) {
      groupBySql = `${cteAlias}.${dialect.escapeName(intermediateName)}`;
    }
    groupByExpressions.push(groupBySql);
  }

  return {
    selectExpressions,
    groupByExpressions,
  };
}

