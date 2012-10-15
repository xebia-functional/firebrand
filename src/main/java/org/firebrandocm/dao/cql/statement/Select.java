/*
 * Copyright (C) 2012 47 Degrees, LLC
 * http://47deg.com
 * hello@47deg.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.firebrandocm.dao.cql.statement;

import org.apache.commons.lang3.StringUtils;
import org.firebrandocm.dao.cql.clauses.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * SELECT
 * Returns the requested rows and columns from a Cassandra column family.
 * <p/>
 * Synopsis
 * SELECT <column_specification>
 * FROM [<keyspace>.]<column_family>
 * [USING CONSISTENCY <consistency_level>]
 * [WHERE <row_specification> [AND <row_specification> [...]]
 * [LIMIT <integer>]
 * where <column_specification> is:
 * <p/>
 * <column_name> [, ...]
 * | [FIRST <integer>] [REVERSED] { <start_of_range> .. <end_of_range> | * }
 * | COUNT(*)
 * and where <row_specification> is:
 * <p/>
 * KEY | <key_alias> { = | < | > | <= | >= } <key_value>
 * KEY | <key_alias> IN (<key_value> [,...])
 * Description
 * A SELECT is used to read one or more rows from a Cassandra column family. It returns a result-set of rows, where each row consists of a row key and a collection of columns corresponding to the query.
 * <p/>
 * Unlike a SQL SELECT, there is no guarantee that the columns specified in the query will be contained in the result set. Cassandra has a schema-optional data model, so it will not give an error if you request columns that do not exist.
 * <p/>
 * Parameters
 * The SELECT List
 * <p/>
 * The SELECT list determines the columns that will appear in the results. It takes a comma-separated list of column names, a range of column names, or COUNT(*). Column names in Cassandra can be specified as string literals or integers, in addition to named identifiers.
 * <p/>
 * To specify a range of columns, specify the start and end column names separated by two periods (..). The set of columns returned for a range is start and end inclusive. The asterisk (*) may also be used as a range representing all columns.
 * <p/>
 * When requesting a range of columns, it may be useful to limit the number of columns that can be returned from each row using the FIRST clause. This sets an upper limit on the number of columns returned per row (the default is 10,000 if not specified).
 * <p/>
 * The REVERSED keyword causes the columns to be returned in reversed sorted order. If using a FIRST clause, the columns at the end of the range will be selected instead of the ones at the beginning of the range.
 * <p/>
 * A SELECT list may also be COUNT(*). In this case, the result will be the number of rows which matched the query.
 * <p/>
 * The FROM Clause
 * <p/>
 * The FROM clause specifies the column family to query. If a keyspace is not specified, the current keyspace will be used.
 * USING CONSISTENCY <consistency_level>
 * <p/>
 * Optional clause to specify the consistency level. If omitted, the default consistency level is ONE. The following consistency levels can be specified. See tunable consistency for more information about the different consistency levels.
 * <p/>
 * ONE
 * QUORUM
 * LOCAL_QUOROM (applicable to multi-data center clusters only)
 * EACH_QUOROM (applicable to multi-data center clusters only)
 * The WHERE Clause
 * <p/>
 * The WHERE clause filters the rows that appear in the results. You can filter on a key name, a range of keys, or on column values (in the case of columns that have a secondary index). Row keys are specified using the KEY keyword or key alias defined on the column family, followed by a relational operator (one of =, >, >=, <, or <=), and then a value. When terms appear on both sides of a relational operator it is assumed the filter applies to an indexed column. With column index filters, the term on the left of the operator must be the name of the indexed column, and the term on the right is the value to filter on.
 * <p/>
 * Note: The greater-than and less-than operators (> and <) result in key ranges that are inclusive of the terms. There is no supported notion of strictly greater-than or less-than; these operators are merely supported as aliases to >= and <=.
 * <p/>
 * The LIMIT Clause
 * <p/>
 * The LIMIT clause limits the number of rows returned by the query. The default is 10,000 rows.
 * Examples
 * Select two columns from three rows:
 * <p/>
 * SELECT name, title FROM employees WHERE KEY IN (199, 200, 207);
 * Select a range of columns from all rows, but limit the number of columns to 3 per row starting with the end of the range:
 * <p/>
 * SELECT FIRST 3 REVERSED 'time199'..'time100' FROM events;
 * Count the number of rows in a column family:
 * <p/>
 * SELECT COUNT(*) FROM users;
 *
 * http://www.datastax.com/docs/1.0/references/cql/SELECT
 */
public class Select extends AbstractStatement<SelectClause> implements Statement<SelectClause>, SelectionStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends SelectClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends SelectClause>>asList(
			First.class, Reversed.class, Columns.class, ColumnRange.class,
			From.class, Where.class,
			Limit.class, Consistency.class
	));

    /* Constructors */

	public Select(SelectClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("SELECT %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends SelectClause>> getClausesOrder() {
		return ORDER;
	}
}
