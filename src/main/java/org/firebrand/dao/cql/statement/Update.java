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

package org.firebrand.dao.cql.statement;

import org.apache.commons.lang3.StringUtils;
import org.firebrand.dao.cql.clauses.*;
import org.firebrand.dao.cql.clauses.Set;

import java.util.*;

/**
 * UPDATE
 * Updates one or more columns in the identified row of a column family.
 * <p/>
 * Synopsis
 * UPDATE <column_family>
 * [ USING <write_option> [ AND <write_option> [...] ] ];
 * SET <column_name> = <column_value> [, ...]
 * | <counter_column_name> = <counter_column_name> {+ | -} <integer>
 * WHERE <row_specification>;
 * where <write_option> is:
 * <p/>
 * USING CONSISTENCY <consistency_level>
 * TTL <seconds>
 * TIMESTAMP <integer>
 * and where <row_specification> is:
 * <p/>
 * KEY | <key_alias> = <key_value>
 * KEY | <key_alias> IN (<key_value> [,...])
 * Description
 * An UPDATE is used to update or write one or more columns to the identified row in a Cassandra column family.
 * Row/column records are created if they do not exist, or updated if they do exist.
 * <p/>
 * Rows are created or updated by supplying column names and values, after the SET keyword.
 * Multiple columns can be set by separating the name/value pairs using commas.
 * Each update statement requires a precise set of row keys to be specified using a WHERE clause.
 * Rows are identified using the KEY keyword or the key alias defined on the column family.
 * <p/>
 * Parameters
 * <column_family>
 * The name of the column family from which to update (or insert) the identified columns and rows.
 * <p/>
 * USING CONSISTENCY <consistency_level>
 * <p/>
 * Optional clause to specify the consistency level. If omitted, the default consistency level is ONE.
 * The following consistency levels can be specified. See tunable consistency for more information about
 * the different consistency levels.
 * <p/>
 * ANY (applicable to writes only)
 * ONE
 * QUORUM
 * LOCAL_QUOROM (applicable to multi-data center clusters only)
 * EACH_QUOROM (applicable to multi-data center clusters only)
 * TTL <seconds>
 * <p/>
 * Optional clause to specify a time-to-live (TTL) period for an inserted or updated column.
 * TTL columns are automatically marked as deleted (with a tombstone) after the requested amount of time has expired.
 * TIMESTAMP <integer>
 * <p/>
 * Defines an optional timestamp to use for the written columns. The timestamp must be in the form of an integer.
 * SET
 * <p/>
 * The SET clause is used to specify a comma-separated list of column name/value pairs that you want to update or insert.
 * If the named column exists, its value will be updated. If it does not exist it will be inserted.
 * For counter column families, you can update a counter column value by specifying an increment or decrement value
 * to be applied to the current value of the counter column.
 * WHERE <row_specification>
 * <p/>
 * The WHERE clause identifies one or more rows in the column family for which to update the named columns.
 * Rows are identified using the KEY keyword or the key alias defined on the column family, and then supplying one or
 * more row key values.
 *
 * Example
 * Update a column in several rows at once:
 * <p/>
 * UPDATE users USING CONSISTENCY QUORUM
 * SET 'state' = 'TX'
 * WHERE KEY IN (88b8fd18-b1ed-4e96-bf79-4280797cba80,
 * 06a8913c-c0d6-477c-937d-6c1b69a95d43,
 * bc108776-7cb5-477f-917d-869c12dfffa8);
 * Update several columns in a single row:
 * <p/>
 * UPDATE users USING CONSISTENCY QUORUM
 * SET 'name' = 'John Smith', 'email' = 'jsmith@cassie.com'
 * WHERE user_uuid = 88b8fd18-b1ed-4e96-bf79-4280797cba80;
 * Update the value of a counter column:
 * <p/>
 * UPDATE page_views USING CONSISTENCY QUORUM AND TIMESTAMP=1318452291034
 * SET 'index.html' = 'index.html' + 1
 * WHERE KEY = 'www.datastax.com';
 *
 * http://www.datastax.com/docs/1.0/references/cql/UPDATE
 */
public class Update extends AbstractStatement<UpdateClause> implements Statement<UpdateClause>, BatchCapable, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends UpdateClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends UpdateClause>>asList(
			ColumnFamily.class, WriteOptionGroup.class, Set.class, Where.class
	));

    /* Constructors */

	public Update(UpdateClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("UPDATE %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends UpdateClause>> getClausesOrder() {
		return ORDER;
	}
}
