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

import java.util.*;

/**
 * DELETE
 * Removes one or more columns from the named row(s).
 * <p/>
 * Synopsis
 * DELETE [<column_name> [, ...]]
 * FROM <column_family>
 * [USING CONSISTENCY <consistency_level> [AND TIMESTAMP <integer>]]
 * WHERE <row_specification>;
 * where <row_specification> is:
 * <p/>
 * KEY | <key_alias> = <key_value>
 * KEY | <key_alias> IN (<key_value> [,...])
 * Description
 * A DELETE statement removes one or more columns from one or more rows in the named column family. Rows are identified using the KEY keyword or the key alias defined on the column family. If no column names are given, the entire row is deleted.
 * <p/>
 * When a column is deleted, it is not removed from disk immediately. The deleted column is marked with a tombstone and then removed after the configured grace period has expired. See About Deletes for more information about how Cassandra handles deleted columns and rows.
 * <p/>
 * Parameters
 * <column_name>
 * <p/>
 * The name of one or more columns to be deleted. If no column names are given, then all columns in the identified rows will be deleted, essentially deleting the entire row.
 * FROM <column_family>
 * <p/>
 * The name of the column family from which to delete the identified columns or rows.
 * USING CONSISTENCY <consistency_level>
 * <p/>
 * Optional clause to specify the consistency level. If omitted, the default consistency level is ONE. The following consistency levels can be specified. See tunable consistency for more information about the different consistency levels.
 * <p/>
 * ANY (applicable to writes only)
 * ONE
 * QUORUM
 * LOCAL_QUOROM (applicable to multi-data center clusters only)
 * EACH_QUOROM (applicable to multi-data center clusters only)
 * TIMESTAMP <integer>
 * <p/>
 * Defines an optional timestamp to use for the new tombstone record. The timestamp must be in the form of an integer.
 * WHERE <row_specification>
 * <p/>
 * The WHERE clause identifies one or more rows in the column family from which to delete columns. Rows are identified using the KEY keyword or the key alias defined on the column family, and then supplying one or more row key values.
 * Example
 * DELETE email, phone
 * FROM users
 * USING CONSISTENCY QUORUM AND TIMESTAMP 1318452291034
 * WHERE user_name = 'jsmith';
 * <p/>
 * DELETE FROM users WHERE KEY IN ('dhutchinson', 'jsmith');
 */
public class Delete extends AbstractStatement<DeleteClause> implements Statement<DeleteClause>, BatchCapable, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends DeleteClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends DeleteClause>>asList(
			Columns.class, From.class, WriteOptionGroup.class, Where.class
	));

    /* Constructors */

	public Delete(DeleteClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("DELETE %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends DeleteClause>> getClausesOrder() {
		return ORDER;
	}
}
