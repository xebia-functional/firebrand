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
 * INSERT
 * Inserts or updates one or more columns in the identified row of a column family.
 * <p/>
 * Synopsis
 * INSERT INTO <column_family> (<key_name>, <column_name> [, ...])
 * VALUES (<key_value>, <column_value> [, ...])
 * [USING <write_option> [AND <write_option> [...] ] ];
 * where <write_option> is:
 * <p/>
 * USING CONSISTENCY <consistency_level>
 * TTL <seconds>
 * TIMESTAMP <integer>
 * Description
 * An INSERT is used to write one or more columns to the identified row in a Cassandra column family.
 * No results are returned. Unlike in SQL, the semantics of INSERT and UPDATE are identical.
 * In either case a row/column record is created if does not exist, or updated if it does exist.
 * <p/>
 * The first column name in the INSERT list must be that of the row key (either the KEY keyword or the row key alias
 * defined on the column family). The first column value in the VALUES list is the row key value for which you want to
 * insert or update columns. After the row key, there must be at least one other column name specified. In Cassandra,
 * a row with only a key and no associated columns is not considered to exist.
 * <p/>
 * Parameters
 * <column_family> (<key_name>, <column_name> [, ...])
 * <p/>
 * The name of the column family to insert or update followed by a comma-separated list of column names enclosed in
 * parenthesis. The first name in the list must be that of the row key (either the KEY keyword or the row key alias
 * defined on the column family) followed by at least one other column name.
 * VALUES (<key_value>, <column_value> [, ...])
 * <p/>
 * Supplies a comma-separated list of column values enclosed in parenthesis. The first column value is always the row
 * key value for which you want to insert columns. Column values should be listed in the same order as the column names
 * supplied in the INSERT list. If a row or column does not exist, it will be inserted. If it does exist,
 * it will be updated.
 * USING CONSISTENCY <consistency_level>
 * <p/>
 * Optional clause to specify the consistency level. If omitted, the default consistency level is ONE. The following
 * consistency levels can be specified. See tunable consistency for more information about the different
 * consistency levels.
 * <p/>
 * ANY (applicable to writes only)
 * ONE
 * QUORUM
 * LOCAL_QUOROM (applicable to multi-data center clusters only)
 * EACH_QUOROM (applicable to multi-data center clusters only)
 * TTL <seconds>
 * <p/>
 * Optional clause to specify a time-to-live (TTL) period for an inserted or updated column. TTL columns are
 * automatically marked as deleted (with a tombstone) after the requested amount of time has expired.
 * TIMESTAMP <integer>
 * <p/>
 * Defines an optional timestamp to use for the written columns. The timestamp must be in the form of an integer.
 *
 * Example
 *
 * INSERT INTO users (KEY, user_name)
 * VALUES ('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6', 'jbellis')
 * USING CONSISTENCY LOCAL_QUORUM AND TTL 86400;
 *
 * http://www.datastax.com/docs/1.0/references/cql/INSERT
 */
public class Insert extends AbstractStatement<InsertClause> implements Statement<InsertClause>, BatchCapable, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends InsertClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends InsertClause>>asList(
			ColumnFamily.class, ColumnParenthesis.class, ValuesParenthesis.class, WriteOptionGroup.class
	));

    /* Constructors */

	public Insert(InsertClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("INSERT INTO %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends InsertClause>> getClausesOrder() {
		return ORDER;
	}
}
