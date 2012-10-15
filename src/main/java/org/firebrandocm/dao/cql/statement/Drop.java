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
import org.firebrandocm.dao.cql.clauses.ColumnFamily;
import org.firebrandocm.dao.cql.clauses.DropClause;
import org.firebrandocm.dao.cql.clauses.IndexName;
import org.firebrandocm.dao.cql.clauses.KeySpace;

import java.util.*;

/**
 * DROP COLUMNFAMILY
 * Drops the named column family.
 * <p/>
 * Synopsis
 * DROP COLUMNFAMILY <name>;
 * Description
 * A DROP COLUMNFAMILY statement results in the immediate, irreversible removal of a column family, including all data contained in the column family. You can also use the alias DROP TABLE.
 * <p/>
 * Parameters
 * <name>
 * <p/>
 * The name of the column family to be dropped.
 * Example
 * DROP COLUMNFAMILY users;
 * <p/>
 * <p/>
 * <p/>
 * DROP INDEX
 * Drops the named secondary.
 * <p/>
 * Synopsis
 * DROP INDEX <name>;
 * Description
 * A DROP INDEX statement removes an existing secondary index.
 * <p/>
 * Parameters
 * <name>
 * <p/>
 * The name of the secondary index to be dropped. If the index was not given a name during creation, the index name is <columnfamily_name>_<column_name>_idx.
 * Example
 * DROP INDEX user_state;
 * <p/>
 * DROP INDEX users_zip_idx;
 * <p/>
 * <p/>
 * DROP KEYSPACE
 * Drops the named keyspace.
 * <p/>
 * Synopsis
 * DROP KEYSPACE <name>;
 * Description
 * A DROP KEYSPACE statement results in the immediate, irreversible removal of a keyspace, including all column families and data contained in the keyspace.
 * <p/>
 * Parameters
 * <name>
 * <p/>
 * The name of the keyspace to be dropped.
 * Example
 * DROP KEYSPACE Demo;
 *
 * http://www.datastax.com/docs/1.0/references/cql/DROP_COLUMNFAMILY
 * http://www.datastax.com/docs/1.0/references/cql/DROP_INDEX
 * http://www.datastax.com/docs/1.0/references/cql/DROP_KEYSPACE
 *
 *
 */
public class Drop extends AbstractStatement<DropClause> implements Statement<DropClause>, MutationStatement {
    /* Fields */

	private static Map<Class<? extends DropClause>, String> TYPES = Collections.unmodifiableMap(new HashMap<Class<? extends DropClause>, String>() {{
		put(ColumnFamily.class, "COLUMNFAMILY");
		put(IndexName.class, "INDEX");
		put(KeySpace.class, "KEYSPACE");
	}});

	@SuppressWarnings("unchecked")
	private static List<Class<? extends DropClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends DropClause>>asList(
			DropClause.class
	));

    /* Constructors */

	public Drop(DropClause clause) {
		super(clause);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		DropClause clause = clauses.get(0);
		String type = TYPES.get(clause.getClass());
		if (type == null) {
			throw new IllegalArgumentException(String.format("%s not on accepted types list", clause.getClass()));
		}
		return String.format("DROP %s %s;", type, StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends DropClause>> getClausesOrder() {
		return ORDER;
	}
}
