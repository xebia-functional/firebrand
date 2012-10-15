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
 * CREATE INDEX
 * Define a new secondary index on a single, typed column of a column family.
 * <p/>
 * Synopsis
 * CREATE INDEX [<index_name>]
 * ON <cf_name> (<column_name>);
 * Description
 * CREATE INDEX creates a secondary index on the named column family, for the named column. A secondary index can only be created on a single, typed column. The indexed column must have a data type defined in the column metadata of the column family definition, although it is not required that the column exist in any currently stored rows.
 * <p/>
 * Parameters
 * <index_name>
 * <p/>
 * Optionally defines a name for the secondary index. Valid index names are strings of alpha-numeric characters and underscores, and must begin with a letter.
 * ON <cf_name> (<column_name>)
 * <p/>
 * Specifies the column family and column name on which to create the secondary index. The named column must have a data type defined in the column family schema definition.
 * Examples
 * Define a static column family and then create a secondary index on two of its named columns:
 * <p/>
 * CREATE COLUMNFAMILY users (
 * KEY uuid PRIMARY KEY,
 * firstname text,
 * lastname text,
 * email text,
 * address text,
 * zip int,
 * state text);
 * <p/>
 * CREATE INDEX user_state
 * ON users (state);
 * <p/>
 * CREATE INDEX ON users (zip);
 *
 * http://www.datastax.com/docs/1.0/references/cql/CREATE_INDEX
 */
public class CreateIndex extends AbstractStatement<CreateIndexClause> implements Statement<CreateIndexClause>, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends CreateIndexClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends CreateIndexClause>>asList(
			IndexName.class, OnColumnFamily.class, ColumnParenthesis.class
	));

    /* Constructors */

	public CreateIndex(CreateIndexClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("CREATE INDEX %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends CreateIndexClause>> getClausesOrder() {
		return ORDER;
	}
}
