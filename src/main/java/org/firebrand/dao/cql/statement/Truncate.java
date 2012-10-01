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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * UPDATE »
 * TRUNCATE
 * Removes all data from a column family.
 * <p/>
 * Synopsis
 * TRUNCATE <column_family>;
 * Description
 * A TRUNCATE statement results in the immediate, irreversible removal of all data in the named column family.
 * <p/>
 * Parameters
 * <column_family>
 * <p/>
 * The name of the column family to truncate.
 * Example
 * TRUNCATE user_activity;
 */
public class Truncate extends AbstractStatement<TruncateClause> implements Statement<TruncateClause>, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends TruncateClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends TruncateClause>>asList(
			ColumnFamily.class
	));

    /* Constructors */

	public Truncate(TruncateClause clause) {
		super(clause);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("TRUNCATE %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends TruncateClause>> getClausesOrder() {
		return ORDER;
	}
}
