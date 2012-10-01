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

package org.firebrand.dao.cql.clauses;

import org.firebrand.dao.cql.statement.Statement;

import java.util.Comparator;

/**
 * A comparator that compares clauses based on the statement they're located in
 */
public class ClauseComparator implements Comparator<Clause> {
    /* Fields */

	private Statement statement;

    /* Constructors */

	public ClauseComparator(Statement statement) {
		this.statement = statement;
	}

    /* Interface Implementations */


// --------------------- Interface Comparator ---------------------

	@Override
	public int compare(Clause clause, Clause otherClause) {
		Integer clauseOrder = statement.getClausesOrder().indexOf(clause.getClass());
		Integer otherClauseOrder = statement.getClausesOrder().indexOf(otherClause.getClass());
		return clauseOrder.compareTo(otherClauseOrder);
	}
}
