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

import org.firebrand.dao.cql.clauses.Clause;
import org.firebrand.dao.cql.clauses.ClauseComparator;

import java.util.*;

/**
 * An abstract class for statements providing common functionality such as orderered based comparators
 * used when adding clauses to a statement
 */
public abstract class AbstractStatement<ClauseType extends Clause> implements Statement<ClauseType> {
    /* Fields */

	protected ClauseComparator clauseComparator;

	protected List<ClauseType> clauses = new ArrayList<ClauseType>();

    /* Constructors */

	protected AbstractStatement(ClauseType... clauses) {
		clauseComparator = new ClauseComparator(this);
		add(clauses);
	}

	public void add(ClauseType... clauses) {
		this.clauses.addAll(Arrays.asList(clauses));
		this.clauses.removeAll(Collections.singletonList(null));
		if (getClausesOrder() != null) {
			Collections.sort(this.clauses, clauseComparator);
		}
	}

    /* Canonical Methods */

	@Override
	public String toString() {
		return build();
	}
}
