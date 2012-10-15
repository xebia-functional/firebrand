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

package org.firebrandocm.dao.cql.clauses;

import org.apache.cassandra.thrift.IndexOperator;

/**
* An index operator based predicate: eq, lt, lte, gt, gte
*/
public class IndexOperatorPredicate implements Predicate {
    /* Fields */

	private String column;

	private IndexOperator operator;

	private Object value;

    /* Constructors */

	public IndexOperatorPredicate(IndexOperator operator, String value) {
		this.operator = operator;
		this.value = value;
	}

	public IndexOperatorPredicate(String column, IndexOperator operator, String value) {
		this.column = column;
		this.operator = operator;
		this.value = value;
	}

    /* Canonical Methods */

	@Override
	public String toString() {
		String operatorToken = null;
		switch (operator) {
			case EQ: operatorToken = "="; break;
			case LT: operatorToken = "<"; break;
			case LTE: operatorToken = "<="; break;
			case GT: operatorToken = ">"; break;
			case GTE: operatorToken = ">="; break;
		}
		return String.format(String.format("%s %s '%s'", column != null ? String.format("'%s'", column) : "KEY", operatorToken, value));
	}
}
