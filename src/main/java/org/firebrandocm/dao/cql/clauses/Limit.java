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

/**
 * A limit clause on a select statement
 */
public class Limit implements SelectClause {
    /* Fields */

	private int limit;

    /* Constructors */

	public Limit(int limit) {
		this.limit = limit;
	}

    /* Canonical Methods */

	@Override
	public String toString() {
		return String.format("LIMIT %d", limit);
	}
}
