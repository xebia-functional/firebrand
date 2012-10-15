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
 * An index name on a create index and drop index statements
 */
public class IndexName implements CreateIndexClause, DropClause {
    /* Fields */

	private String indexName;

    /* Constructors */

	public IndexName(String indexName) {
		this.indexName = indexName;
	}

    /* Canonical Methods */

	@Override
	public String toString() {
		return indexName;
	}
}
