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

import org.apache.commons.lang3.StringUtils;

/**
 * A comma separated list of columns
 */
public class Columns implements SelectClause, DeleteClause {
    /* Fields */

	private String[] columns;

	private boolean quoted = true;

    /* Constructors */

	public Columns(String... columns) {
		this.columns = columns;
	}

	public Columns(boolean quoted, String... columns) {
		this.columns = columns;
		this.quoted = quoted;
	}

    /* Canonical Methods */

	@Override
	public String toString() {
		return quoted
				? String.format("'%s'", StringUtils.join(columns, "', '"))
				: String.format("%s", StringUtils.join(columns, ", "));
	}
}
