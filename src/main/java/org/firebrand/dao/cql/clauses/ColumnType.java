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

/**
 * A column type on an alter column family statement
 */
public class ColumnType implements AlterColumnFamilyClause {
    /* Fields */

	private String column;

	private ColumnDataType columnDataType;

	private boolean typeKeywordRequired;
	
	private boolean primaryKey;

    /* Constructors */

	public ColumnType(String column, ColumnDataType columnDataType, boolean typeKeywordRequired, boolean primaryKey) {
		this.columnDataType = columnDataType;
		this.column = column;
		this.typeKeywordRequired = typeKeywordRequired;
		this.primaryKey = primaryKey;
	}

    /* Canonical Methods */

	@Override
	public String toString() {
		String result;
		String quotedColumn = String.format("'%s'", column);
		if (columnDataType != null) {
			String lowercaseType = columnDataType.name().toLowerCase();
			result = typeKeywordRequired
					? String.format("%s TYPE %s", quotedColumn, lowercaseType)
					: String.format("%s %s", quotedColumn, lowercaseType) ;
		} else {
			result = quotedColumn;
		}
		return primaryKey ? String.format("%s PRIMARY KEY", result) : result;
	}
}
