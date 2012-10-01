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
 * A column = value assignment
 */
public class Assignment {
    /* Fields */

	private String column;

	private Object value;

	private boolean quoted = true;

    /* Constructors */

	public Assignment(String column, Object value) {
		this.column = column;
		this.value = value;
	}

	public Assignment(boolean quoted, String column, Object value) {
		this(column, value);
		this.quoted = quoted;
	}

    /* Getters & Setters */

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

    /* Canonical Methods */

	@Override
	public String toString() {
		return quoted ?
				String.format(String.format("'%s' = '%s'", column, value))
				:String.format(String.format("%s = %s", column, value));
	}
}
