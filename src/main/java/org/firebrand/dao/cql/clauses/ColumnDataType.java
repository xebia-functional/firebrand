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
 * Types supported in columns
 */
public enum ColumnDataType {
	/**
	 * US-ASCII character string
	 */
	ASCII,
	/**
	 * 64-bit signed long
	 */
	BIGINT,
	/**
	 * Arbitrary hexadecimal bytes (no validation)
	 */
	BLOB,
	/**
	 * true or false
	 */
	BOOLEAN,
	/**
	 * Distributed counter value (64-bit long)
	 */
	COUNTER,
	/**
	 * Variable-precision decimal
	 */
	DECIMAL,
	/**
	 * 64-bit IEEE-754 floating point
	 */
	DOUBLE,
	/**
	 * 32-bit IEEE-754 floating point
	 */
	FLOAT,
	/**
	 * 32-bit signed integer
	 */
	INT,
	/**
	 * UTF-8 encoded string
	 */
	TEXT,
	/**
	 * Date plus time, encoded as 8 bytes since epoch
	 */
	TIMESTAMP,
	/**
	 * Type 1 or type 4 UUID
	 */
	UUID,
	/**
	 * UTF-8 encoded string
	 */
	VARCHAR,
	/**
	 * Arbitrary-precision integer
	 */
	VARINT
}
