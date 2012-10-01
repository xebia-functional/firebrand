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
import org.firebrand.dao.cql.clauses.BatchCapable;

import java.util.List;

/**
 * BATCH
 *
 * Sets a global consistency level, client-supplied timestamp, and optional time-to-live (TTL) for all columns
 * written by the statements in the batch.
 *
 * Synopsis
 * BEGIN BATCH
 * <p/>
 * [ USING <write_option> [ AND <write_option> [...] ] ];
 * <p/>
 * <dml_statement>
 * <dml_statement>
 * [...]
 * <p/>
 * APPLY BATCH;
 * where <write_option> is:
 * <p/>
 * USING CONSISTENCY <consistency_level>
 * TTL <seconds>
 * TIMESTAMP <integer>
 * Description
 * A BATCH statement allows you combine multiple data modification statements into a single logical operation.
 * All columns modified by the batch statement will have the same global timestamp.
 * Only INSERT, UPDATE, and DELETE statements are allowed within a BATCH statement.
 * Individual statements within a BATCH should be specified one statement per line without an ending semi-colon.
 * <p/>
 * All statements within the batch are executed using the same consistency level.
 * <p/>
 * BATCH should not be considered as an analogue for SQL ACID transactions.
 * BATCH does not provide transaction isolation. Column updates are only considered atomic within a given record (row).
 * <p/>
 * Parameters
 * BEGIN BATCH
 * <p/>
 * Command to initiate a batch. All statements in the batch will be executed with the same timestamp and consistency
 * level (and optional time-to-live).
 * USING CONSISTENCY <consistency_level>
 * <p/>
 * Optional clause to specify the consistency level. If omitted, the default consistency level is ONE.
 * The following consistency levels can be specified.
 * See tunable consistency for more information about the different consistency levels.
 * <p/>
 * ANY (applicable to writes only)
 * ONE
 * QUORUM
 * LOCAL_QUOROM (applicable to multi-data center clusters only)
 * EACH_QUOROM (applicable to multi-data center clusters only)
 * TTL <seconds>
 * <p/>
 * Optional clause to specify a time-to-live (TTL) period for an inserted or updated column.
 * TTL columns are automatically marked as deleted (with a tombstone) after the requested amount of time has expired.
 * TIMESTAMP <integer>
 * <p/>
 * Defines an optional timestamp to use for the written columns. The timestamp must be in the form of an integer.
 * <dml_statement>
 * <p/>
 * An INSERT, UPDATE, or DELETE statement to be executed.
 * A statement should be contained on a single line and without an ending semi-colon.
 * APPLY BATCH
 * <p/>
 * Closes the batch statement. All statements in the batch are submitted for execution as a unit.
 *
 * Example
 *
 * BEGIN BATCH USING CONSISTENCY QUORUM AND TTL 8640000
 * INSERT INTO users (KEY, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')
 * UPDATE users SET password = 'ps22dhds' WHERE KEY = 'user2'
 * INSERT INTO users (KEY, password) VALUES ('user3', 'ch@ngem3c')
 * DELETE name FROM users WHERE key = 'user2'
 * INSERT INTO users (KEY, password, name) VALUES ('user4', 'ch@ngem3c', 'Andrew')
 * APPLY BATCH;
 *
 * http://www.datastax.com/docs/1.0/references/cql/BATCH
 */
public class Batch extends AbstractStatement<BatchCapable> implements Statement<BatchCapable>, MutationStatement {
    /* Constructors */

	public Batch(BatchCapable... batchCapables) {
		super(batchCapables);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("BEGIN BATCH\n%s\nAPPLY BATCH;", StringUtils.join(clauses, '\n'));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends BatchCapable>> getClausesOrder() {
		return null;
	}
}

