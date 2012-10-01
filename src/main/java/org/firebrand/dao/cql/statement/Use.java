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
import org.firebrand.dao.cql.clauses.KeySpace;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * ASSUME »
 * USE
 * Connects the current client session to a keyspace.
 * <p/>
 * Synopsis
 * USE <keyspace_name>;
 * Description
 * A USE statement tells the current client session and the connected Cassandra instance what keyspace you will be working in. All subsequent operations on column families and indexes will be in the context of the named keyspace, unless otherwise specified or until the client connection is terminated or another USE statement is issued.
 * <p/>
 * Parameters
 * <keyspace_name>
 * <p/>
 * The name of the keyspace to connect to for the current client session.
 * Example
 * USE PortfolioDemo;
 *
 * http://www.datastax.com/docs/1.0/references/cql/USE
 */
public class Use extends AbstractStatement<UseClause> implements Statement<UseClause> {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends UseClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends UseClause>>asList(
			KeySpace.class
	));

    /* Constructors */

	public Use(UseClause clause) {
		super(clause);
	}

    /* Interface Implementations */


// --------------------- Interface Statement ---------------------


	public String build() {
		return String.format("USE %s;", StringUtils.join(clauses, ' '));
	}

	@Override
	public List<Class<? extends UseClause>> getClausesOrder() {
		return ORDER;
	}
}

