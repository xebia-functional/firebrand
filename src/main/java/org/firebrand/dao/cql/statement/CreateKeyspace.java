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
import org.firebrand.dao.cql.clauses.CreateKeySpaceClause;
import org.firebrand.dao.cql.clauses.KeySpace;
import org.firebrand.dao.cql.clauses.StrategyOptions;
import org.firebrand.dao.cql.clauses.WithStrategyClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * DELETE »
 * CREATE KEYSPACE
 * Define a new keyspace and its replica placement strategy.
 * <p/>
 * Synopsis
 * CREATE KEYSPACE <ks_name>
 * WITH strategy_class = <value>
 * [ AND strategy_options:<option> = <value> [...] ];
 * Description
 * CREATE KEYSPACE creates a new keyspace and sets the replica placement strategy (and associated replication options) for the keyspace.
 * <p/>
 * See Choosing Keyspace Replication Options for guidance on how to best configure replication strategy and strategy options for your cluster.
 * <p/>
 * Parameters
 * <ks_name>
 * <p/>
 * Defines the name of the keyspace. Valid keyspace names are strings of alpha-numeric characters and underscores, and must begin with a letter.
 * WITH strategy_class=<value>
 * <p/>
 * Required. Sets the replica placement strategy to use for this keyspace. The most common choices are NetworkTopologyStrategy or SimpleStrategy.
 * AND strategy_options:<option>=<value>
 * <p/>
 * Certain additional options must be defined depending on the replication strategy chosen.
 * <p/>
 * For SimpleStrategy, you must specify the replication factor in the format of strategy_options:replication_factor=<number>.
 * <p/>
 * For NetworkTopologyStrategy, you must specify the number of replicas per data center in the format of strategy_options:<datacenter_name>=<number>. Note that what you specify for <datacenter_name> depends on the cluster-configured snitch you are using. There is a correlation between the data center name defined in the keyspace strategy_options and the data center name as recognized by the snitch you are using. The nodetool ring command prints out data center names and rack locations of your nodes if you are not sure what they are.
 * <p/>
 * Examples
 * Define a new keyspace using the simple replication strategy:
 * <p/>
 * CREATE KEYSPACE MyKeyspace WITH strategy_class = 'SimpleStrategy'
 * AND strategy_options:replication_factor = 1;
 * Define a new keyspace using a network-aware replication strategy and snitch. This example assumes you are using the PropertyFileSnitch and your data centers are named DC1 and DC2 in the cassandra-topology.properties file:
 * <p/>
 * CREATE KEYSPACE MyKeyspace WITH strategy_class = 'NetworkTopologyStrategy'
 * AND strategy_options:DC1 = 3 AND strategy_options:DC2 = 3;
 *
 * http://www.datastax.com/docs/1.0/references/cql/CREATE_KEYSPACE
 *
 */
public class CreateKeyspace extends AbstractStatement<CreateKeySpaceClause> implements Statement<CreateKeySpaceClause>, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends CreateKeySpaceClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends CreateKeySpaceClause>>asList(
			KeySpace.class, WithStrategyClass.class, StrategyOptions.class
	));

    /* Constructors */

	public CreateKeyspace(CreateKeySpaceClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("CREATE KEYSPACE %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends CreateKeySpaceClause>> getClausesOrder() {
		return ORDER;
	}
}
