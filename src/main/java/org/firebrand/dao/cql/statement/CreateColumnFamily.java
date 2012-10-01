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
import org.firebrand.dao.cql.clauses.ColumnDefinitions;
import org.firebrand.dao.cql.clauses.ColumnFamily;
import org.firebrand.dao.cql.clauses.CreateColumnFamilyClause;
import org.firebrand.dao.cql.clauses.StorageOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * CREATE COLUMNFAMILY
 * Define a new column family.
 * <p/>
 * Synopsis
 * CREATE COLUMNFAMILY <cf_name> (
 * <key_column_name> <data_type> PRIMARY KEY
 * [, <column_name> <data_type> [, ...] ] )
 * [ WITH <storage_parameter> = <value>
 * [AND <storage_parameter> = <value> [...] ] ];
 * Description
 * CREATE COLUMNFAMILY creates a new column family under the current keyspace. You can also use the alias CREATE TABLE.
 * <p/>
 * The only schema information that must be defined for a column family is the primary key (or row key) and its associated data type. Other column metadata can be defined as needed.
 * <p/>
 * See CQL Data Types for the available data types.
 * <p/>
 * See CQL Column Family Storage Parameters for the available storage parameters you can define on a column family when using CQL.
 * <p/>
 * Parameters
 * <cf_name>
 * <p/>
 * Defines the name of the column family. Valid column family names are strings of alpha-numeric characters and underscores, and must begin with a letter.
 * <key_column_name> <data_type> PRIMARY KEY
 * <p/>
 * Columns are defined in a comma-separated list enclosed in parenthesis. The first column listed in the column definition is always the row key (or primary key of the column family), and is required. Any other column definitions are optional.
 * <p/>
 * Row keys can be defined using the generic KEY keyword, or can be given a column name to use as the alias for the row key. The row key data type must be compatible with the partitioner configured for your Cassandra cluster. For example, OrderPreservingPartitioner requires UTF-8 row keys.
 * <p/>
 * <column_name> <data_type>
 * <p/>
 * Defines column metadata for static column families (when you know what the column names will be ahead of time).
 * WITH <storage_parameter> = <value>
 * <p/>
 * Defines certain storage parameters on a column family. See CQL Column Family Storage Parameters for the available storage parameters you can define on a column family when using CQL.
 * <p/>
 * For dynamic column families (where you do not know the column names ahead of time), it is best practice to still define a default data type for column names ( using WITH comparator=<data_type>) and values (using WITH default_validation=<data_type>).
 * <p/>
 * Examples
 * Dynamic column family definition:
 * <p/>
 * CREATE COLUMNFAMILY user_events (user text PRIMARY KEY)
 * WITH comparator=timestamp AND default_validation=int;
 * Static column family definition:
 * <p/>
 * CREATE COLUMNFAMILY users (
 * KEY uuid PRIMARY KEY,
 * username text,
 * email text )
 * WITH comment='user information'
 * AND read_repair_chance = 1.0;
 *
 * http://www.datastax.com/docs/1.0/references/cql/CREATE_COLUMNFAMILY
 */
public class CreateColumnFamily extends AbstractStatement<CreateColumnFamilyClause> implements Statement<CreateColumnFamilyClause>, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends CreateColumnFamilyClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends CreateColumnFamilyClause>>asList(
			ColumnFamily.class, ColumnDefinitions.class, StorageOptions.class
	));

    /* Constructors */

	public CreateColumnFamily(CreateColumnFamilyClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("CREATE COLUMNFAMILY %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends CreateColumnFamilyClause>> getClausesOrder() {
		return ORDER;
	}
}
