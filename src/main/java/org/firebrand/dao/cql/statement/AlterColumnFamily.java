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
import org.firebrand.dao.cql.clauses.AlterColumn;
import org.firebrand.dao.cql.clauses.AlterColumnFamilyClause;
import org.firebrand.dao.cql.clauses.ColumnFamily;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * ALTER COLUMNFAMILY
 * <p/>
 * Manipulates the column metadata of a column family.
 * <p/>
 * Synopsis
 * <p/>
 * ALTER COLUMNFAMILY <name>
 * ALTER <column_name> TYPE <data_type>
 * | ADD <column_name> <data_type>
 * | DROP <column_name> ;
 * <p/>
 * Description
 * <p/>
 * ALTER COLUMNFAMILY is used to manipulate the column metadata of a static column family.
 * You can also use the alias ALTER TABLE. It allows you to add new columns, drop existing columns, or change the data
 * storage type of existing columns. No results are returned.
 * <p/>
 * See CQL Data Types for the available data types.
 * http://www.datastax.com/docs/1.0/references/cql/index#cql-data-types
 * <p/>
 * Parameters
 * <p/>
 * The name of the column family to be altered.
 * <p/>
 * ALTER <column_name> TYPE <data_type>
 * Changes the data type of an existing column. The named column must exist in the column family schema definition and
 * have a type defined, but the column does not have to exist in any rows currently stored in the column family.
 * Note that when you change the data type of a column, existing data is not changed or validated on disk.
 * If existing data is not compatible with the defined type, then this will cause your CQL driver or other client
 * interfaces to return errors when accessing the data. For example if you changed a column type to int,
 * but the existing stored data had non-numeric characters in it, your client requests of that data would report errors.
 * <p/>
 * ADD <column_name> <data_type>
 * Adds a typed column to the column metadata of a column family schema definition. The column must not already have a
 * type in the column family metadata.
 * <p/>
 * DROP <column_name> <data_type>
 * Removes a column from the column family metadata. Note that this does not remove the column from current rows. It
 * just removes the metadata saying that the values stored under that column name are expected to be a certain type.
 * <p/>
 * Examples
 * <p/>
 * ALTER COLUMNFAMILY users ALTER email TYPE varchar;
 * ALTER COLUMNFAMILY users ADD gender varchar;
 * ALTER COLUMNFAMILY users DROP gender;
 * <p/>
 * http://www.datastax.com/docs/1.0/references/cql/ALTER_COLUMNFAMILY
 */
public class AlterColumnFamily extends AbstractStatement<AlterColumnFamilyClause> implements Statement<AlterColumnFamilyClause>, MutationStatement {
    /* Fields */

	@SuppressWarnings("unchecked")
	private static List<Class<? extends AlterColumnFamilyClause>> ORDER = Collections.unmodifiableList(Arrays.<Class<? extends AlterColumnFamilyClause>>asList(
			ColumnFamily.class, AlterColumn.class
	));

    /* Constructors */

	public AlterColumnFamily(AlterColumnFamilyClause... clauses) {
		super(clauses);
	}

    /* Interface Implementations */


// --------------------- Interface MutationOrSelectionStatement ---------------------


	public String build() {
		return String.format("ALTER COLUMNFAMILY %s;", StringUtils.join(clauses, ' '));
	}

// --------------------- Interface Statement ---------------------

	@Override
	public List<Class<? extends AlterColumnFamilyClause>> getClausesOrder() {
		return ORDER;
	}
}
