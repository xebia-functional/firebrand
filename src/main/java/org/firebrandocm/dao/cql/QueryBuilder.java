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

package org.firebrandocm.dao.cql;

import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.thrift.IndexOperator;
import org.firebrandocm.dao.cql.clauses.*;
import org.firebrandocm.dao.cql.clauses.Set;
import org.firebrandocm.dao.cql.converters.CQLDateValueConverter;
import org.firebrandocm.dao.cql.converters.CQLGenericObjectValueConverter;
import org.firebrandocm.dao.cql.converters.CQLStringValueConverter;
import org.firebrandocm.dao.cql.converters.CQLValueConverter;
import org.firebrandocm.dao.cql.statement.*;

import java.util.*;

/**
 * Constructs CQL queries
 * - SELECT [FIRST N] [REVERSED] <SELECT EXPR> FROM <COLUMN FAMILY> [USING <CONSISTENCY>] [WHERE <CLAUSE>] [LIMIT N];
 * - UPDATE [USING CONSISTENCY ] SET name1 = value1, name2 = value2 WHERE KEY = keyname;
 * - DELETE [COLUMNS] FROM [USING ] WHERE KEY = keyname1 DELETE [COLUMNS] FROM [USING ] WHERE KEY IN (keyname1, keyname2);
 * - TRUNCATE <COLUMN FAMILY>
 * - CREATE KEYSPACE WITH replication_factor = AND strategy_class = [AND strategy_options. = [AND strategy_options. = ]];
 * - CREATE COLUMNFAMILY [(name1 type, name2 type, ...)] [WITH keyword1 = arg1 [AND keyword2 = arg2 [AND ...]]];
 * - CREATE INDEX [index_name] ON <column_family> (column_name);
 * - DROP <KEYSPACE|COLUMNFAMILY> namespace;
 */
public class QueryBuilder {
    /* Fields */

	// ADAPTERS
	private static List<CQLValueConverter> valueConverters;

    /* Static Methods */

	static {
		valueConverters = new ArrayList<CQLValueConverter>(){{
			add(new CQLDateValueConverter());
			add(new CQLStringValueConverter());
			add(new CQLGenericObjectValueConverter());
		}};
	}

	public static void addConverter(int position, CQLValueConverter<?> valueConverter) {
		valueConverters.add(position, valueConverter);
	}

	// STATEMENTS

	public static CreateKeyspace createKeyspace(KeySpace keySpace, WithStrategyClass withStrategyClass, StrategyOptions strategyOptions) {
		return new CreateKeyspace(keySpace, withStrategyClass, strategyOptions);
	}

	public static CreateColumnFamily createColumnFamily(CreateColumnFamilyClause... createColumnFamilyClauses) {
		return new CreateColumnFamily(createColumnFamilyClauses);
	}

	public static AlterColumnFamily alterColumnFamily(ColumnFamily columnFamily, AlterColumn alterColumn) {
		return new AlterColumnFamily(columnFamily, alterColumn);
	}

	public static Select select(SelectClause... clauses) {
		return new Select(clauses);
	}

	public static Update update(UpdateClause... clauses) {
		return new Update(clauses);
	}

	public static Delete delete(DeleteClause... clauses) {
		return new Delete(clauses);
	}

	public static Truncate truncate(TruncateClause clause) {
		return new Truncate(clause);
	}

	public static Drop drop(DropClause clause) {
		return new Drop(clause);
	}

	public static AlterColumn drop(String column) {
		return new AlterColumn(AlterColumn.Type.DROP, new ColumnType(column, null, false, false));
	}

	public static CreateIndex createIndex(CreateIndexClause... clauses) {
		return new CreateIndex(clauses);
	}

	public static Insert insert(InsertClause... clauses) {
		return new Insert(clauses);
	}

	public static Batch batch(BatchCapable... batchCapables) {
		return new Batch(batchCapables);
	}

	public static Use use(UseClause useClause) {
		return new Use(useClause);
	}

	//CLAUSES

	public static Count count() {
		return new Count();
	}

	public static Count count(String... columns) {
		return new Count(columns);
	}

	public static WithStrategyClass withStrategyClass(Class<? extends AbstractReplicationStrategy> replicationStrategyClass) {
		return new WithStrategyClass(replicationStrategyClass);
	}

	public static ColumnDefinitions columnDefinitions(ColumnType... columnTypes) {
		return new ColumnDefinitions(columnTypes);
	}

	public static StorageOptions storageOptions(StorageOption... storageOptions) {
		return new StorageOptions(storageOptions);
	}

	public static StorageOption storageOption(StorageParameter option, String value) {
		return new StorageOption(option, value);
	}

	public static StrategyOptions strategyOptions(StrategyOption... strategyOptions) {
		return new StrategyOptions(strategyOptions);
	}

	public static StrategyOption strategyOption(String option, String value) {
		return new StrategyOption(option, value);
	}

	public static StrategyOption replicationFactor(int value) {
		return new StrategyOption("replication_factor", String.valueOf(value));
	}

	public static StrategyOption dataCenterName(String dataCenterName, int value) {
		return new StrategyOption(dataCenterName, String.valueOf(value));
	}

	public static WriteOptionGroup writeOptions(WriteOption... options) {
		return new WriteOptionGroup(options);
	}

	public static ColumnParenthesis into(String... columns) {
		return new ColumnParenthesis(columns);
	}

	public static ValuesParenthesis values(Object... values) {
		return new ValuesParenthesis(values);
	}

	public static TTL ttl(long seconds) {
		return new TTL(seconds);
	}

	public static Timestamp timestamp(long timestamp) {
		return new Timestamp(timestamp);
	}

	public static ColumnType primaryKey(String column, ColumnDataType dataType) {
		return new ColumnType(column, dataType, false, true);
	}

	public static ColumnParenthesis column(String... columns) {
		return new ColumnParenthesis(columns);
	}

	public static ColumnType column(String column, ColumnDataType dataType) {
		return new ColumnType(column, dataType, false, false);
	}

	public static AlterColumn add(String column, ColumnDataType dataType) {
		return new AlterColumn(AlterColumn.Type.ADD, new ColumnType(column, dataType, false, false));
	}

	public static Assignment add(String column, long value) {
		return new Assignment(false, String.format("'%s'", column), value > 0 ? String.format("'%s' + %d", column, value) : String.format("'%s' - %d", column, Math.abs(value)));
	}

	public static AlterColumn alter(String column, ColumnDataType dataType) {
		return new AlterColumn(AlterColumn.Type.ALTER, new ColumnType(column, dataType, true, false));
	}

	public static IndexName indexName(String indexName) {
		return new IndexName(indexName);
	}

	public static KeySpace keySpace(String keySpace) {
		return new KeySpace(keySpace);
	}

	public static ColumnFamily columnFamily(String columnFamily) {
		return new ColumnFamily(columnFamily);
	}

	public static ColumnFamily columnFamily(Class<?> entityClass) {
		return columnFamily(entityClass.getSimpleName());
	}

	public static OnColumnFamily onColumnFamily(String columnFamily) {
		return new OnColumnFamily(columnFamily);
	}

	public static OnColumnFamily onColumnFamily(Class<?> entityClass) {
		return onColumnFamily(entityClass.getSimpleName());
	}

	public static First first(int first) {
		return new First(first);
	}

	public static Reversed reversed() {
		return new Reversed();
	}

	public static Columns columns(String... columns) {
		return new Columns(columns);
	}

	public static Columns allColumns() {
		return new Columns(false, "*");
	}

	public static ColumnRange columnRange(String from, String to) {
		return new ColumnRange(from, to);
	}

	public static From from(Class<?> entityClass) {
		return new From(entityClass.getSimpleName());
	}

	public static From from(String columnFamily) {
		return new From(columnFamily);
	}

	public static Consistency consistency(ConsistencyType consistencyType) {
		return new Consistency(consistencyType);
	}

	public static Set set(Assignment... assignments) {
		return new Set(assignments);
	}

	public static Assignment assign(String column, String value) {
		return new Assignment(column, value);
	}

	public static Assignment assign(String column, Object value) {
		return assign(column, convert(value));
	}

	@SuppressWarnings("unchecked")
	public static String convert(Object value) {
		String converted = null;
		if (value != null) {
			for (CQLValueConverter converter : valueConverters) {
				if (converter.applyConverter(value)) {
					converted = converter.convert(value);
					break;
				}
			}
		}
		return converted;
	}

	public static Where where(Predicate... predicates) {
		return new Where(predicates);
	}

	public static KeyInPredicate keyIn(String... values) {
		return new KeyInPredicate(values);
	}

	public static KeyInPredicate keyIn(Object... values) {
		return keyIn(convert(values));
	}

	public static String[] convert(Object... values) {
		String[] convertedValues = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			Object value = values[i];
			convertedValues[i] = convert(value);
		}
		return convertedValues;
	}

	public static IndexOperatorPredicate key(String value) {
		return new IndexOperatorPredicate(IndexOperator.EQ, value);
	}

	public static IndexOperatorPredicate key(Object value) {
		return key(convert(value));
	}

	public static IndexOperatorPredicate type(Class<?> classValue) {
		return new IndexOperatorPredicate("___class", IndexOperator.EQ, classValue.getName());
	}

	public static IndexOperatorPredicate eq(String column, String value) {
		return new IndexOperatorPredicate(column, IndexOperator.EQ, value);
	}

	public static IndexOperatorPredicate eq(String column, Object value) {
		return eq(column, convert(value));
	}

	public static IndexOperatorPredicate lt(String column, String value) {
		return new IndexOperatorPredicate(column, IndexOperator.LT, value);
	}

	public static IndexOperatorPredicate lt(String column, Object value) {
		return lt(column, convert(value));
	}

	public static IndexOperatorPredicate lte(String column, String value) {
		return new IndexOperatorPredicate(column, IndexOperator.LTE, value);
	}

	public static IndexOperatorPredicate lte(String column, Object value) {
		return lte(column, convert(value));
	}

	public static IndexOperatorPredicate gt(String column, String value) {
		return new IndexOperatorPredicate(column, IndexOperator.GT, value);
	}

	public static IndexOperatorPredicate gt(String column, Object value) {
		return gt(column, convert(value));
	}

	public static IndexOperatorPredicate gte(String column, Object value) {
		return gte(column, convert(value));
	}

	public static Predicate startAt(String startKey) {
		return gte(null, startKey);
	}

	public static IndexOperatorPredicate gte(String column, String value) {
		return new IndexOperatorPredicate(column, IndexOperator.GTE, value);
	}

	public static Predicate range(String startKey, String endKey) {
		return new Between(null, startKey, endKey);
	}

	public static Predicate between(String column, String from, String to) {
		return new Between(column, from, to);
	}

	public static Predicate between(String column, Object from, Object to) {
		return between(column, convert(from), convert(to));
	}

	public static Limit limit(int limit) {
		return new Limit(limit);
	}

    /* Constructors */

    /**
     * Prevents instantiation
     */
    private QueryBuilder() {
    }
}
