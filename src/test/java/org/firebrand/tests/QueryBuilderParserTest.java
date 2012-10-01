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

package org.firebrand.tests;

import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.firebrand.dao.cql.clauses.ColumnDataType;
import org.firebrand.dao.cql.clauses.ConsistencyType;
import org.firebrand.dao.cql.clauses.StorageParameter;
import org.firebrand.dao.cql.statement.Statement;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static junit.framework.Assert.assertEquals;
import static org.firebrand.dao.cql.QueryBuilder.*;


public class QueryBuilderParserTest {

	private void test(String expected, Statement statement) {
		assertEquals(expected, statement.build());
	}

	private void test(String expected, Object clause) {
		assertEquals(expected, clause.toString());
	}

	@Test
	public void testAlterColumnFamily() throws Exception {
		test(
				"ALTER COLUMNFAMILY ColumnFamily ALTER 'myColumn' TYPE int;",
				alterColumnFamily(
						columnFamily("ColumnFamily"),
						alter("myColumn", ColumnDataType.INT)
                )
		);
		test(
				"ALTER COLUMNFAMILY ColumnFamily ADD 'myColumn' int;",
				alterColumnFamily(
						columnFamily("ColumnFamily"),
						add("myColumn", ColumnDataType.INT)
				)
		);
		test(
				"ALTER COLUMNFAMILY ColumnFamily DROP 'myColumn';",
				alterColumnFamily(
						columnFamily("ColumnFamily"),
						drop("myColumn")
				)
		);
	}

	@Test
	public void testSelect() throws Exception {
		test(
				"SELECT FIRST 5 'a'..'b' FROM ColumnFamily WHERE 'property' = '4' AND 'property' < 'test' AND 'property' <= '0' AND 'other' > '-234' AND 'name' >= '-92334' AND 'a' >= '1' AND 'a' <= '10' AND KEY in ('0', '1', '2', '3') LIMIT 10 USING CONSISTENCY ONE;",
				select(
						first(5),
						columnRange("a", "b"),
						from("ColumnFamily"),
						where(
								eq("property", 4),
								lt("property", "test"),
								lte("property", 0),
								gt("other", -234),
								gte("name", -92334),
								between("a", 1, 10),
								keyIn(0, 1, 2, 3)
						),
						limit(10),
                        consistency(ConsistencyType.ONE)
				)
		);
	}

	@Test
	public void testCreateKeyspace() {
		test(
				"CREATE KEYSPACE KeySpaceName WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",
				createKeyspace(
						keySpace("KeySpaceName"),
						withStrategyClass(SimpleStrategy.class),
						strategyOptions(
								replicationFactor(1)
						)
				)
		);
		test(
				"CREATE KEYSPACE KeySpaceName WITH strategy_class = NetworkTopologyStrategy AND strategy_options:DC1 = 1 AND strategy_options:DC2 = 1 AND strategy_options:DC3 = 1;",
				createKeyspace(
						keySpace("KeySpaceName"),
						withStrategyClass(NetworkTopologyStrategy.class),
						strategyOptions(
								dataCenterName("DC1", 1),
								dataCenterName("DC2", 1),
								dataCenterName("DC3", 1)
						)
				)
		);
	}

	@Test
	public void testCreateColumnFamily() {
		test(
				"CREATE COLUMNFAMILY ColumnFamilyName (\n" +
						"'primaryKeyColumn' uuid PRIMARY KEY,\n" +
						"'a' text,\n" +
						"'b' int) \n" +
						"WITH comment = comment\n" +
						" AND read_repair_chance = 1;",
				createColumnFamily(
						columnFamily("ColumnFamilyName"),
						columnDefinitions(
								primaryKey("primaryKeyColumn", ColumnDataType.UUID),
								column("a", ColumnDataType.TEXT),
								column("b", ColumnDataType.INT)
						),
						storageOptions(
								storageOption(StorageParameter.COMMENT, "comment"),
								storageOption(StorageParameter.READ_REPAIR_CHANCE, "1")
						)
				)
		);
	}

	@Test
	public void testPrimaryKey() {
		test("'a' text PRIMARY KEY", primaryKey("a", ColumnDataType.TEXT));
	}

	@Test
	public void testWithStrategyClass() {
		test("WITH strategy_class = NetworkTopologyStrategy", withStrategyClass(NetworkTopologyStrategy.class));
	}

	@Test
	public void testStrategyOptions() {
		test("AND strategy_options:a = b", strategyOptions(strategyOption("a", "b")));
	}

	@Test
	public void testStrategyOption() {
		test("strategy_options:a = b", strategyOption("a", "b"));
	}

	@Test
	public void testReplicationFactor() {
		test("strategy_options:replication_factor = 1", replicationFactor(1));
	}

	@Test
	public void testDataCenterName() {
		test("strategy_options:DC1 = 1", dataCenterName("DC1", 1));
	}

	@Test
	public void testCount() throws Exception {
		test("COUNT(*)", count());
	}

	@Test
	public void testColumnRange() throws Exception {
		test("'a'..'c'", columnRange("a", "c"));
	}

	@Test
	public void testUse() throws Exception {
		test(
				"USE KeySpaceName;",
				use(
						keySpace("KeySpaceName")
				)
		);
	}

	@Test
	public void testUpdate() throws Exception {
		test(
				"UPDATE USING CONSISTENCY ONE AND TIMESTAMP 546745 AND TTL 34352 SET 'propertya' = '4', 'propertyb' = 'test', 'propertyc' = '0' WHERE KEY = '4';",
				update(
						writeOptions(
								consistency(ConsistencyType.ONE),
								timestamp(546745),
								ttl(34352)
						),
						set(
								assign("propertya", 4),
								assign("propertyb", "test"),
								assign("propertyc", 0)
						),
						where(
								key(4)
						)
				)
		);
	}

	@Test
	public void testDelete() throws Exception {
		test("DELETE 'a', 'b', 'c' FROM ColumnFamily USING CONSISTENCY ONE AND TIMESTAMP 21342134 WHERE KEY in ('0', '1', '2', '3');",
				delete(
						columns("a", "b", "c"),
						from("ColumnFamily"),
						writeOptions(
								consistency(ConsistencyType.ONE),
								timestamp(21342134)
						),
						where(
								keyIn(0, 1, 2, 3)
						)
				)
		);
	}

	@Test
	public void testTruncate() throws Exception {
		test(
				"TRUNCATE ColumnFamily;",
				truncate(
						columnFamily("ColumnFamily")
				)
		);
	}

	@Test
	public void testDrop() throws Exception {
		test(
				"DROP INDEX myIndex;",
				drop(
						indexName("myIndex")
				)
		);
		test(
				"DROP COLUMNFAMILY ColumnFamily;",
				drop(
						columnFamily("ColumnFamily")
				)
		);
		test(
				"DROP KEYSPACE KeySpace;",
				drop(
						keySpace("KeySpace")
				)
		);
	}

	@Test
	public void testCreateIndex() throws Exception {
		test(
				"CREATE INDEX myIndex ON ColumnFamily ('myColumn');",
				createIndex(
						onColumnFamily("ColumnFamily"),
						indexName("myIndex"),
						column("myColumn")
				)
		);
	}

	@Test
	public void testInsert() throws Exception {
		test(
				"INSERT INTO ('a', 'b', 'c') VALUES ('0', '1', '2') USING CONSISTENCY ONE AND TTL 3600;",
				insert(
						writeOptions(
								consistency(ConsistencyType.ONE),
								ttl(3600)
						),
						into("a", "b", "c"),
						values(0, 1, 2)

				)
		);
		test(
				"INSERT INTO Test ('KEY', 'a', 'b', 'c') VALUES ('1', '0', '1', '2');",
				insert(
                        columnFamily("Test"),
						into("KEY", "a", "b", "c"),
						values(1, 0, 1, 2)
				)
		);
	}

	@Test
	public void testBatch() throws Exception {
		test("BEGIN BATCH\n" +
				"USING CONSISTENCY LOCAL_QUOROM AND TTL 3600 AND TIMESTAMP 2342134\n" +
				"DELETE 'a', 'b', 'c' FROM ColumnFamily WHERE KEY in ('0', '1', '2', '3');\n" +
				"INSERT INTO ('a', 'b', 'c') VALUES ('0', '1', '2');\n" +
				"UPDATE ColumnFamily SET 'propertya' = '4', 'propertyb' = 'test', 'propertyc' = '0', 'counter' = 'counter' + 1, 'counterb' = 'counterb' - 98 WHERE KEY = '4';\n" +
				"APPLY BATCH;", batch(
				writeOptions(
						consistency(ConsistencyType.LOCAL_QUOROM),
						ttl(3600),
						timestamp(2342134)
				),
				delete(
						columns("a", "b", "c"),
						from("ColumnFamily"),
						where(
								keyIn(0, 1, 2, 3)
						)
				),
				insert(
						into("a", "b", "c"),
						values(0, 1, 2)

				),
				update(
						columnFamily("ColumnFamily"),
						set(
								assign("propertya", 4),
								assign("propertyb", "test"),
								assign("propertyc", 0),
								add("counter", 1),
								add("counterb", -98)
						),
						where(
								key(4)
						)
				)

		)
		);
	}

	@Test
	public void testWriteOptions() throws Exception {
		test("TIMESTAMP 0 AND TTL 1 AND USING CONSISTENCY ANY", writeOptions(timestamp(0), ttl(1), consistency(ConsistencyType.ANY)));
	}

	@Test
	public void testInto() throws Exception {
		test("('a', 'b', 'c')", into("a", "b", "c"));
	}

	@Test
	public void testValues() throws Exception {
		test("VALUES ('a', 'b', 'c')", values("a", "b", "c"));
	}

	@Test
	public void testType() throws Exception {
		test(String.format("'___class' = '%s'", FirstEntity.class.getName()), type(FirstEntity.class));
	}

	@Test
	public void testTtl() throws Exception {
		test("TTL 1", ttl(1));
	}

	@Test
	public void testTimestamp() throws Exception {
		test("TIMESTAMP 1", timestamp(1));
	}

	@Test
	public void testAlter() throws Exception {
		test("ALTER 'a' TYPE text", alter("a", ColumnDataType.TEXT));
	}

	@Test
	public void testIndexName() throws Exception {
		test("a", indexName("a"));
	}

	@Test
	public void testKeySpace() throws Exception {
		test("a", keySpace("a"));
	}

	@Test
	public void testColumnFamily() throws Exception {
		test("a", columnFamily("a"));
	}

	@Test
	public void testOnColumnFamily() throws Exception {
		test("ON a", onColumnFamily("a"));
	}

	@Test
	public void testColumn() throws Exception {
		test("('a', 'b', 'c')", column("a", "b", "c"));
	}

	@Test
	public void testFirst() throws Exception {
		test("FIRST 3", first(3));
	}

	@Test
	public void testReversed() throws Exception {
		test("REVERSED", reversed());
	}

	@Test
	public void testColumns() throws Exception {
		test("'a', 'b', 'c'", columns("a", "b", "c"));
	}

	@Test
	public void testAllColumns() throws Exception {
		test("*", allColumns());
	}

	@Test
	public void testFrom() throws Exception {
		test(String.format("FROM %s", FirstEntity.class.getSimpleName()), from(FirstEntity.class));
		test("FROM FirstEntity", from("FirstEntity"));
	}

	@Test
	public void testConsistency() throws Exception {
		test("USING CONSISTENCY ANY", consistency(ConsistencyType.ANY));
	}

	@Test
	public void testSet() throws Exception {
		test("SET 'a' = 'b', 'b' = 'c'", set(assign("a", "b"), assign("b", "c")));
	}

	@Test
	public void testAssign() throws Exception {
		test("'a' = 'b'", assign("a", "b"));
	}

	@Test
	public void testAddCounter() throws Exception {
		test("'a' = 'a' + 1", add("a", 1));
		test("'a' = 'a' - 1", add("a", -1));
	}

	@Test
	public void testAdd() throws Exception {
		test("ADD 'a' uuid", add("a", ColumnDataType.UUID));
	}

	@Test
	public void testWhere() throws Exception {
		test("WHERE 'b' = 'a' AND 'c' = 'd' AND 'x' > '5'", where(eq("b","a"),eq("c","d"), gt("x", "5")));
	}

	@Test
	public void testKeyIn() throws Exception {
		test("KEY in ('1', '2', '3')", keyIn(1,2,3));
	}

	@Test
	public void testKey() throws Exception {
		test("KEY = '1'", key(1));
	}

	@Test
	public void testEq() throws Exception {
		test("'a' = 'b'", eq("a", "b"));
	}

	@Test
	public void testLt() throws Exception {
		test("'a' < 'b'", lt("a", "b"));
	}

	@Test
	public void testLte() throws Exception {
		test("'a' <= 'b'", lte("a", "b"));
	}

	@Test
	public void testGt() throws Exception {
		test("'a' > 'b'", gt("a", "b"));
	}

	@Test
	public void testGte() throws Exception {
		test("'a' >= 'b'", gte("a", "b"));
	}

	@Test
	public void testBetween() throws Exception {
		test("'a' >= '1' AND 'a' <= '10'", between("a", 1, 10));
	}

	@Test
	public void testStartAt() throws Exception {
		test("KEY >= 'a'", startAt("a"));
	}

	@Test
	public void testRange() throws Exception {
		test("KEY >= 'a' AND KEY <= 'z'", range("a", "z"));
	}

	@Test
	public void testLimit() throws Exception {
		test("LIMIT 1", limit(1));
	}

	@Test
	public void testConvert() throws Exception {
		assertEquals("1", convert(1));
		assertEquals("1.0", convert(1.0));
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		format.setTimeZone(TimeZone.getTimeZone("PST"));
		assertEquals("1325404800000", convert(format.parse("2012-01-01")));
	}
}
