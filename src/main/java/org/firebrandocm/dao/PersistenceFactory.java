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

package org.firebrandocm.dao;


import org.apache.cassandra.thrift.ConsistencyLevel;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Interface for persistence factories that manage entities and provide a persistence operations and context
 */
public interface PersistenceFactory {
    /* Fields */

    /**
     * The property name for all classes stored at the column:row level to introspect the entity that contains the
     * properties persisted in a row's columns
     */
    String CLASS_PROPERTY = "___class";

    /* Misc */

    /**
     * Deletes columns by name from column family
     *
     * @param columnFamily the column family
     * @param key          the key
     * @param columns      the columns to be deleted by name
     */
    void deleteColumns(String columnFamily, String key, String... columns);

    /**
     * Executes a query
     *
     * @param expectedResult the result expected from the query execution
     * @param query          the query
     * @param <T>            the result type
     * @return the result
     */
    <T> T executeQuery(Class<T> expectedResult, Query query);

    /**
     * @param entityClass the class
     * @param key          the id
     * @param <T>         the entity type
     * @return an entity from the data store looked up by its id
     */
    <T> T get(Class<T> entityClass, String key);

    /**
     * Fetch a map of columns and their values
     *
     * @param columnFamily the column family
     * @param key          the column family key
     * @param reversed     if the order should be reversed
     * @param columns      the column names
     * @return a map of columns and their values
     */
    Map<String, ByteBuffer> getColumns(String columnFamily, String key, boolean reversed, String... columns);

    /**
     * Fetch a map of columns and their values
     *
     * @param columnFamily the column family
     * @param key          the column family key
     * @param limit        of columns
     * @param reversed     if the order should be reversed
     * @param fromColumn   from column
     * @param toColumn     to column
     * @return a map of columns and their values
     */
    Map<String, ByteBuffer> getColumns(String columnFamily, String key, int limit, boolean reversed, String fromColumn, String toColumn);

    /**
     * @return the default consistency level
     */
    ConsistencyLevel getDefaultConsistencyLevel();

    /**
     * @return the default keyspace
     */
    String getDefaultKeySpace();

    /**
     * @param entityClass the class type for this instance
     * @param <T>         the type of class to be returned
     * @return an instance of this type after transformation of its accessors to notify the persistence context that there are ongoing changes
     */
    <T> T getInstance(Class<T> entityClass);

    /**
     * Obtains an entity key
     *
     * @param entity the entity
     * @return the key
     */
    String getKey(Object entity);

    /**
     * The list of managed class by this factory
     *
     * @return The list of managed class by this factory
     */
    List<Class<?>> getManagedClasses();

    /**
     * Get a list of entities given a query
     *
     * @param type  the type of objects to expect back
     * @param query the query
     * @param <T>   the result type
     * @return the list of entities
     */
    <T> List<T> getResultList(Class<T> type, Query query);

    /**
     * Get a single result from a CQL query
     *
     * @param type  the type of objects to expect back
     * @param query the query
     * @param <T>   the entity type
     * @return the resulting entity
     */
    <T> T getSingleResult(Class<T> type, Query query);

    /**
     * Inserts columns based on a map representing keys with properties and their corresponding values
     *
     * @param columnFamily  the column family
     * @param key           the column family key
     * @param keyValuePairs the map with keys and values
     */
    void insertColumns(String columnFamily, String key, Map<String, Object> keyValuePairs);

    /**
     * Entry point method to persist and arbitrary list of objects into the datastore
     *
     * @param entities the entities to be persisted
     */
    <T> void persist(T... entities);

    /**
     * @param entities the entities to be removed from the data store
     */
    <T> void remove(T... entities);
}
