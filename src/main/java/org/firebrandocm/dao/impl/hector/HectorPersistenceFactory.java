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

package org.firebrandocm.dao.impl.hector;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.BatchSizeHint;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.beanutils.NestedNullException;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.firebrandocm.dao.*;
import org.firebrandocm.dao.events.ColumnEventListener;
import org.firebrandocm.dao.events.EntityEventListener;
import org.firebrandocm.dao.events.Event;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static org.firebrandocm.dao.cql.QueryBuilder.*;

/**
 * An Hector based impl for the Persistence Factory
 */
public class HectorPersistenceFactory extends AbstractPersistenceFactory {
    /* Fields */

    /**
     * the cluster name
     */
    private String clusterName;

    /**
     * The cluster credentials
     */
    private Map<String, String> credentials;

    /**
     * Whether to auto discover hosts
     */
    private boolean autoDiscoverHosts;

    /**
     * A cassandra host configurator instance.
     * If non provided a default one will be created on init
     */
    private CassandraHostConfigurator cassandraHostConfigurator;

    /**
     * The cluster instance
     */
    private Cluster cluster;

    /* Getters & Setters */

    /**
     * Sets the autoDiscoverHosts property
     *
     * @param autoDiscoverHosts whether to auto discover hosts
     */
    public void setAutoDiscoverHosts(boolean autoDiscoverHosts) {
        this.autoDiscoverHosts = autoDiscoverHosts;
    }

    /**
     * Sets a cassandra host configurator instance.
     * If non provided a default one will be created on init
     *
     * @param cassandraHostConfigurator the cassandra host configurator instance
     */
    public void setCassandraHostConfigurator(CassandraHostConfigurator cassandraHostConfigurator) {
        this.cassandraHostConfigurator = cassandraHostConfigurator;
    }

    /**
     * Sets the cluster name
     *
     * @param clusterName the cluster name
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Sets the cluster credentials
     *
     * @param credentials the credentials
     */
    public void setCredentials(Map<String, String> credentials) {
        this.credentials = credentials;
    }

    /* Interface Implementations */


// --------------------- Interface PersistenceFactory ---------------------


    /**
     * Deletes columns by name from column family
     *
     * @param colFamily the column family
     * @param key       the key
     * @param columns   the columns to be deleted by name
     */
    @Override
    public void deleteColumns(String colFamily, String key, String... columns) {
        Keyspace keyspace = HFactory.createKeyspace(getDefaultKeySpace(), cluster);
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get(), new BatchSizeHint(1, columns.length));
        for (String property : columns) {
            fireColumnEvent(Event.Column.PRE_COLUMN_DELETION, null, property, key, colFamily, null);
            mutator.addDeletion(key, colFamily, property, StringSerializer.get());
            log.debug(String.format("\tD: %s", property));
            fireColumnEvent(Event.Column.POST_COLUMN_DELETION, null, property, key, colFamily, null);
        }
        mutator.execute();
    }

    /**
     * Get an entity by id
     *
     * @param entityClass the class
     * @param key          the key
     * @param <T>         the entity type
     * @return the entity
     */
    public <T> T get(Class<T> entityClass, String key) {
        if (log.isDebugEnabled()) log.debug(String.format("get (start): %s, %s", entityClass, key));
        fireEntityEvent(Event.Entity.PRE_LOAD, entityClass, key);
        T result = getSingleResult(entityClass, Query.get(select(allColumns(), from(entityClass), where(key(key)))));
        fireEntityEvent(Event.Entity.POST_LOAD, result);
        if (log.isDebugEnabled()) log.debug(String.format("get (end): %s", result));
        return result;
    }

    /**
     * Fetch a map of columns and their values
     *
     * @param columnFamily the column family
     * @param key          the column family key
     * @param reversed     if the order should be reversed
     * @param columns      the column names
     * @return a map of columns and their values
     */
    @Override
    public Map<String, ByteBuffer> getColumns(String columnFamily, String key, boolean reversed, String... columns) {
        return getColumns(
                Query.get(select(reversed ? reversed() : null, columns(columns), from(columnFamily), where(key(key)))).getQuery()
        );
    }

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
    @Override
    public Map<String, ByteBuffer> getColumns(String columnFamily, String key, int limit, boolean reversed, String fromColumn, String toColumn) {
        return getColumns(
                Query.get(select(reversed ? reversed() : null, first(limit), columnRange(fromColumn, toColumn), from(columnFamily), where(key(key)))).getQuery()
        );
    }

    /**
     * Get a list of entities given a query
     *
     * @param type  the type of objects to expect back
     * @param query the query
     * @param <T>   the result type
     * @return the list of entities
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getResultList(Class<T> type, Query query) {
        if (log.isDebugEnabled()) log.debug(String.format("getResultList (start): %s", query.getQuery()));
        List<T> result = new ArrayList<T>();
        try {
            ClassMetadata metadata = getClassMetadata(type);
            if (metadata == null && !Long.class.isAssignableFrom(type))
                throw new IllegalArgumentException(String.format("type: %s not recognized as ColumnFamily or returnable value", type));
            CqlQuery<String, String, Object> indexedQuery = getCQLQuery(type, query.getQuery());
            CqlRows<String, String, Object> cqlRows = indexedQuery.execute().get();
            if (cqlRows != null) {
                T entity = null;
                if (metadata == null) {
                    entity = (T) Long.valueOf(cqlRows.getAsCount());
                    result.add(entity);
                } else {
                    for (Row<String, String, Object> orderedRow : cqlRows.getList()) {
                        ColumnSlice<String, Object> slice = orderedRow.getColumnSlice();
                        List<HColumn<String, Object>> columns = slice.getColumns();
                        if (columns.size() > 0) {
                            entity = serializeColumns(orderedRow.getKey(), metadata, type, columns, null, false);
                            result.add(entity);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (log.isDebugEnabled()) log.debug(String.format("getResultList (end): %s", result.size()));
        return result;
    }

    /**
     * Get a single result from a CQL query
     *
     * @param type  the type of objects to expect back
     * @param query the query
     * @param <T>   the entity type
     * @return the resulting entity
     */
    @Override
    public <T> T getSingleResult(Class<T> type, Query query) {
        List<T> results = getResultList(type, query);
        if (results.size() > 1) {
            throw new IllegalStateException(String.format("Expected a single result but found %d", results.size()));
        }
        return results.size() == 1 ? results.get(0) : null;
    }

    /**
     * Inserts columns based on a map representing keys with properties and their corresponding values
     *
     * @param colFamily     the column family
     * @param key           the column family key
     * @param keyValuePairs the map with keys and values
     */
    @Override
    public void insertColumns(String colFamily, String key, Map<String, Object> keyValuePairs) {
        Keyspace keyspace = HFactory.createKeyspace(getDefaultKeySpace(), cluster);
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get(), new BatchSizeHint(1, keyValuePairs.size()));
        for (Map.Entry<String, Object> entry : keyValuePairs.entrySet()) {
            String property = entry.getKey();
            Object value = entry.getValue();
            HColumn<String, Object> column = createColumn(property, value, StringSerializer.get(), new TypeConverterSerializer<Object>(value));
            fireColumnEvent(Event.Column.PRE_COLUMN_MUTATION, null, property, key, colFamily, column);
            mutator.addInsertion(key, colFamily, column);
            log.debug(String.format("\tI: %s : %s ", property, value));
            fireColumnEvent(Event.Column.POST_COLUMN_MUTATION, null, property, key, colFamily, column);
        }
        mutator.execute();
    }

    /**
     * Entry point method to persist and arbitrary list of objects into the datastore
     *
     * @param entities the entities to be persisted
     */
    public void persist(Object... entities) {
        try {
            createKeysIfNeeded(entities);
            persistAll(entities);
            fireEntityEvent(Event.Entity.POST_COMMIT, entities);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes a list of entities from the data store
     *
     * @param entities the entities to be removed from the data store
     */
    public void remove(Object... entities) {
        log.debug(String.format("START remove(%s)", Arrays.toString(entities)));
        try {
            for (Object entity : entities) {
                ClassMetadata classMetadata = getClassMetadata(entity.getClass());
                Mutator<String> mutator = getMutator(classMetadata);
                fireEntityEvent(Event.Entity.PRE_DELETE, entity);
                String colFamily = classMetadata.getColumnFamily();
                String key = getKey(entity);
                if (key == null) {
                    log.warn(String.format("entity: %s had no value for key and was ignored", entity));
                } else {
                    mutator.addDeletion(key, colFamily);
                    mutator.execute();
                    fireEntityEvent(Event.Entity.POST_DELETE, entity);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        log.debug(String.format("END remove(%s)", Arrays.toString(entities)));
    }

    /* Misc */

    /**
     * Destroy method that shutdowns this factory
     *
     * @see org.firebrandocm.dao.AbstractPersistenceFactory#destroy()
     */
    public void destroy() {
        if (isDropOnDestroy()) {
            executeQuery(Void.class, Query.get(drop(keySpace(getDefaultKeySpace()))));
        }
        HFactory.shutdownCluster(cluster);
        super.destroy();
    }

    /**
     * Executes a query
     *
     * @param expectedResult the result expected from the query execution
     * @param query          the query
     * @param <T>            the result type
     * @return the result
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T executeQuery(Class<T> expectedResult, Query query) {
        return (T) getCQLExecuteQuery(query.getQuery()).execute();
    }

    /**
     * Private helper that returns a CqlExecuteQuery
     * @param query the query
     * @param <T>  the entity type
     * @param <V> the value type
     * @return the CqlQuery
     */
    private <T, V> CqlExecuteQuery<V> getCQLExecuteQuery(String query) {
        CqlExecuteQuery<V> cqlQuery = new CqlExecuteQuery<V>(getDefaultKeyspace());
        cqlQuery.setQuery(query);
        return cqlQuery;
    }

    /**
     * Private helper to obtain a CqlQuery given an entity class type and a cql query
     * @param type the class type
     * @param query the cql query
     * @param <T>  The type of entity
     * @param <V>  The type of value
     * @return a CqlQuery
     */
    private <T, V> CqlQuery<String, String, V> getCQLQuery(Class<T> type, String query) {
        ClassMetadata classMetadata = getClassMetadata(type);
        Keyspace keyspace;
        if (classMetadata == null) { //this is not a managed class such as requesting a long for now as workaround the first keyspace will be selected //todo change in the future to be able to pass akeyspace
            keyspace = getDefaultKeyspace();
        } else {
            keyspace = getKeyspace(classMetadata);
        }
        CqlQuery<String, String, V> cqlQuery = new CqlQuery<String, String, V>(keyspace, StringSerializer.get(), StringSerializer.get(), new TypeConverterSerializer<V>());
        cqlQuery.setQuery(query);
        cqlQuery.setSuppressKeyInColumns(true);
        return cqlQuery;
    }

    /**
     * Fetch a map of columns and their values
     *
     * @param query a cql query
     * @return the resulting columns and their values
     */
    private Map<String, ByteBuffer> getColumns(String query) {
        Map<String, ByteBuffer> resultMap = new LinkedHashMap<String, ByteBuffer>();
        CqlQuery<String, String, Object> cqlQuery = new CqlQuery<String, String, Object>(getDefaultKeyspace(), StringSerializer.get(), StringSerializer.get(), new TypeConverterSerializer<Object>());
        cqlQuery.setQuery(query);
        cqlQuery.setSuppressKeyInColumns(true);
        QueryResult<CqlRows<String, String, Object>> results = cqlQuery.execute();
        CqlRows<String, String, Object> rows = results.get();
        for (Row<String, String, Object> row : rows) {
            ColumnSlice<String, Object> slice = row.getColumnSlice();
            for (HColumn<String, Object> column : slice.getColumns()) {
                resultMap.put(column.getName(), column.getValueBytes());
            }
        }
        return resultMap;
    }

    /**
     *
     * @return the default keyspace
     */
    private Keyspace getDefaultKeyspace() {
        return HFactory.createKeyspace(getDefaultKeySpace(), cluster);
    }

    /**
     * Initializes the factory
     */
    public synchronized void init() throws Exception {
        super.init();
        log.debug("initializing factory");
        //favor an existing cassandraHostConfigurator
        if (cassandraHostConfigurator == null) {
            cassandraHostConfigurator = new CassandraHostConfigurator();
            cassandraHostConfigurator.setPort(getThriftPort());
            cassandraHostConfigurator.setHosts(StringUtils.join(getContactNodes(), ','));
            cassandraHostConfigurator.setAutoDiscoverHosts(autoDiscoverHosts);
        }
        cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator, credentials);
        initializeTypeConverters();
        initializeKeyspaceDefinitions();
        initializeSchema();
        log.debug("factory initialized");
    }

    /**
     * Private helper to initialize the schema
     */
    private void initializeSchema() throws Exception {
        if (keyspaceDefinitions.size() == 0)
            throw new IllegalStateException("no keyspace definitions founds, maybe add some entities to the factory");
        for (KeyspaceDefinition keyspaceDefinition : ThriftKsDef.fromThriftList(new ArrayList<KsDef>(keyspaceDefinitions.values()))) {
            KeyspaceDefinition existingKeyspace = cluster.describeKeyspace(keyspaceDefinition.getName());
            if (existingKeyspace == null) {
                cluster.addKeyspace(keyspaceDefinition, true);
            } else {
                for (ColumnFamilyDefinition columnFamilyDefinition : keyspaceDefinition.getCfDefs()) {
                    if (keyspaceContainsColumnFamily(columnFamilyDefinition, existingKeyspace)) {
                        log.debug(String.format("found column family %s, updating schema", columnFamilyDefinition.getName()));
                        ColumnFamilyDefinition existingColumnFamilyDefinition = getColumnFamilyFromKeyspace(columnFamilyDefinition.getName(), existingKeyspace);
                        existingColumnFamilyDefinition.getColumnMetadata().clear();
                        for (ColumnDefinition columnDefinition : columnFamilyDefinition.getColumnMetadata()) {
                            existingColumnFamilyDefinition.addColumnDefinition(columnDefinition);
                        }
                        cluster.updateColumnFamily(existingColumnFamilyDefinition, true);
                    } else {
                        log.debug(String.format("not found column family %s, adding to schema", columnFamilyDefinition.getName()));
                        cluster.addColumnFamily(columnFamilyDefinition, true);
                    }
                }
            }
        }
    }

    /**
     * Private helper to determine if a column family has already been created in a keyspace
     *
     * @param columnFamilyDefinition the column family definition
     * @param keyspaceDefinition     the keyspace definition
     * @return true if the column family is already present in the keyspace
     */
    private boolean keyspaceContainsColumnFamily(ColumnFamilyDefinition columnFamilyDefinition, KeyspaceDefinition keyspaceDefinition) {
        boolean contains = false;
        for (ColumnFamilyDefinition columnFamilyDefinitionEntry : keyspaceDefinition.getCfDefs()) {
            if (columnFamilyDefinitionEntry.getName().equals(columnFamilyDefinition.getName())) {
                contains = true;
                break;
            }
        }
        return contains;
    }

    /**
     * Private helper to obtain a column family from a keyspace by name
     *
     * @param name               the column family name
     * @param keyspaceDefinition the keyspace
     * @return the column family if found, null otherwise
     */
    private ColumnFamilyDefinition getColumnFamilyFromKeyspace(String name, KeyspaceDefinition keyspaceDefinition) {
        ColumnFamilyDefinition definition = null;
        for (ColumnFamilyDefinition columnFamilyDefinitionEntry : keyspaceDefinition.getCfDefs()) {
            if (columnFamilyDefinitionEntry.getName().equals(name)) {
                definition = columnFamilyDefinitionEntry;
                break;
            }
        }
        return definition;
    }

    /**
     * Loads a lazy property's value
     *
     * @param metadata the entity metadata
     * @param self     the entity instance
     * @param proceed  the method being intercepted
     * @param m
     * @param args     the method arguments
     */
    @Override
    protected <T> void loadLazyPropertyIfNecessary(ClassMetadata<T> metadata, Object self, Method proceed, Method m, Object[] args) throws Exception {
        Object value = proceed.invoke(self, args);
        String key = getKey(self);
        if (key != null) { //key may be null if this is just a regular access to the property before the entity has been persisted and no key has been assigned
            SliceQuery<String, String, Object> query = getSliceQuery(metadata);
            query.setColumnFamily(metadata.getColumnFamily());
            query.setKey(key);
            String column = metadata.getLazyProperty(m);
            query.setColumnNames(column);
            List<HColumn<String, Object>> columns = query.execute().get().getColumns();
            HColumn<String, Object> mappedColumnValue = columns.size() == 1 ? columns.get(0) : null;
            if (mappedColumnValue != null && isEmptyContainerValue(value)) { //todo once a load attempt has been made we should not attempt again but we have no sessions...perhaps a weakreference map?
                Object propertyValue = loadProperty(metadata, column, mappedColumnValue);
                PropertyUtils.setProperty(self, column, propertyValue);
            }
        }
    }

    /**
     * Private helper to get a slice query given a class metadata
     * @param classMetadata the class metadata
     * @param <V> the value type
     * @return a slice query
     */
    private <V> SliceQuery<String, String, V> getSliceQuery(ClassMetadata<?> classMetadata) {
        return HFactory.createSliceQuery(getKeyspace(classMetadata), StringSerializer.get(), StringSerializer.get(), new TypeConverterSerializer<V>());
    }

    /**
     * Gets a keyspace associated with a class metadata
     * @param classMetadata the class metadata
     * @return the keyspace
     */
    private Keyspace getKeyspace(ClassMetadata<?> classMetadata) {
        String keySpace = classMetadata.getKeySpace();
        keySpace = keySpace != null ? keySpace : getDefaultKeySpace();
        Keyspace keyspace = HFactory.createKeyspace(keySpace, cluster, getColumnFamilyConsistencyLevel(classMetadata));
        return keyspace;
    }

    /**
     * Get the default consistency level for a keyspace given the class metadata
     * @param classMetadata the class metadata
     * @return the configured or default consistency level
     */
    private ColumnFamilyConsistencyLevel getColumnFamilyConsistencyLevel(ClassMetadata<?> classMetadata) {
        return new ColumnFamilyConsistencyLevel(classMetadata.getConsistencyLevel());
    }

    /**
     * Private helper to determine if a container is empty
     *
     * @param value the container
     * @return if the container is empty
     */
    private boolean isEmptyContainerValue(Object value) {
        boolean empty = value == null;
        if (!empty) {
            if (Collections.class.isAssignableFrom(value.getClass())) {
                empty = ((Collection) value).isEmpty();
            }
        }
        return empty;
    }

    /**
     * private helper to persist and arbitrary list of objects into the datastore
     *
     * @param entities
     */
    private void persistAll(Object... entities) {
        log.debug(String.format("persist(%s) { ", Arrays.toString(entities)));
        for (Object entity : entities) {
            persistEntity(entity);
        }
        log.debug(String.format("} (%s)", Arrays.toString(entities)));
    }

    /**
     * helper that persists a single an entity in the data store
     *
     * @param entity
     */
    protected void persistEntity(Object entity) {
        fireEntityEvent(Event.Entity.PRE_PERSIST, entity);
        ClassMetadata<?> classMetadata = getClassMetadata(entity.getClass());
        Mutator<String> mutator = getMutator(classMetadata);
        String key = getKey(entity);
        for (String property : classMetadata.getMutationProperties()) {
            persistPropertyIfNecessary(mutator, key, classMetadata, entity, property);
        }
        mutator.execute();
        fireEntityEvent(Event.Entity.POST_PERSIST, entity);
    }

    /**
     * Given a classMetadata obtain a mutator
     * @param classMetadata the class metadata
     * @return the mutator
     */
    private Mutator<String> getMutator(ClassMetadata classMetadata) {
        String keySpace = classMetadata.getKeySpace();
        keySpace = keySpace != null ? keySpace : getDefaultKeySpace();
        Keyspace keyspace = HFactory.createKeyspace(keySpace, cluster, getColumnFamilyConsistencyLevel(classMetadata));
        return HFactory.createMutator(keyspace, StringSerializer.get(), new BatchSizeHint(1, classMetadata.getMutationProperties().size()));
    }

    /**
     * helper that persist an entity property
     *
     * @param mutator       the current operation mutator
     * @param key           the column key
     * @param classMetadata the class metadata
     * @param entity        the entity
     * @param property      the property being persisted
     */
    protected void persistPropertyIfNecessary(Mutator<String> mutator, String key, ClassMetadata<?> classMetadata, Object entity, String property) {
        if (!property.equals(classMetadata.getKeyProperty())) {
            try {
                Object value;
                if (property.equals(CLASS_PROPERTY)) { //if this is a special case class property
                    value = classMetadata.getTarget().getName();
                } else { //otherwise proceed with serialization
                    value = PropertyUtils.getProperty(entity, property);
                    if (classMetadata.isMappedContainer(property)) { //we have a mapped property so we set its class:key as the value
                        value = getMappedPropertyTokenForPersistentValues(value);
                    } else if (classMetadata.isMappedCollection(property)) { //we have a mapped property so we set its key as the value
                        value = getMappedPropertyTokenForPersistentValues(value);
                    }
                }
                update(mutator, key, entity, property, value);
            } catch (NestedNullException nestedNullException) {
                String colFamily = classMetadata.getColumnFamily();
                if (key == null) {
                    log.warn(String.format("entity: %s had no value for key and was ignored", entity));
                } else {
                    fireColumnEvent(Event.Column.PRE_COLUMN_DELETION, entity, property, key, colFamily, null);
                    mutator.addDeletion(key, colFamily, property, StringSerializer.get());
                    log.debug(String.format("\tD:  %s ", property));
                    fireColumnEvent(Event.Column.POST_COLUMN_DELETION, entity, property, key, colFamily, null);
                }
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Invoked when a setter is invoked in any entity changing a property value
     *
     * @param entity   the entity being invoked
     * @param property the property most likely to map to a column name
     * @param value    the new value for the property, null usually handled as deletions of that key column association
     */
    private void update(Mutator<String> mutator, String key, Object entity, String property, Object value) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
        ClassMetadata metadata = getClassMetadata(entity.getClass());
        if (!metadata.getKeyProperty().equals(property)) { //there is no point to create a column for the key
            updateWithMetadata(mutator, key, metadata, property, value, entity);
        }
    }

    /**
     * Private helper that delegates to updateSimpleColumn
     */
    private void updateWithMetadata(Mutator<String> mutator, String key, ClassMetadata metadata, String property, Object value, Object entity) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
        updateSimpleColumn(mutator, key, metadata, property, value, entity);
    }

    /**
     * Private helper that updates a simple column
     *
     * @param mutator  the operation mutator
     * @param key      the column key
     * @param metadata the class metadata associated to the entity
     * @param property the property represented by this column
     * @param value    the value
     * @param entity   the entity the property belongs to
     */
    private void updateSimpleColumn(Mutator<String> mutator, String key, ClassMetadata metadata, String property, Object value, Object entity) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
        String colFamily = metadata.getColumnFamily();
        if (!metadata.isAssociationContainer(property)) { //association container properties are ignored since their embedded properties are flatten into the column family
            if (value == null) {
                if (!metadata.isContainer(property)) { //mapped properties are removed when loading a property with id that returns null
                    fireColumnEvent(Event.Column.PRE_COLUMN_DELETION, entity, property, key, colFamily, null);
                    mutator.addDeletion(key, colFamily, property, StringSerializer.get());
                    log.debug(String.format("\tD: %s", property));
                    fireColumnEvent(Event.Column.POST_COLUMN_DELETION, entity, property, key, colFamily, null);
                }
            } else {
                if (metadata.isCounterIncreaseProperty(property)) { //a counter property increase
                    Long counterIncreaseValue = (Long) value;
                    //if (counterIncreaseValue != 0) {
                    String targetCounterProperty = metadata.getTargetCounterProperty(property);
                    fireColumnEvent(Event.Column.PRE_COUNTER_MUTATION, entity, property, key, colFamily, null);
                    mutator.incrementCounter(key, colFamily, targetCounterProperty, counterIncreaseValue);
                    log.debug(String.format("C: %s increments to %d", targetCounterProperty, counterIncreaseValue));
                    //once applied the increase the counter increase value gets reset to 0
                    PropertyUtils.setProperty(entity, property, 0L);
                    fireColumnEvent(Event.Column.POST_COUNTER_MUTATION, entity, property, key, colFamily, null);
                    //}
                } else if (!metadata.isCounterProperty(property)) {  //a regular column update, counter are ignored since they're just serialized
                    HColumn<String, Object> column = createColumn(property, value, StringSerializer.get(), new TypeConverterSerializer<Object>(value));
                    fireColumnEvent(Event.Column.PRE_COLUMN_MUTATION, entity, property, key, colFamily, column);
                    mutator.addInsertion(key, colFamily, column);
                    log.debug(String.format("\tI: %s : %s ", property, value));
                    fireColumnEvent(Event.Column.POST_COLUMN_MUTATION, entity, property, key, colFamily, column);
                }
            }
            //log.debug(String.format("batched mutation for colFamily: %s, key: %s, col: %s, val: %s", colFamily, key, property, value));
        }
    }

    /**
     * Private helper that hidrates an entity from a list of columns in the datastore
     *
     * @param key              the key
     * @param metadata        the entity metadata
     * @param entityClass     the entity class
     * @param columns         the list of columns to set in the entity proeprties
     * @param instance        the entity instance
     * @param ignoreLazyFlags whether lazy flags in @Column annotations should be ignored for this operation
     * @param <T>             the entity type
     * @return the hidrated entity
     */
    private <T> T serializeColumns(String key, ClassMetadata<?> metadata, Class<T> entityClass, List<HColumn<String, Object>> columns, T instance, boolean ignoreLazyFlags) throws Exception {
        instance = getInstance(entityClass, instance);
        for (HColumn<String, Object> column : columns) {
            String name = column.getName();
            serializeColumn(metadata, instance, name, column, ignoreLazyFlags);
        }
        PropertyUtils.setProperty(instance, metadata.getKeyProperty(), key);
        return instance;
    }

    /**
     * Private helper that serializes a column to an entity property
     *
     * @param metadata        the class metadata
     * @param instance        the entity instance
     * @param name            the property name
     * @param column          the column object
     * @param ignoreLazyFlags whether lazy flags in @Column annotations should be ignored for this operation
     */
    protected void serializeColumn(ClassMetadata<?> metadata, Object instance, String name, HColumn<String, Object> column, boolean ignoreLazyFlags) throws Exception {
        if (!name.equals(CLASS_PROPERTY) && metadata.getSelectionProperties().contains(name)) { //ignore the class type property while deserializing
            if (ignoreLazyFlags || !metadata.isLazyProperty(name)) {
                if ("KEY".equals(name)) {
                    name = metadata.getKeyProperty();
                }
                Object value = loadProperty(metadata, name, column);
                try {
                    instantiateContainersIfNecessary(metadata, instance, name);
                    PropertyUtils.setProperty(instance, name, value);
                } catch (Throwable e) {
                    throw new UnsupportedOperationException(e);
                }
            }
        }
    }

    /* Inner Classes */

    /**
     * Private helper to convert from the data store type to java types and viceversa.
     * It delegates its operations to AbstractPersistenceFactory#convertRead and AbstractPersistenceFactory#convertWrite
     * @param <Type>
     */
    public final class TypeConverterSerializer<Type> extends AbstractSerializer<Type> {
        /**
         * The target class
         */
        private Class<?> target;

        /**
         * Default constructor
         */
        private TypeConverterSerializer() {
        }

        /**
         * Constructor based on a target object
         * @param targetObject the target object
         */
        private TypeConverterSerializer(Type targetObject) {
            this.target = targetObject != null ? targetObject.getClass() : null;
        }

        /**
         * Constructor based on target object class
         * @param target the target object class
         */
        private TypeConverterSerializer(Class<Type> target) {
            this.target = target;
        }

        /**
         * @see AbstractSerializer#toByteBuffer(Object)
         */
        @Override
        public ByteBuffer toByteBuffer(Object obj) {
            try {
                return convertWrite(obj);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * @see AbstractSerializer#fromByteBuffer(java.nio.ByteBuffer)
         */
        @Override
        @SuppressWarnings("unchecked")
        public Type fromByteBuffer(ByteBuffer byteBuffer) {
            try {
                return (Type) convertRead(target, byteBuffer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * A builder for this factory impl
     */
    public static final class Builder {

        private HectorPersistenceFactory delegate;

        public Builder() {
            delegate = new HectorPersistenceFactory();
        }

        public Builder autoDiscoverHosts(boolean autoDiscoverHosts) {
            delegate.setAutoDiscoverHosts(autoDiscoverHosts);
            return this;
        }

        public Builder clusterName(String clusterName) {
            delegate.setClusterName(clusterName);
            return this;
        }

        public Builder credentials(Map<String, String> credentials) {
            delegate.setCredentials(credentials);
            return this;
        }

        public Builder contactNodes(String[] contactNodes) {
            delegate.setContactNodes(contactNodes);
            return this;
        }

        public Builder defaultConsistencyLevel(ConsistencyLevel defaultConsistencyLevel) {
            delegate.setDefaultConsistencyLevel(defaultConsistencyLevel);
            return this;
        }

        public Builder defaultKeySpace(String defaultKeySpace) {
            delegate.setDefaultKeySpace(defaultKeySpace);
            return this;
        }

        public Builder poolName(String poolName) {
            delegate.setPoolName(poolName);
            return this;
        }

        public Builder thriftPort(int thriftPort) {
            delegate.setThriftPort(thriftPort);
            return this;
        }

        public Builder typeConverters(Map<Class<?>, TypeConverter<?>> typeConverters) {
            delegate.setTypeConverters(typeConverters);
            return this;
        }

        public Builder debug(boolean debug) {
            delegate.setDebug(debug);
            return this;
        }

        public Builder deleteIfNull(boolean deleteIfNull) {
            delegate.setDeleteIfNull(deleteIfNull);
            return this;
        }

        public Builder dropOnDestroy(boolean dropOnDestroy) {
            delegate.setDropOnDestroy(dropOnDestroy);
            return this;
        }

        public Builder columnEventListenerMap(Map<Event.Column, List<ColumnEventListener>> columnEventListenerMap) {
            delegate.setColumnEventListenerMap(columnEventListenerMap);
            return this;
        }

        public Builder embeddedServerBaseDir(String embeddedServerBaseDir) {
            delegate.setEmbeddedServerBaseDir(embeddedServerBaseDir);
            return this;
        }

        public Builder entityEventListenerMap(Map<Event.Entity, List<EntityEventListener>> entityEventListenerMap) {
            delegate.setEntityEventListenerMap(entityEventListenerMap);
            return this;
        }

        public Builder replicationFactor(int replicationFactor) {
            delegate.setReplicationFactor(replicationFactor);
            return this;
        }

        public Builder startEmbeddedServer(boolean startEmbeddedServer) {
            delegate.setStartEmbeddedServer(startEmbeddedServer);
            return this;
        }

        public Builder entities(List<Class<?>> entities) {
            delegate.setEntities(entities);
            return this;
        }

        public HectorPersistenceFactory build() throws Exception {
            delegate.init();
            return delegate;
        }

    }
}
