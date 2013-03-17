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

import javassist.NotFoundException;
import me.prettyprint.hector.api.beans.HColumn;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.firebrandocm.dao.annotations.ColumnFamily;
import org.firebrandocm.dao.cql.QueryBuilder;
import org.firebrandocm.dao.events.*;
import org.firebrandocm.dao.impl.*;
import org.firebrandocm.dao.ocmcql.CQLMappedCollectionValueConverter;
import org.firebrandocm.dao.ocmcql.CQLMappedEntityValueConverter;
import org.firebrandocm.dao.utils.ClassUtil;
import org.firebrandocm.dao.utils.ObjectUtils;
import org.firebrandocm.dao.utils.embedded.EmbeddedCassandraServer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;

import static org.firebrandocm.dao.cql.QueryBuilder.*;

/**
 * Abstract factory that may be used by implementations to avoid parsing and writing class metadata and other commons utils
 */
public abstract class AbstractPersistenceFactory implements PersistenceFactory {
    /* Fields */

    /**
     * The separator for values between the class name and the key it belongs to
     */
    protected static final String MAPPED_ENTITY_VALUE_SEPARATOR = ":";

    /**
     * The format for mapped entities
     */
    protected static final String MAPPED_VALUE_FORMAT = "%s" + MAPPED_ENTITY_VALUE_SEPARATOR + "%s";

    /**
     * The separator for collection item ids
     */
    protected static final String COLLECTION_VALUE_SEPARATOR = ",";

    /**
     * This class log
     */
    protected static Log log;

    /**
     * The registered type converters
     */
    protected Map<Class<?>, TypeConverter<?>> typeConverters;

    /**
     * The name / keyspace definitions map
     */
    protected Map<String, KsDef> keyspaceDefinitions = new HashMap<String, KsDef>();

    /**
     * The list of entities supported in this factory
     */
    private List<Class<?>> entities;

    /**
     * The default consistency level
     */
    private ConsistencyLevel defaultConsistencyLevel = ConsistencyLevel.ONE;

    /**
     * whether the keyspace should be dropped on destroy
     */
    private boolean dropOnDestroy;

    /**
     * The default impl used when properties match to interfaces in the Java Collections API
     */
    private Map<Class, Class> defaultInterfaceContainersImpl = new HashMap<Class, Class>() {{
        put(Collection.class, ArrayList.class);
        put(List.class, ArrayList.class);
        put(Set.class, HashSet.class);
        put(Map.class, HashMap.class);
    }};

    /**
     * The port cassandra is running on
     */
    private int thriftPort;

    /**
     * A predefined set of nodes that should be considered for operations
     */
    private String[] contactNodes;

    /**
     * The poll name
     */
    private String poolName;

    /**
     * If deleting when values are null
     */
    private boolean deleteIfNull = true;

    /**
     * If dynamic node discovery should be enabled
     */
    private boolean dynamicNodeDiscovery;

    /**
     * If the test embedded server should be used and started. Useful while unit testing
     */
    private boolean startEmbeddedServer;

    /**
     * If this factory is in debug mode
     */
    private boolean debug;

    /**
     * The path where the embedded server will be started
     */
    private String embeddedServerBaseDir;

    /**
     * If directories for the embedded server should be deleted
     */
    private boolean cleanupDirectories;

    /**
     * The embedded cassandra server
     */
    private EmbeddedCassandraServer cassandraServer;

    /**
     * The entity events / listeners map
     */
    private Map<Event.Entity, List<EntityEventListener>> entityEventListenerMap;

    /**
     * The column events / listeners map
     */
    private Map<Event.Column, List<ColumnEventListener>> columnEventListenerMap;

    /**
     * The replica replacement strategy class
     */
    private Class<? extends AbstractReplicationStrategy> replicaReplacementStrategyClass;

    /**
     * The entity class by names map
     */
    private Map<String, Class<?>> entityClassNameMap = new HashMap<String, Class<?>>();

    /**
     * The default keyspace
     */
    private String defaultKeySpace;

    /**
     * The configuration strategy options
     */
    private Map<String, String> strategyOptions = new HashMap<String, String>();

    /**
     * The classes associated to their metadata
     */
    private Map<Class<?>, ClassMetadata<?>> classMetadataMap = new HashMap<Class<?>, ClassMetadata<?>>();

    /**
     * The replication factor defaults to 1
     */
    private int replicationFactor = 1;

    /* Constructors */

    /**
     * Default constructor
     */
    public AbstractPersistenceFactory() {
        log = LogFactory.getLog(getClass());
    }

    /* Getters & Setters */

    /**
     * @return the list of predefined nodes to contact
     */
    public String[] getContactNodes() {
        return contactNodes;
    }

    /**
     * Sets the contact nodes
     *
     * @param contactNodes the list of predefined nodes to contact
     */
    public void setContactNodes(String[] contactNodes) {
        this.contactNodes = contactNodes;
    }

    /**
     * @return the default consistency level
     */
    public ConsistencyLevel getDefaultConsistencyLevel() {
        return defaultConsistencyLevel;
    }

    /**
     * Sets the default consistency level
     *
     * @param defaultConsistencyLevel the consistency level
     */
    public void setDefaultConsistencyLevel(ConsistencyLevel defaultConsistencyLevel) {
        this.defaultConsistencyLevel = defaultConsistencyLevel;
    }

    /**
     * @return the default keyspace
     */
    public String getDefaultKeySpace() {
        return defaultKeySpace;
    }

    /**
     * sets the default keyspace
     *
     * @param defaultKeySpace the default keyspace
     */
    public void setDefaultKeySpace(String defaultKeySpace) {
        this.defaultKeySpace = defaultKeySpace;
    }

    /**
     * @return the connection pool name
     */
    public String getPoolName() {
        return poolName;
    }

    /**
     * sets the connection pool name
     *
     * @param poolName the pool name
     */
    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    /**
     * @return the connection thrift port
     */
    public int getThriftPort() {
        return thriftPort;
    }

    /**
     * Sets the connection thrift port
     *
     * @param thriftPort
     */
    public void setThriftPort(int thriftPort) {
        this.thriftPort = thriftPort;
    }

    /**
     * gets the registered type converters
     *
     * @return the registered type converters
     */
    public Map<Class<?>, TypeConverter<?>> getTypeConverters() {
        return typeConverters;
    }

    /**
     * The type converters map
     *
     * @param typeConverters the type converters
     */
    public void setTypeConverters(Map<Class<?>, TypeConverter<?>> typeConverters) {
        this.typeConverters = typeConverters;
    }

    /**
     * @return the factory debug mode
     */
    public boolean isDebug() {
        return debug;
    }

    /**
     * @param debug sets the factory debug mode
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * @return if deleting when null
     */
    public boolean isDeleteIfNull() {
        return deleteIfNull;
    }

    /**
     * Sets if deleting when null
     *
     * @param deleteIfNull
     */
    public void setDeleteIfNull(boolean deleteIfNull) {
        this.deleteIfNull = deleteIfNull;
    }

    /**
     * If the factory should drop the keyspace when destroyed
     *
     * @return if the factory should drop the keyspace when destroyed
     */
    public boolean isDropOnDestroy() {
        return dropOnDestroy;
    }

    /**
     * Sets if the factory should drop the keyspace when destroyed
     *
     * @param dropOnDestroy if the factory should drop the keyspace when destroyed
     */
    public void setDropOnDestroy(boolean dropOnDestroy) {
        this.dropOnDestroy = dropOnDestroy;
    }

    /**
     * Sets if directories for the embedded server should be deleted
     *
     * @param cleanupDirectories
     */
    public void setCleanupDirectories(boolean cleanupDirectories) {
        this.cleanupDirectories = cleanupDirectories;
    }

    /**
     * The entity events / listeners map
     * All listeners subscribed to a given event will be automatically notified
     *
     * @param columnEventListenerMap The events / listeners map
     */
    public void setColumnEventListenerMap(Map<Event.Column, List<ColumnEventListener>> columnEventListenerMap) {
        this.columnEventListenerMap = columnEventListenerMap;
    }

    /**
     * Sets if dynamic node discovery should be enabled
     *
     * @param dynamicNodeDiscovery
     */
    public void setDynamicNodeDiscovery(boolean dynamicNodeDiscovery) {
        this.dynamicNodeDiscovery = dynamicNodeDiscovery;
    }

    /**
     * Sets The path where the embedded server will be started
     *
     * @param embeddedServerBaseDir The path where the embedded server will be started
     */
    public void setEmbeddedServerBaseDir(String embeddedServerBaseDir) {
        this.embeddedServerBaseDir = embeddedServerBaseDir;
    }

    /**
     * Sets the factory list of entities
     *
     * @param entities
     */
    public void setEntities(List<Class<?>> entities) {
        this.entities = entities;
    }

    /**
     * Sets the factory entities base package
     *
     * @param entitiesPkg
     */
    public void setEntitiesPkg(String entitiesPkg) throws IOException, ClassNotFoundException {
      setEntities(ClassUtil.get(entitiesPkg, ColumnFamily.class));
    }

    /**
     * The entity events / listeners map
     * All listeners subscribed to a given event will be automatically notified
     *
     * @param entityEventListenerMap The events / listeners map
     */
    public void setEntityEventListenerMap(Map<Event.Entity, List<EntityEventListener>> entityEventListenerMap) {
        this.entityEventListenerMap = entityEventListenerMap;
    }

    /**
     * Sets The replica replacement strategy class
     *
     * @param replicaReplacementStrategyClass
     *         The replica replacement strategy class
     */
    public void setReplicaReplacementStrategyClass(Class<? extends AbstractReplicationStrategy> replicaReplacementStrategyClass) {
        this.replicaReplacementStrategyClass = replicaReplacementStrategyClass;
    }

    /**
     * Sets the replication factor
     *
     * @param replicationFactor the replication factor
     */
    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    /**
     * sets whether to start the embedded server along with the factory initialization
     *
     * @param startEmbeddedServer the embedded server
     */
    public void setStartEmbeddedServer(boolean startEmbeddedServer) {
        this.startEmbeddedServer = startEmbeddedServer;
    }

    /* Interface Implementations */


// --------------------- Interface PersistenceFactory ---------------------

    /**
     * The list of managed class by this factory
     *
     * @return The list of managed class by this factory
     */
    public List<Class<?>> getManagedClasses() {
        return entities;
    }

    /* Misc */

    /**
     * Allows external callers to contribute or override the list of default impl for the collection containers
     *
     * @param interfaceClass the interface class
     * @param implClass      the impl class
     */
    public void addDefaultImplForContainer(Class interfaceClass, Class implClass) {
        synchronized (defaultInterfaceContainersImpl) {
            defaultInterfaceContainersImpl.put(interfaceClass, implClass);
        }
    }

    /**
     * Converts an object value to a ByteBuffer
     *
     * @param value the object value
     * @return the ByteBuffer
     */
    @SuppressWarnings("unchecked")
    public ByteBuffer convertWrite(Object value) throws Exception {
        ByteBuffer byteBuffer = null;
        if (value != null) {
            Class<?> valueClass = value.getClass();
            TypeConverter<Object> converter = (TypeConverter<Object>) getTypeConverter(valueClass);
            deffenseConverter(converter, value.getClass());
            byteBuffer = converter.toValue(value);
        }
        return byteBuffer;
    }

    /**
     * Gets a type converter for a given type
     *
     * @param type the type
     * @return the type converter
     */
    public TypeConverter<?> getTypeConverter(Class<?> type) {
        TypeConverter<?> converter = null;
        for (Map.Entry<Class<?>, TypeConverter<?>> classTypeConverterEntry : typeConverters.entrySet()) {
            if (classTypeConverterEntry.getKey().isAssignableFrom(type)) {
                converter = classTypeConverterEntry.getValue();
                break;
            }
        }
        return converter;
    }

    /**
     * Private helper
     * Ensure there is a converter configured for this type otherwise the type is unsupported
     *
     * @param converter the converter
     * @param type      the class
     */
    private void deffenseConverter(TypeConverter<Object> converter, Class<?> type) {
        if (type != null) {
            if (converter == null) {
                throw new IllegalStateException(String.format("missing converter for type: %s", type));
            }
        }
    }

    /**
     * Creates and assigns set of unique keys
     *
     * @param entities the object entities
     */
    protected void createKeysIfNeeded(Object... entities) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        for (Object entity : entities) {
            String key = getKey(entity);
            if (key == null) {
                key = ObjectUtils.newTimeUuid().toString();
            }
            ClassMetadata<?> metadata = getClassMetadata(entity.getClass());
            PropertyUtils.setProperty(entity, metadata.getKeyProperty(), key);
            for (String mappedProperty : metadata.getMappedProperties()) {
                Object mappedEntity = PropertyUtils.getProperty(entity, mappedProperty);
                if (mappedEntity != null) {
                    if (Collection.class.isAssignableFrom(mappedEntity.getClass())) {
                        Collection<?> nestedEntities = (Collection) mappedEntity;
                        createKeysIfNeeded(nestedEntities.toArray());
                    } else {
                        createKeysIfNeeded(mappedEntity);
                    }
                }
            }
        }
    }

    /**
     * Obtains an entity key
     *
     * @param entity the entity
     * @return the key
     */
    public String getKey(Object entity) {
        ClassMetadata metadata = getClassMetadata(entity.getClass());
        String keyProperty = metadata.getKeyProperty();
        if (keyProperty == null) {
            throw new IllegalStateException(String.format("no key defined for %s", entity.getClass()));
        }
        try {
            return (String) PropertyUtils.getProperty(entity, keyProperty);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the class metadata map
     *
     * @param entityClass
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> ClassMetadata<T> getClassMetadata(Class<T> entityClass) {
        return (ClassMetadata<T>) classMetadataMap.get(ObjectUtils.getRealClass(entityClass));
    }

    /**
     * Destroys this factory instance releasing any resources
     */
    public void destroy() {
        try {
            for (ClassMetadata<?> classMetadata : classMetadataMap.values()) {
                classMetadata.destroy();
            }
            cassandraServer.stop();
        } catch (Exception e) {
            log.error(e);
        }
    }

    /**
     * notifies column event listeners
     *
     * @param event        the event
     * @param entity       the entity
     * @param property     the property
     * @param key          the key
     * @param columnFamily the column family
     * @param column       the column object
     */
    protected void fireColumnEvent(Event.Column event, Object entity, String property, String key, String columnFamily, Object column) {
        //log.debug(String.format("fireColumnEvent %s, %s, %s, %s, %s, %s", event, entity, property, key, columnFamily, column));
        if (columnEventListenerMap != null) {
            List<ColumnEventListener> listeners = columnEventListenerMap.get(event);
            if (listeners != null) {
                ColumnEvent columnEvent = new ColumnEvent(this, event, entity, property, key, columnFamily, column);
                for (ColumnEventListener listener : listeners) {
                    if (listener != null) {
                        listener.onEvent(columnEvent);
                    }
                }
            }
        }
    }

    /**
     * notifies entity event listeners
     *
     * @param event    the event
     * @param entities the affected entities
     */
    protected void fireEntityEvent(Event.Entity event, Object... entities) {
        //log.debug(String.format("fireEntityEvent %s, %s", event, Arrays.toString(entities)));
        // call any entities internal listeners
        if (entities != null) {
            for (Object entity : entities) {
                if (entity != null) {
                    ClassMetadata<?> metadata = getClassMetadata(entity.getClass());
                    if (metadata != null) {
                        Set<Method> listeners = metadata.getListenersForEvent(event);
                        if (listeners != null) {
                            for (Method listener : listeners) {
                                try {
                                    listener.invoke(entity);
                                } catch (IllegalAccessException e) {
                                    throw new RuntimeException(e);
                                } catch (InvocationTargetException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                }
            }
        }

        //call any registered listeners
        if (entityEventListenerMap != null) {
            List<EntityEventListener> listeners = entityEventListenerMap.get(event);
            if (listeners != null) {
                EntityEvent entityEvent = new EntityEvent(this, event, entities);
                for (EntityEventListener listener : listeners) {
                    if (listener != null) {
                        listener.onEvent(entityEvent);
                    }
                }
            }
        }
    }

    /**
     * Retrieves an entity class by name
     *
     * @param entityName the entity name
     * @return the found class if any
     */
    public Class<?> getEntityClassByName(String entityName) {
        return entityClassNameMap.get(entityName);
    }

    /**
     * Gets a initialized class instance or returns the same instance if not null
     *
     * @param entityClass the entity class
     * @param instance    the instance
     * @param <T>         the entity type
     * @return the instance
     */
    protected <T> T getInstance(Class<T> entityClass, T instance) {
        if (instance == null) {
            try {
                instance = getInstance(entityClass);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        return instance;
    }

    /**
     * Gets the mapped property token from a persistent value
     *
     * @param value the value
     * @return the mapped property token
     */
    public String getMappedPropertyTokenForPersistentValues(Object value) {
        return getMappedPropertyToken(value, false);
    }

    /**
     * Private Helper.
     * Gets the mapped property token from a persistent value optionally persisting the property value
     *
     * @param value   the value
     * @param persist whether to persist the property value or not
     * @return the mapped property token
     */
    private String getMappedPropertyToken(Object value, boolean persist) {
        String mappedValue = null;
        if (value != null) {
            if (Collection.class.isAssignableFrom(value.getClass())) {
                Collection mappedEntities = (Collection) value;
                List<Object> values = new ArrayList<Object>(mappedEntities.size());
                for (Object mappedEntity : mappedEntities) {
                    if (persist) {
                        persist(mappedEntity);
                    }
                    values.add(getMappedPropertyToken(mappedEntity, persist));
                }
                mappedValue = StringUtils.join(values, COLLECTION_VALUE_SEPARATOR);
            } else {
                if (persist) {
                    persist(value);
                }
                mappedValue = getTokenValueForMappedEntity(value);
            }
        }
        return mappedValue;
    }

    /**
     * Gets a token value from a mapped entity
     *
     * @param value the mapped entity
     * @return the token value (id)
     */
    protected String getTokenValueForMappedEntity(Object value) {
        return String.format(MAPPED_VALUE_FORMAT, ObjectUtils.getRealClass(value.getClass()).getName(), getKey(value));
    }

    /**
     * Initializes the factory.
     * Should be invoked by subclasses
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public void init() throws Exception {
        CQLMappedEntityValueConverter mappedEntityConverter = new CQLMappedEntityValueConverter(this);
        QueryBuilder.addConverter(0, mappedEntityConverter);
        QueryBuilder.addConverter(1, new CQLMappedCollectionValueConverter(mappedEntityConverter));
        for (Class<?> entityClass : entities) {
            ClassMetadata metadata = new ClassMetadata(entityClass, this);
            classMetadataMap.put(entityClass, metadata);
        }
        if (startEmbeddedServer) {
            log.warn("starting dev embedded server");
            if (cassandraServer == null) {
                cassandraServer = new EmbeddedCassandraServer(embeddedServerBaseDir);
                cassandraServer.setCleanupDirectories(cleanupDirectories);
                cassandraServer.start();

                // wait until cassandra server starts up. could wait less time, but
                // 2 seconds to be sure.
                Thread.sleep(2000);
            }
        }
        ObjectUtils.notNull(getDefaultKeySpace(), "no keyspace provided");
    }

    protected void initializeKeyspaceDefinitions() throws NoSuchFieldException, InstantiationException, IllegalAccessException, ClassNotFoundException, NotFoundException {
        for (Map.Entry<Class<?>, ClassMetadata<?>> entry : classMetadataMap.entrySet()) {
            ClassMetadata classMetadata = entry.getValue();
            String keysPace = classMetadata.getKeySpace();
            KsDef ksDef = keyspaceDefinitions.get(keysPace);
            if (ksDef == null) {
                ksDef = new KsDef();
                ksDef.setName(keysPace);
                if (replicaReplacementStrategyClass == null) {
                    strategyOptions.put("replication_factor", String.valueOf(replicationFactor));
                    replicaReplacementStrategyClass = SimpleStrategy.class;
                }
                ksDef.setStrategy_class(replicaReplacementStrategyClass.getName());
                ksDef.setStrategy_options(strategyOptions);
                ksDef.setCf_defs(new ArrayList<CfDef>());
                keyspaceDefinitions.put(keysPace, ksDef);
            }
            List<CfDef> columnFamilyDefinitions = ksDef.getCf_defs();
            columnFamilyDefinitions.add(classMetadata.getColumnFamilyDefinition());
        }
    }

    /**
     * Initializes the type converters
     */
    protected void initializeTypeConverters() {
        if (typeConverters == null) {
            typeConverters = new LinkedHashMap<Class<?>, TypeConverter<?>>();
            typeConverters.put(String.class, new StringTypeConverter());
            typeConverters.put(Boolean.class, new BooleanTypeConverter());
            typeConverters.put(Long.class, new LongTypeConverter());
            typeConverters.put(Double.class, new DoubleTypeConverter());
            typeConverters.put(Integer.class, new IntegerTypeConverter());
            typeConverters.put(Date.class, new DateTypeConverter());
            typeConverters.put(boolean.class, new BooleanTypeConverter());
            typeConverters.put(long.class, new LongTypeConverter());
            typeConverters.put(double.class, new DoubleTypeConverter());
            typeConverters.put(int.class, new IntegerTypeConverter());
            typeConverters.put(byte[].class, new ByteArrayTypeConverter());
            typeConverters.put(Byte[].class, new ByteArrayTypeConverter());
            typeConverters.put(Object.class, new ObjectBytesTypeConverter());
        }
    }

    /**
     * Instantiates null containers if an association is gonna take place on a mapped property
     *
     * @param metadata the class metadata for the affected entity
     * @param instance the entity
     * @param name     the mapped property
     * @param <T>      the type of entity
     */
    protected <T> void instantiateContainersIfNecessary(ClassMetadata<?> metadata, T instance, String name) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, ClassNotFoundException {
        Object target = instance;
        String path = "";
        String[] properties = name.split("\\.");
        for (String property : properties) {
            path = StringUtils.isNotBlank(path) ? path + "." + property : property;
            if (metadata.isContainer(path)) {
                Object value = PropertyUtils.getProperty(target, property);
                if (value == null) {
                    Class<?> typeClass = metadata.getPropertiesTypesMap().get(path);
                    value = getInstance(typeClass);
                    PropertyUtils.setProperty(target, property, value);
                }
                target = value;
            }
        }
    }

    /**
     * @param entityClass the class type for this instance
     * @param <T>         the type of class to be returned
     * @return an instance of this type after transformation of its accessors to notify the persistence context that there are ongoing changes
     */
    @SuppressWarnings("unchecked")
    public <T> T getInstance(Class<T> entityClass) {
        final ClassMetadata<T> metadata = getClassMetadata(entityClass);
        T instance;
        try {
            if (metadata != null) { //create a proxy that can support lazy loading and interception of certain methods
                instance = metadata.createProxy();
            } else { //an embedded entity with no metadata
                if (entityClass.isInterface()) {
                    Class implClass = defaultInterfaceContainersImpl.get(entityClass);
                    if (implClass == null) {
                        throw new IllegalStateException(String.format("no default impl found for %s", entityClass));
                    }
                    instance = (T) implClass.newInstance();
                } else {
                    instance = entityClass.newInstance();
                }
            }
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return instance;
    }

    /**
     * To be implemented by subclasses request that a lazy property gets loaded
     *
     * @param metadata the class metadata
     * @param self     the object in which the propertyload is getting performed
     * @param proceed  the method containing the value
     * @param m
     * @param args     the method arguments
     * @param <T>      the entity type
     */
    protected abstract <T> void loadLazyPropertyIfNecessary(ClassMetadata<T> metadata, Object self, Method proceed, Method m, Object[] args) throws Exception, IllegalAccessException, ClassNotFoundException, NoSuchMethodException;

    /**
     * Loads a mapped entity out of a column value
     *
     * @param columnValue the column value
     * @return the loaded mapped entity
     */
    protected Object loadMappedEntity(String columnValue) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        if (!columnValue.contains(MAPPED_ENTITY_VALUE_SEPARATOR))
            throw new IllegalStateException(String.format("%s does not contain the appropriate tokens or separators", columnValue));
        String[] parts = columnValue.split(MAPPED_ENTITY_VALUE_SEPARATOR);
        Class<?> mappedEntityClass = Class.forName(parts[0]);
        String mappedEntityKey = parts[1];
        return get(mappedEntityClass, mappedEntityKey);
    }

    /**
     * Loads a simple, mapped or collection entity property
     *
     * @param metadata the entity property
     * @param name     the property name
     * @param column   the column object
     * @return the property value
     */
    protected Object loadProperty(ClassMetadata<?> metadata, String name, HColumn<String, Object> column) throws Exception {
        Object value;
        if (metadata.isMappedContainer(name)) {
            value = loadMappedEntity(column);
        } else if (metadata.isMappedCollection(name)) {
            value = loadMappedCollection(column);
        } else {
            value = convertRead(metadata.getColumnClass(name), column.getValueBytes());
        }
        return value;
    }

    /**
     * Loads a mapped entity
     *
     * @param column the column object
     * @return the mapped entity
     */
    protected Object loadMappedEntity(HColumn<String, Object> column) throws Exception {
        Object value = convertRead(String.class, column.getValueBytes());
        return loadMappedEntity(value.toString());
    }

    /**
     * Loads a mapped collection out of a column
     *
     * @param column the column
     * @return the loaded mapped collection
     */
    protected Object loadMappedCollection(HColumn<String, Object> column) throws Exception {
        Object value = convertRead(String.class, column.getValueBytes());
        Object retVal = null;
        String[] tokens = value.toString().split(COLLECTION_VALUE_SEPARATOR);
        Map<Class<?>, List<String>> loadBatch = new HashMap<Class<?>, List<String>>();
        if (tokens != null) {
            List<Object> entities = new ArrayList<Object>(tokens.length);
            for (String tokenValue : tokens) {
                String[] parts = tokenValue.split(MAPPED_ENTITY_VALUE_SEPARATOR);
                Class<?> mappedEntityClass = Class.forName(parts[0]);
                String mappedEntityKey = parts[1];
                List<String> collectedIds = loadBatch.get(mappedEntityClass);
                if (collectedIds == null) {
                    collectedIds = new ArrayList<String>();
                    loadBatch.put(mappedEntityClass, collectedIds);
                }
                collectedIds.add(mappedEntityKey);
            }
            for (Map.Entry<Class<?>, List<String>> entry : loadBatch.entrySet()) {
                List<?> batchedEntities = getResultList(entry.getKey(), Query.get(select(allColumns(), from(entry.getKey()), where(keyIn(entry.getValue().toArray(new String[entry.getValue().size()]))))));
                entities.addAll(batchedEntities);
            }
            retVal = entities;
        }
        return retVal;
    }

    /**
     * Converts ByteBuffer to an object value
     *
     * @param value the object value
     * @return the ByteBuffer
     */
    @SuppressWarnings("unchecked")
    protected Object convertRead(Class<?> type, ByteBuffer value) throws Exception {
        TypeConverter<Object> converter = (TypeConverter<Object>) getTypeConverter(type);
        deffenseConverter(converter, type);
        return converter.fromValue(value);
    }
}
