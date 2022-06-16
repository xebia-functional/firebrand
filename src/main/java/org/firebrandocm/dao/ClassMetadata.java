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

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import me.prettyprint.cassandra.serializers.StringSerializer;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.firebrandocm.dao.annotations.*;
import org.firebrandocm.dao.events.Event;
import org.firebrandocm.dao.utils.ClassUtil;
import org.firebrandocm.dao.utils.ObjectUtils;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Holds metadata information about a class, initialized at startup time and reused
 */
public class ClassMetadata<T> {
    /* Fields */

    private static final String CLASS_PROPERTY = "class";

    private static Map<String, String> namedQueries = new HashMap<String, String>();

    private Log log = LogFactory.getLog(getClass());

    /**
     * the keyspace this entity belongs to
     */
    private String keySpace;

    /**
     * a column family for this entity
     */
    private String columnFamily;

    /**
     * the entity's class
     */
    private Class<T> target;

    /**
     * the set of all class properties
     */
    private Set<String> mutationProperties = new HashSet<String>();

    /**
     * the set of all readable properties
     */
    private Set<String> selectionProperties = new HashSet<String>();

    /**
     * the set of all persistent properties that declared themselves as being part of secondary indexes
     */
    private Set<String> indexedProperties = new HashSet<String>();

    /**
     * the property that holds the key in this class
     */
    private String keyProperty;

    /**
     * a map from columns to types
     */
    private Map<String, Class<?>> propertiesTypesMap = new HashMap<String, Class<?>>();

    /**
     * set of properties that map to embedded entities
     */
    private Set<String> embeddedEntities = new HashSet<String>();

    /**
     * set of properties that map to mappedEntities
     */
    private Set<String> mappedEntities = new HashSet<String>();

    /**
     * set of properties that map to element collections
     */
    private Set<String> elementCollections = new HashSet<String>();

    /**
     * the column family definition
     */
    private CfDef columnFamilyDefinition;

    /**
     * set of properties that map to counter properties
     */
    private Set<String> counterProperties = new HashSet<String>();

    /**
     * set of properties that map to properties that increase counter values
     */
    private Map<String, String> counterPropertiesIncrease = new HashMap<String, String>();

    /**
     * set of properties that map to mapped properties
     */
    private Set<String> mappedProperties = new HashSet<String>();

    /**
     * set of properties that map to mapped collections
     */
    private Set<String> mappedCollections = new HashSet<String>();

    /**
     * map of methods and their properties that are lazy accessors
     */
    private Map<Method, String> lazyAccesors = new HashMap<Method, String>();

    /**
     * set of properties that map to properties that should be loaded on demand when their read method is invoked
     */
    private Set<String> lazyProperties = new HashSet<String>();

    /**
     * a proxy method handler
     */
    private MethodHandler proxyMethodHandler;

    /**
     * the proxy class
     */
    private Class<?> proxyClass;

    /**
     * true if this metadata object is associated with a class that represents a Counter style column family
     */
    private boolean counterColumnFamily;

    /**
     * map of properties and the container classes associated
     */
    private Map<String, Class<?>> propertyContainerMap = new HashMap<String, Class<?>>();

    /**
     * map of events and the methods that act as listeners of those events
     */
    private Map<Event.Entity, Set<Method>> entityEventListenersMap = new TreeMap<Event.Entity, Set<Method>>();

    /**
     * the keyspace consistency level
     */
    private ConsistencyLevel consistencyLevel;

    /* Static Methods */

    /**
     * Gets a named query by its name
     *
     * @param name the query name
     * @return the query value
     */
    public static String getNamedQuery(String name) {
        String query = getNullSafeNamedQuery(name);
        if (query == null)
            throw new IllegalArgumentException(String.format("named query not found for name: %s", name));
        return query;
    }

    /**
     * Gets a name query by its name returning null if not found
     *
     * @param name the query name
     * @return the query value
     */
    public static String getNullSafeNamedQuery(String name) {
        return namedQueries.get(name);
    }

    /* Constructors */

    /**
     * Constructor.
     * Extracts and caches metadata for persistent entity classes
     *
     * @param target             the target class
     * @param persistenceFactory the persistence factory managing the entity
     */
    public ClassMetadata(Class<T> target, AbstractPersistenceFactory persistenceFactory) throws ClassNotFoundException, IntrospectionException, InstantiationException, IllegalAccessException {
        log.debug(String.format("Initializing class metadata for %s", target));
        this.target = target;
        //If this is a top level structure that holds a column family
        if (target.isAnnotationPresent(ColumnFamily.class)) {
            ColumnFamily columnFamilyAnnotation = target.getAnnotation(ColumnFamily.class);
            consistencyLevel = columnFamilyAnnotation.consistencyLevel();
            counterColumnFamily = columnFamilyAnnotation.defaultValidationClass() == CounterColumnType.class;
            keySpace = StringUtils.defaultIfEmpty(columnFamilyAnnotation.keySpace(), persistenceFactory.getDefaultKeySpace());
            columnFamily = ClassUtil.getColumnFamilyName(target);
            initializeColumnFamilyDefinition();
            processFields(target, "");
            processMethods(target);
            addClassTypePropertyIfSupported();
            initializeProxyFactory(persistenceFactory);
            initializeNamedQueries(target);
        } else {
            throw new IllegalArgumentException(target + " is not annotated with " + ColumnFamily.class);
        }
    }

    /**
     * Private helper to initialize a column family definition
     */
    protected void initializeColumnFamilyDefinition() throws ClassNotFoundException {
        columnFamilyDefinition = new CfDef();
        columnFamilyDefinition.setName(columnFamily);
        columnFamilyDefinition.setKeyspace(getKeySpace());
        ColumnFamily cfAnnotation = target.getAnnotation(ColumnFamily.class);
        String comparatorType = cfAnnotation.compareWith().getName();
        if (cfAnnotation.reversed()) {
            comparatorType = String.format("%s(reversed=true)", comparatorType);
        }
        columnFamilyDefinition.setComparator_type(comparatorType);
        columnFamilyDefinition.setKey_cache_size(cfAnnotation.keysCached());
        columnFamilyDefinition.setRow_cache_size(cfAnnotation.rowsCached());
        columnFamilyDefinition.setComment(StringUtils.defaultIfEmpty(cfAnnotation.comment(), null));
        columnFamilyDefinition.setComparator_type(cfAnnotation.compareWith().getName());
        columnFamilyDefinition.setRead_repair_chance(cfAnnotation.readRepairChance());
        columnFamilyDefinition.setGc_grace_seconds(cfAnnotation.gcGraceSeconds());
        columnFamilyDefinition.setDefault_validation_class(StringUtils.defaultIfEmpty(cfAnnotation.defaultValidationClass().getName(), null));
        columnFamilyDefinition.setKey_validation_class(StringUtils.defaultIfEmpty(cfAnnotation.defaultKeyValidationClass().getName(), null));
        columnFamilyDefinition.setMin_compaction_threshold(cfAnnotation.minCompactionThreshold());
        columnFamilyDefinition.setMax_compaction_threshold(cfAnnotation.maxCompactionThreshold());
        columnFamilyDefinition.setReplicate_on_write(cfAnnotation.replicateOnWrite());
    }

    /**
     * Processes the target fields searching for persistent annotations
     *
     * @param target the target class
     * @param prefix a potential prefix utilized for deep nested properties
     */
    protected void processFields(Class<?> target, String prefix) throws IntrospectionException, ClassNotFoundException {
        if (StringUtils.isNotBlank(prefix)) {
            prefix += ".";
        }
        for (Field field : ObjectUtils.getAllFieldsInHierarchy(target)) {
            processElement(field, prefix + field.getName(), field.getType());
        }
    }

    /**
     * Processes a field searching for persistent annotations
     *
     * @param element      the annotated element
     * @param propertyName the element name
     * @param type         the property type
     */
    protected void processElement(Field element, String propertyName, Class<?> type) throws ClassNotFoundException, IntrospectionException {
        if (valid(element, propertyName, type)) {
            if (element.isAnnotationPresent(Embedded.class)) {
                processEmbeddedEntity(type, propertyName);
            } else if (element.isAnnotationPresent(Mapped.class)) {
                processMappedEntity(type, element, propertyName);
            } else if (element.isAnnotationPresent(MappedCollection.class)) {
                processMappedCollection(type, element, propertyName);
            } else if (element.isAnnotationPresent(CounterIncrease.class)) {
                processCounterIncrease(type, element, propertyName, element.getAnnotation(CounterIncrease.class).value());
            } else {
                if (element.isAnnotationPresent(Key.class)) {
                    keyProperty = propertyName;
                }
                processSimpleColumn(element, propertyName);
            }
        }
    }

    /**
     * Private helper.
     * Determines whether a field is valid for processing and persistence consideration
     *
     * @param element the field element
     * @param name    the name
     * @param type    the field type
     * @return if the field should be considered for persistence
     */
    protected boolean valid(Field element, String name, Class<?> type) {
        return !name.equals(CLASS_PROPERTY)
                && !(element.isAnnotationPresent(Transient.class))
                && !name.equals(keyProperty)
                && !type.getName().equals(PersistenceFactory.class.getName())
                && !Modifier.isStatic(element.getModifiers());
    }

    /**
     * Processes metadata for a nested embedded association. Helper method
     *
     * @param type         the type
     * @param propertyName the property name
     */
    protected void processEmbeddedEntity(Class<?> type, String propertyName) throws ClassNotFoundException, IntrospectionException {
        embeddedEntities.add(propertyName);
        propertiesTypesMap.put(propertyName, type);
        mutationProperties.add(propertyName);
        propertyContainerMap.put(propertyName, type);
        log.debug(String.format("added type %s and property %s", type.getName(), propertyName));
        processFields(type, propertyName);
    }

    /**
     * Processes metadata for a nested mapped association. Helper method
     *
     * @param type         the type
     * @param element
     * @param propertyName the property name
     */
    protected void processMappedEntity(Class<?> type, Field element, String propertyName) throws ClassNotFoundException, IntrospectionException {
        Mapped mapped = element.getAnnotation(Mapped.class);
        mappedEntities.add(propertyName);
        propertiesTypesMap.put(propertyName, type);
        mutationProperties.add(propertyName);
        propertyContainerMap.put(propertyName, type);
        mappedProperties.add(propertyName);
        boolean lazy = mapped != null && mapped.lazy();
        addProperty(null, propertyName, element.getType(), true, lazy, false, false);
        log.debug(String.format("added mapped type %s and property %s", type.getName(), propertyName));
    }

    /**
     * Protected helper that caches information for a property for further persistence consideration
     *
     * @param colAnnotation   the column annotation
     * @param propertyName    the property name
     * @param type            the property type
     * @param indexed         whether the property should be indexed in the data store
     * @param lazy            if access to this property should be loaded on demand
     * @param counter         if this property represents a counter
     * @param counterIncrease if this property represents a value for a counter arithmetic operation
     */
    protected void addProperty(Column colAnnotation, String propertyName, Class<?> type, boolean indexed, boolean lazy, boolean counter, boolean counterIncrease) throws ClassNotFoundException, IntrospectionException {
        propertiesTypesMap.put(propertyName, type);
        mutationProperties.add(propertyName);
        if (indexed) {
            indexedProperties.add(propertyName);
            log.debug(String.format("added indexed property %s", propertyName));
        }
        if (lazy) {
            PropertyDescriptor descriptor = new PropertyDescriptor(propertyName, target);
            lazyAccesors.put(descriptor.getReadMethod(), propertyName);
            lazyProperties.add(propertyName);
        }
        if (counter) {
            counterProperties.add(propertyName);
        }
        if (!counterIncrease) {
            selectionProperties.add(propertyName);
            addColumnToColumnFamilyDefinition(colAnnotation, propertyName, indexed);
        }
        log.debug(String.format("added property %s", propertyName));
    }

    /**
     * Private helper that adds a c olumn to a column family definition
     *
     * @param colAnnotation the column annotation
     * @param property      the property
     * @param indexed       if this property should be indexed in the datastore
     */
    private void addColumnToColumnFamilyDefinition(Column colAnnotation, String property, boolean indexed) throws ClassNotFoundException {
        if (!property.equals(keyProperty)) {
            boolean defaults = colAnnotation == null;
            ColumnDef columnDef = new ColumnDef();
            columnDef.setName(StringSerializer.get().toByteBuffer(property));
            columnDef.setValidation_class(defaults ? org.firebrandocm.dao.annotations.Column.DEFAULTS.VALIDATION_CLASS.getName() : colAnnotation.validationClass().getName());
            indexed = indexed || isMappedContainer(property);
            if (!indexed) {
                indexed = defaults ? org.firebrandocm.dao.annotations.Column.DEFAULTS.INDEXED : colAnnotation.indexed();
            }
            if (indexed) {
                columnDef.setIndex_name(String.format("%s_%s_%s", keySpace, columnFamily, property));
                columnDef.setIndex_type(defaults ? org.firebrandocm.dao.annotations.Column.DEFAULTS.INDEX_TYPE : colAnnotation.indexType());
            }
            columnFamilyDefinition.addToColumn_metadata(columnDef);
        }
    }

    /**
     * Whether this property should be considered as a mapped container which data belongs in some other row
     *
     * @param property the property
     * @return if this property is a mapped container
     */
    public boolean isMappedContainer(String property) {
        return mappedEntities.contains(property);
    }

    /**
     * Processes metadata for a nested mapped collection. Helper method
     *
     * @param type
     * @param element
     * @param propertyName
     * @throws ClassNotFoundException
     * @throws IntrospectionException
     */
    private void processMappedCollection(Class<?> type, Field element, String propertyName) throws ClassNotFoundException, IntrospectionException {
        MappedCollection mappedCollection = element.getAnnotation(MappedCollection.class);
        mappedCollections.add(propertyName);
        propertiesTypesMap.put(propertyName, type);
        mutationProperties.add(propertyName);
        propertyContainerMap.put(propertyName, type);
        mappedProperties.add(propertyName);
        boolean lazy = mappedCollection != null && mappedCollection.lazy();
        addProperty(null, propertyName, element.getType(), true, lazy, false, false);
        log.debug(String.format("added mapped type %s and property %s", type.getName(), propertyName));
    }

    /**
     * Processes metadata for counter increase property
     *
     * @param type
     * @param element
     * @param propertyName
     * @throws ClassNotFoundException
     * @throws IntrospectionException
     */
    private void processCounterIncrease(Class<?> type, Field element, String propertyName, String targetCounter) throws ClassNotFoundException, IntrospectionException {
        counterPropertiesIncrease.put(propertyName, targetCounter);
        addProperty(null, propertyName, element.getType(), true, false, false, true);
        log.debug(String.format("added processCounterIncrease type %s and property %s", type.getName(), propertyName));
    }

    /**
     * Processes metadata for a simple column. Helper method
     *
     * @param element      the type
     * @param propertyName the property name
     */
    protected void processSimpleColumn(Field element, String propertyName) throws ClassNotFoundException, IntrospectionException {
        propertiesTypesMap.put(propertyName, element.getType());
        mutationProperties.add(propertyName);
        propertyContainerMap.put(propertyName, element.getDeclaringClass());
        boolean indexed = false;
        if (element.isAnnotationPresent(Column.class)) {
            Column columnAnnotation = element.getAnnotation(Column.class);
            indexed = columnAnnotation != null && columnAnnotation.indexed();
        }
        org.firebrandocm.dao.annotations.Column colAnnotation = element.getAnnotation(org.firebrandocm.dao.annotations.Column.class);
        boolean lazy = colAnnotation != null && colAnnotation.lazy();
        boolean counter = colAnnotation != null && colAnnotation.counter();
        addProperty(colAnnotation, propertyName, element.getType(), indexed, lazy, counter, false);
        log.debug(String.format("added property %s", propertyName));
    }

    /**
     * Private Helper.
     * Processes all methods in hierarchy for the annotated entities scanning for persistence annotations
     *
     * @param target the target class
     */
    private void processMethods(Class<T> target) {
        for (Method method : ObjectUtils.getAllMethodsInHierarchy(target)) {
            processMethod(method);
        }
    }

    /**
     * Private Helper.
     * Processes a method scanning for persistence annotations
     *
     * @param method the method
     */
    private void processMethod(Method method) {
        if (method.isAnnotationPresent(OnEvent.class)) {
            OnEvent onEvent = method.getAnnotation(OnEvent.class);
            Set<Method> methods = entityEventListenersMap.get(onEvent.value());
            if (methods == null) {
                methods = new LinkedHashSet<Method>();
                entityEventListenersMap.put(onEvent.value(), methods);
            }
            methods.add(method);
        }
    }

    /**
     * Private Helper.
     * Adds an internal class property to obtain class information from each inserted row
     */
    private void addClassTypePropertyIfSupported() throws ClassNotFoundException, IntrospectionException {
        if (!counterColumnFamily) {
            addProperty(null, PersistenceFactory.CLASS_PROPERTY, String.class, true, false, false, false);
        }
    }

    /**
     * Initializes a class proxy factory that enhances instances wrapping calls to lazy and other methods that need to be
     * audited around invokations
     *
     * @param persistenceFactory the persistence factory associated with this context
     */
    private void initializeProxyFactory(final AbstractPersistenceFactory persistenceFactory) throws IllegalAccessException, InstantiationException {
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setSuperclass(target);
        proxyFactory.setFilter(new MethodFilter() {
            public boolean isHandled(Method m) {
                return isLazyAccessor(m);
            }
        });
        proxyClass = proxyFactory.createClass();
        proxyMethodHandler = new MethodHandler() {
            public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {
                log.debug("lazy loading: " + m.getName());
                persistenceFactory.loadLazyPropertyIfNecessary(ClassMetadata.this, self, proceed, m, args);
                return proceed.invoke(self, args);  // execute the original method.
            }
        };
    }

    /**
     * Informs whether a method corresponds with a lazy accessor
     *
     * @param method the method
     * @return if it's the accessor of a lazy property
     */
    public boolean isLazyAccessor(Method method) {
        return lazyAccesors.containsKey(method);
    }

    /**
     * Private Helper. Scans for named query annotations an initializes the named queries associated with this persistence factory
     *
     * @param target the target class
     */
    private void initializeNamedQueries(Class<T> target) {
        List<NamedQuery> queries = new ArrayList<NamedQuery>();
        if (target.isAnnotationPresent(NamedQueries.class)) {
            NamedQuery[] value = target.getAnnotation(NamedQueries.class).value();
            for (int i = 0, valueLength = value.length; i < valueLength; i++) {
                NamedQuery namedQuery = value[i];
                queries.add(namedQuery);
            }
        }
        if (target.isAnnotationPresent(NamedQuery.class)) {
            queries.add(target.getAnnotation(NamedQuery.class));
        }
        for (NamedQuery query : queries) {
            if (namedQueries.containsKey(query.name())) {
                throw new IllegalStateException(String.format("Duplicated named query name: %s", query.name()));
            }
            namedQueries.put(query.name(), query.query());
        }
    }

    /* Getters & Setters */

    /**
     * @return the column family
     */
    public String getColumnFamily() {
        return columnFamily;
    }

    /**
     * @return the column family definition
     */
    public CfDef getColumnFamilyDefinition() {
        return columnFamilyDefinition;
    }

    /**
     * @return the set of properties that declares themselves as being part of secondary indexes
     */
    public Set<String> getIndexedProperties() {
        return indexedProperties;
    }

    /**
     * @return the key property that holds the key for this class / columnfamily
     */
    public String getKeyProperty() {
        return keyProperty;
    }

    /**
     * @return the keyspace for this column family
     */
    public String getKeySpace() {
        return keySpace;
    }

    /**
     * @return the mapped properties
     */
    public Set<String> getMappedProperties() {
        return mappedProperties;
    }

    /**
     * @return the set of all persistent properties
     */
    public Set<String> getMutationProperties() {
        return mutationProperties;
    }

    /**
     * @return the map of properties and their types
     */
    public Map<String, Class<?>> getPropertiesTypesMap() {
        return propertiesTypesMap;
    }

    /**
     * @return the map of property containers and their type
     */
    public Map<String, Class<?>> getPropertyContainerMap() {
        return propertyContainerMap;
    }

    /**
     * @return the properties that will be considered on selection operation
     */
    public Set<String> getSelectionProperties() {
        return selectionProperties;
    }

    /**
     * @return the target
     */
    public Class<T> getTarget() {
        return target;
    }

    /**
     * @return true if the metadata is associated with a counter column family
     */
    public boolean isCounterColumnFamily() {
        return counterColumnFamily;
    }

    /* Canonical Methods */

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return "ClassMetadata{" +
                "target=" + target.getName() +
                '}';
    }

    /* Misc */

    /**
     * Creates a proxy instance for the class represented in this metadata
     *
     * @return the proxy class
     */
    @SuppressWarnings("unchecked")
    public T createProxy() {
        T instance;
        try {
            instance = (T) proxyClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        ((ProxyObject) instance).setHandler(proxyMethodHandler);
        return instance;
    }

    /**
     * destroys and frees any resources retained by this metadata
     */
    public void destroy() {
        namedQueries.clear();
    }

    /**
     * @param name the column name
     * @return the java type associated to the column
     */
    public Class<?> getColumnClass(String name) {
        return propertiesTypesMap.get(name);
    }

    /**
     * Gets a lazy property associated to a given a method
     *
     * @param method the method
     * @return the lazy property if found, null otherwise
     */
    public String getLazyProperty(Method method) {
        return lazyAccesors.get(method);
    }

    /**
     * Gets a method associated to a given event
     *
     * @param event the event
     * @return the method if found, null otherwise
     */
    public Set<Method> getListenersForEvent(Event.Entity event) {
        return entityEventListenersMap.get(event);
    }

    /**
     * Gets a counter property associated to an increase counter property
     *
     * @param increaseCounterProperty the increase counter property
     * @return the counter property if found, null otherwise
     */
    public String getTargetCounterProperty(String increaseCounterProperty) {
        return counterPropertiesIncrease.get(increaseCounterProperty);
    }

    /**
     * Checks if a property is a container (embedded, mapped or collection)
     *
     * @param property the property
     * @return true if the property is a container
     */
    public boolean isContainer(String property) {
        return isAssociationContainer(property) || isMappedContainer(property) || isMappedCollection(property);
    }

    /**
     * Checks if the property is a embedded container
     *
     * @param property the property
     * @return true if the property is an embedded container
     */
    public boolean isAssociationContainer(String property) {
        return embeddedEntities.contains(property);
    }

    /**
     * Checks if the property is a mapped collection
     *
     * @param property the property
     * @return true if the property is a mapped collection
     */
    public boolean isMappedCollection(String property) {
        return mappedCollections.contains(property);
    }

    /**
     * Checks if the property is a counter increase property.
     * A property that is used to increase counter columns
     *
     * @param property the property
     * @return true if the property is a counter increase property
     */
    public boolean isCounterIncreaseProperty(String property) {
        return counterPropertiesIncrease.containsKey(property);
    }

    /**
     * Checks if the property is a counter property
     * @param property the property
     * @return true if the property is a counter property
     */
    public boolean isCounterProperty(String property) {
        return counterProperties.contains(property);
    }

    /**
     * Checks if the property is an element collection e.g. List<? not an entity>
     * @param property the property
     * @return true if the property is an element collection
     */
    public boolean isElementCollection(String property) {
        return elementCollections.contains(property);
    }

    /**
     * Check if the property is flagged as lazy and should be loaded when it's getter is invoked
     * @param property the property
     * @return true if the property is flagged as a lazy property
     */
    public boolean isLazyProperty(String property) {
        return lazyProperties.contains(property);
    }

    /**
     *
     * @return the keyspace consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }
}
