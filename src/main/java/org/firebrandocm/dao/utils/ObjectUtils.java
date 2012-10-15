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

package org.firebrandocm.dao.utils;

import javassist.util.proxy.ProxyObject;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Generic static object utils
 */
public class ObjectUtils {
    /* Static Methods */

    /**
     * Ensures an object is not null
     * @param object the object
     * @param errorMessage the error message thrown in the @see IllegalArgumentException if the object was null
     */
    public static void notNull(Object object, String errorMessage) {
        if (object == null) throw new IllegalStateException(errorMessage);
    }

    /**
     * Rerturns a @see UUID based on time
     * @return the UUID
     */
    public static java.util.UUID newTimeUuid() {
        return TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    }

    /**
     * Gets an array of all fields in a class hierarchy walking up to parent classes
     * @param objectClass the class
     * @return the fields array
     */
    public static Field[] getAllFieldsInHierarchy(Class<?> objectClass) {
        Set<Field> allFields = new HashSet<Field>();
        Field[] declaredFields = objectClass.getDeclaredFields();
        Field[] fields = objectClass.getFields();
        if (objectClass.getSuperclass() != null) {
            Class<?> superClass = objectClass.getSuperclass();
            Field[] superClassFields = getAllFieldsInHierarchy(superClass);
            allFields.addAll(Arrays.asList(superClassFields));
        }
        allFields.addAll(Arrays.asList(declaredFields));
        allFields.addAll(Arrays.asList(fields));
        return allFields.toArray(new Field[allFields.size()]);
    }

    /**
     * Gets an array of all methods in a class hierarchy walking up to parent classes
     * @param objectClass the class
     * @return the methods array
     */
    public static Method[] getAllMethodsInHierarchy(Class<?> objectClass) {
        Set<Method> allMethods = new HashSet<Method>();
        Method[] declaredMethods = objectClass.getDeclaredMethods();
        Method[] methods = objectClass.getMethods();
        if (objectClass.getSuperclass() != null) {
            Class<?> superClass = objectClass.getSuperclass();
            Method[] superClassMethods = getAllMethodsInHierarchy(superClass);
            allMethods.addAll(Arrays.asList(superClassMethods));
        }
        allMethods.addAll(Arrays.asList(declaredMethods));
        allMethods.addAll(Arrays.asList(methods));
        return allMethods.toArray(new Method[allMethods.size()]);
    }

    /**
     * Validates that all values are set.
     *
     * @param values a varargs array of arguments
     */
    public static void defenseNotNull(Object... values) {
        if (values == null) {
            throw new IllegalArgumentException("values is null");
        }
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                throw new IllegalArgumentException(String.format("values[%d] is null", i));
            }
        }
    }

    /**
     * Gets the real class from a potentially proxied class
     * @param potentialProxyClass the potentially proxied class
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Class<?> getRealClass(Class<?> potentialProxyClass) {
        Class<?> targetClass = potentialProxyClass;
        if (targetClass != null && ProxyObject.class.isAssignableFrom(targetClass)) {
            targetClass = potentialProxyClass.getSuperclass();
        }
        return targetClass;
    }

    /* Constructors */

    /**
     * Prevents from instantiation
     */
    private ObjectUtils() {
    }
}
