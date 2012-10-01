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

package org.firebrand.dao.annotations;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for properties that are mapped to columns
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {

    /* Misc */

	/**
	 *
	 * @return if this column represents a cassandra counter, bind the value of the column with the counter
	 */
	boolean counter() default false;

	/**
	 *
	 * @return the index type
	 */
	IndexType indexType() default IndexType.KEYS;

	/**
	 *
	 * @return if this column should be part of the secondary index
	 */
	boolean indexed() default false;

	/**
	 *
	 * @return if this basic column should be loaded when its getter  is invoked
	 */
	boolean lazy() default false;

	/**
	 *
	 * @return an optional validation class
	 */
	Class<? extends AbstractType> validationClass() default UTF8Type.class;

    /* Inner Classes */

	public static interface DEFAULTS {
		IndexType INDEX_TYPE = IndexType.KEYS;

		Class<? extends AbstractType> VALIDATION_CLASS = UTF8Type.class;

		boolean INDEXED = false;
	}
}
