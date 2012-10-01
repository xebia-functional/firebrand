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
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for classes that represent a column family
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ColumnFamily {
    /* Misc */

	/**
	 *
	 * @return additional human-readable information about the column family to its definition.
	 */
	String comment() default "";

	/**
	 *
	 * @return how to sort the columns for slicing
	 * operations. The default is UTF8Type, unlike in the default Cassandra distribution which is BytesType.
	 * Other options are AsciiType, BytesType, LexicalUUIDType, TimeUUIDType, LongType,
	 * and IntegerType (a generic variable-length integer type).
	 * You can also specify the fully-qualified class name to a class of
	 * your choice extending org.apache.cassandra.db.marshal.AbstractType.
	 */
	Class<? extends AbstractType> compareWith() default UTF8Type.class;

	/**
	 *
	 * @return a validator class to use for validating key values in the CF.
	 */
	Class<? extends AbstractType> defaultKeyValidationClass() default UTF8Type.class;

	/**
	 *
	 * @return a validator class to use for validating all the column values in the CF.
	 */
	Class<? extends AbstractType> defaultValidationClass() default BytesType.class;

	/**
	 *
	 * @return the time to wait before garbage collecting tombstones (deletion markers).
	 * defaults to 864000 (10 days). See http://wiki.apache.org/cassandra/DistributedDeletes
	 */
	int gcGraceSeconds() default 864000;

	/**
	 *
	 * @return the keySpace this column family belongs to. Defaults to the default factory application id if not specified
	 */
	String keySpace() default "";

	/**
	 *
	 * @return the number of keys per sstable whose
	 * locations we keep in memory in "mostly LRU" order.  (JUST the key
	 * locations, NOT any column values.) Specify a fraction (value less
	 * than 1) or an absolute number of keys to cache.  Defaults to 200000
	 * keys.
	 */
	long keysCached() default 200000;

	/**
	 *
	 * @return the maximum number of SSTables allowed before a minor compaction is forced.
	 * Decreasing this will cause minor compactions to start more frequently and be less intensive.
	 * Setting this to 0 disables minor compactions.  defaults to 32.
	 */
	int maxCompactionThreshold() default 32;

	/**
	 *
	 * @return the minimum number of SSTables needed to start a minor compaction.
	 * Increasing this will cause minor
	 * compactions to start less frequently and be more intensive. setting this to 0 disables minor compactions.
	 * defaults to 4.
	 */
	int minCompactionThreshold() default 4;

	/**
	 *
	 * @return the probability with which read repairs should be invoked on non-quorum reads.
	 * must be between 0 and 1. defaults to 1.0 (always read repair).
	 */
	double readRepairChance() default 1.0;

	/**
	 *
	 * @return
	 */
	boolean replicateOnWrite() default true;

	/**
	 *
	 * @return if the (reversed=true) flag should be applied to the comparator for inserted sorted of columns
	 */
	boolean reversed() default false;

	/**
	 *
	 * @return the number of rows whose entire contents we
	 * cache in memory. Do not use this on ColumnFamilies with large rows,
	 * or ColumnFamilies with high write:read ratios. Specify a fraction
	 * (value less than 1) or an absolute number of rows to cache.
	 * Defaults to 0. (i.e. row caching is off by default)
	 */
	long rowsCached() default 0;
}