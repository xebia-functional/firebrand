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

package org.firebrandocm.dao.cql.clauses;

/**
 * Allowed storage parameters in column families
 */
public enum StorageParameter {
	/**
	 * n/a (container attribute)
	 */
	COLUMN_METADATA,
	/**
	 * Standard
	 */
	COLUMN_TYPE,
	/**
	 * n/a
	 */
	COMMENT,
	/**
	 * SizeTiered
	 */
	COMPACTION_STRATEGY,
	/**
	 * n/a (container attribute)
	 */
	COMPACTION_STRATEGY_OPTIONS,
	/**
	 * BytesType
	 */
	COMPARATOR,
	/**
	 * BytesType
	 */
	COMPARE_SUBCOLUMNS_WITH,
	/**
	 * n/a (container attribute)
	 */
	COMPRESSION_OPTIONS,
	/**
	 * n/a
	 */
	DEFAULT_VALIDATION_CLASS,
	/**
	 * 864000 (10 days)
	 */
	GC_GRACE_SECONDS,
	/**
	 * n/a
	 */
	KEY_VALIDATION_CLASS,
	/**
	 * n/a
	 */
	KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
	/**
	 * 200000
	 */
	KEYS_CACHED,
	/**
	 * 32
	 */
	MAX_COMPACTION_THRESHOLD,
	/**
	 * 4
	 */
	MIN_COMPACTION_THRESHOLD,
	/**
	 * n/a (A user-defined value is required)
	 */
	CF_NAME,
	/**
	 * 0.1 (repair 10% of the time)
	 */
	READ_REPAIR_CHANCE,
	/**
	 * true
	 */
	REPLICATE_ON_WRITE,
	/**
	 * 0 (disabled by default)
	 */
	ROWS_CACHED,
	/**
	 * ConcurrentLinkedHashCacheProvider
	 */
	ROW_CACHE_PROVIDER,
	/**
	 * n/a
	 */
	ROW_CACHE_SAVE_PERIOD_IN_SECONDS
}
