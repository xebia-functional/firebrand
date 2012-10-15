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

package org.firebrandocm.dao.events;

public interface Event {
    /* Enumerations */

	enum Entity {
		/**
		 * Before an entity is delete
		 */
		PRE_DELETE,

		/**
		 * After an entity is deleted
		 */
		POST_DELETE,

		/**
		 * Before an entity is loaded
		 */
		PRE_LOAD,

		/**
		 * After an entity is loaded
		 */
		POST_LOAD,

		/**
		 * Before an entity is persisted or updated
		 */
		PRE_PERSIST,

		/**
		 * After an entity is persisted or updated
		 */
		POST_PERSIST,

		/**
		 * After a batch of operations is flushed to the data store
		 */
		POST_COMMIT
	}

	enum Column {
		/**
		 * Before a column is mutated
		 */
		PRE_COLUMN_MUTATION,

		/**
		 * Before a counter is mutated
		 */
		PRE_COUNTER_MUTATION,

		/**
		 * After a column is mutated
		 */
		POST_COLUMN_MUTATION,

		/**
		 * After a counter is mutated
		 */
		POST_COUNTER_MUTATION,

		/**
		 * Before a column is deleted
		 */
		PRE_COLUMN_DELETION,

		/**
		 * After a column is deleted
		 */
		POST_COLUMN_DELETION
	}
}
