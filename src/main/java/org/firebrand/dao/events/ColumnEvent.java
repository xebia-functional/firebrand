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

package org.firebrand.dao.events;

import org.firebrand.dao.PersistenceFactory;

/**
 * Represents a column related event
 */
public class ColumnEvent {
    /* Fields */

	/**
	 * the persistence factory
	 */
	private PersistenceFactory persistenceFactory;

	/**
	 * the event
	 */
	private Event.Column event;

	/**
	 * the affected entity
	 */
	private Object entity;

	/**
	 * the affected property
	 */
	private String property;

	/**
	 * the key used for the column
	 */
	private String key;

	/**
	 * the column family this column belongs to
	 */
	private String columnFamily;

	/**
	 * the actual column object
	 */
	private Object column;

    /* Constructors */

    /**
     * Constructor
     * @param persistenceFactory the persistence factory
     * @param event the event
     * @param entity the affected entity
     * @param property the affected property
     * @param key the key used for the column
     * @param columnFamily the column family this column belongs to
     * @param column the actual column object
     */
	public ColumnEvent(PersistenceFactory persistenceFactory, Event.Column event, Object entity, String property, String key, String columnFamily, Object column) {
		this.persistenceFactory = persistenceFactory;
		this.event = event;
		this.entity = entity;
		this.property = property;
		this.key = key;
		this.columnFamily = columnFamily;
		this.column = column;
	}

    /* Getters & Setters */

    /**
     *
     * @return the actual column object
     */
	public Object getColumn() {
		return column;
	}

    /**
     *
     * @return the column family this column belongs to
     */
	public String getColumnFamily() {
		return columnFamily;
	}

    /**
     *
     * @return the affected entity
     */
	public Object getEntity() {
		return entity;
	}

    /**
     *
     * @return the event
     */
	public Event.Column getEvent() {
		return event;
	}

    /**
     *
     * @return the key used for the column
     */
	public String getKey() {
		return key;
	}

    /**
     *
     * @return the persistence factory
     */
	public PersistenceFactory getPersistenceFactory() {
		return persistenceFactory;
	}

    /**
     *
     * @return the affected property
     */
	public String getProperty() {
		return property;
	}
}
