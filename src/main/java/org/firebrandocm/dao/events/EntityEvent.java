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

import org.firebrandocm.dao.PersistenceFactory;

/**
 * An entity level event
 */
public class EntityEvent {
    /* Fields */

	/**
	 * the factory in the context of this event
	 */
	private PersistenceFactory persistenceFactory;

	/**
	 * the event
	 */
	private Event.Entity event;

	/**
	 * the affected entity
	 */
	private Object[] entities;

    /* Constructors */

    /**
     * Constructor
     * @param persistenceFactory  the factory in the context of this event
     * @param event the event
     * @param entities the affected entity
     */
	public EntityEvent(PersistenceFactory persistenceFactory, Event.Entity event, Object... entities) {
		this.persistenceFactory = persistenceFactory;
		this.event = event;
		this.entities = this.entities;
	}

    /* Getters & Setters */

    /**
     *
     * @return the affected entity
     */
	public Object[] getEntities() {
		return entities;
	}

    /**
     *
     * @return the event
     */
	public Event.Entity getEvent() {
		return event;
	}

    /**
     *
     * @return the factory in the context of this event
     */
	public PersistenceFactory getPersistenceFactory() {
		return persistenceFactory;
	}
}
