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

package org.firebrandocm.dao.ocmcql;

import org.firebrandocm.dao.AbstractPersistenceFactory;
import org.firebrandocm.dao.annotations.ColumnFamily;
import org.firebrandocm.dao.cql.converters.CQLValueConverter;
import org.firebrandocm.dao.utils.ObjectUtils;

/**
 * Converts a mapped entity to a string value switchable for CQL
 */
public class CQLMappedEntityValueConverter implements CQLValueConverter<Object>{
    /* Fields */

	private AbstractPersistenceFactory persistenceFactory;

    /* Constructors */

	public CQLMappedEntityValueConverter(AbstractPersistenceFactory persistenceFactory) {
		this.persistenceFactory = persistenceFactory;
	}

    /* Interface Implementations */


// --------------------- Interface CQLValueConverter ---------------------

	@Override
	public boolean applyConverter(Object value) {
		return ObjectUtils.getRealClass(value.getClass()).isAnnotationPresent(ColumnFamily.class);
	}

	@Override
	public String convert(Object value) {
		return persistenceFactory.getMappedPropertyTokenForPersistentValues(value);
	}
}
