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

package org.firebrand.dao.ocmcql;

import org.firebrand.dao.cql.converters.CQLValueConverter;

import java.util.Collection;

/**
 * Converts a mapped entity to a string value switchable for CQL
 */
public class CQLMappedCollectionValueConverter implements CQLValueConverter<Collection<?>>{
    /* Fields */

	private CQLMappedEntityValueConverter cqlMappedEntityValueConverter;

    /* Constructors */

	public CQLMappedCollectionValueConverter(CQLMappedEntityValueConverter cqlMappedEntityValueConverter) {
		this.cqlMappedEntityValueConverter = cqlMappedEntityValueConverter;
	}

    /* Interface Implementations */


// --------------------- Interface CQLValueConverter ---------------------

	@Override
	public boolean applyConverter(Object value) {
		return Collection.class.isAssignableFrom(value.getClass()) && applyConverterInternal((Collection<?>) value);
	}

	@Override
	public String convert(Collection<?> value) {
		return cqlMappedEntityValueConverter.convert(value);
	}

    /* Misc */

	private boolean applyConverterInternal(Collection<?> value) {
		boolean apply = true;
		for (Object item : value) {
			apply = cqlMappedEntityValueConverter.applyConverter(item);
			if (apply) break;
		}
		return apply;
	}
}
