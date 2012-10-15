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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.firebrandocm.dao.cql.QueryBuilder;
import org.firebrandocm.dao.cql.statement.Statement;
import org.firebrandocm.dao.utils.ObjectUtils;

import java.util.Map;

/**
 * Encapsulates information regarding a query
 */
public class Query {
    /* Fields */

	private static Log log = LogFactory.getLog(Query.class);
	
	private String query;
	
	private Map<String, Object> params;

    /* Static Methods */

	public static Query get(Statement query) {
		return new Query(query.build(), null);
	}

	public static Query get(String query) {
		return new Query(query, null);
	}

	public static Query get(Statement query, Map<String, Object> params) {
		return new Query(query.build(), params);
	}

	public static Query get(String query, Map<String, Object> params) {
		return new Query(query, params);
	}

    /* Constructors */

	private Query(String query, Map<String, Object> params) {
		ObjectUtils.defenseNotNull(query);
		this.params = params;
		this.query = buildQuery(query);
	}

	private String buildQuery(String query) {
		String queryText = ClassMetadata.getNullSafeNamedQuery(query);
		if (queryText == null) {
			queryText = query;
		}
		if (params != null) {
			for (Map.Entry<String, Object> entryParam : params.entrySet()) {
				String convertedValue = QueryBuilder.convert(entryParam.getValue());
				queryText = queryText.replaceAll(String.format("\\:%s", entryParam.getKey()), convertedValue);
			}	
		}
		if (log.isDebugEnabled()) log.debug(String.format("query: %s", queryText));
		return queryText;
	}

    /* Getters & Setters */

	public String getQuery() {
		return query;
	}
}
