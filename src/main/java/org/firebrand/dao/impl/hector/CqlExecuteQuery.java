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

package org.firebrand.dao.impl.hector;

import me.prettyprint.cassandra.model.AbstractBasicQuery;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.QueryResultImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.query.QueryResult;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Support for concrete execute queries to complement the existing Hector API
 *
 * @param <V>
 */
@SuppressWarnings("unchecked")
public class CqlExecuteQuery<V> extends AbstractBasicQuery {
    /* Fields */

    private static Logger log = LoggerFactory.getLogger(CqlExecuteQuery.class);

    private boolean useCompression;

    private ByteBuffer query;

    /* Constructors */

    public CqlExecuteQuery(Keyspace k) {
        super(k, StringSerializer.get(), StringSerializer.get());
    }

    /* Interface Implementations */


// --------------------- Interface Query ---------------------

    @Override
    public QueryResult<V> execute() {
        return new QueryResultImpl<V>(
                keyspace.doExecuteOperation(new Operation<V>(OperationType.WRITE) {
                    @Override
                    public V execute(Client cassandra) throws HectorException {
                        Object queryResult = null;
                        try {
                            CqlResult result = cassandra.execute_cql_query(query,
                                    useCompression ? Compression.GZIP : Compression.NONE);
                            if (log.isDebugEnabled()) {
                                log.debug("Found CqlResult: {}", result);
                            }
                            switch (result.getType()) {
                                case VOID:
                                    break;

                                case INT:
                                    queryResult = result.getNum();  //TODO this may be not correct as there seems to be no way to obtain affected columns/rows
                                    break;

                                default:
                                    throw new IllegalArgumentException(String.format("query returned result rows. use %s instead", CqlQuery.class));
                            }
                        } catch (Exception ex) {
                            throw keyspace.getExceptionsTranslator().translate(ex);
                        }
                        return (V) queryResult;
                    }
                }), this);
    }

    /* Misc */

    /**
     * Set the query as a String. Here for convienience. See above for some
     * caveats. Calls {@link StringSerializer#toByteBuffer(String)} directly.
     *
     * @param query
     * @return
     */
    public CqlExecuteQuery<V> setQuery(String query) {
        log.debug(String.format("setQuery: %s", query));
        this.query = StringSerializer.get().toByteBuffer(query);
        return this;
    }

    public CqlExecuteQuery<V> setQuery(ByteBuffer query) {
        this.query = query;
        return this;
    }

    public CqlExecuteQuery<V> useCompression() {
        useCompression = true;
        return this;
    }
}
