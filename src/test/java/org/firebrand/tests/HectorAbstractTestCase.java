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

package org.firebrand.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.firebrand.dao.Query;
import org.firebrand.dao.impl.hector.HectorPersistenceFactory;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.firebrand.dao.cql.QueryBuilder.*;

/**
 * Abstract test case
 */
public abstract class HectorAbstractTestCase {

	protected static Log log;

	protected static HectorPersistenceFactory factory = null;

	private static final String DEFAULT_KEYSPACE = "FirebrandTestKeyspace";

	public static final String RPC_LISTEN_ADDRESS = "127.0.0.1";

	public static final int RPC_PORT = 19160;

//	public static final int RPC_PORT = 9160;

	public static String BASE_DIRECTORY = "/tmp/FirebrandTestKeyspace";

	public static final boolean START_EMBEDDED_SERVER = true;

	public static final boolean CLEAN_UP_DIRECTORIES = true;

	public static final boolean DROP_ON_DESTROY = true;

	public HectorAbstractTestCase() {
		log = LogFactory.getLog(getClass());
	}

	protected static List<Class<?>> persistentClasses;


	public static void initWithClasses(Class<?>... clazzez) throws Exception {
		List<Class<?>> classList = Arrays.asList(clazzez);
		factory = new HectorPersistenceFactory();
		factory.setDefaultKeySpace(DEFAULT_KEYSPACE);
		factory.setContactNodes(new String[]{RPC_LISTEN_ADDRESS});
		factory.setPoolName("Main");
		factory.setDebug(false);
		factory.setThriftPort(RPC_PORT);
		factory.setStartEmbeddedServer(START_EMBEDDED_SERVER);
		factory.setEmbeddedServerBaseDir(BASE_DIRECTORY);
		factory.setCleanupDirectories(CLEAN_UP_DIRECTORIES);
		factory.setEntities(classList);
		factory.setDropOnDestroy(DROP_ON_DESTROY);
		factory.init();
		persistentClasses = classList;
	}

	@Before
	public void setup(){
		if (persistentClasses == null) {
			throw new IllegalStateException("set persistentClassNames in @BeforeClass");
		}
		for (Class<?> persistentClass : persistentClasses) {
			factory.executeQuery(Integer.class, Query.get(truncate(
					columnFamily(persistentClass.getSimpleName())
			)));
			List<?> results = factory.getResultList(persistentClass, Query.get(select(
					allColumns(),
					from(persistentClass.getSimpleName())
			)));
			assertEquals(0, results.size());
		}
	}

	@AfterClass
	public static void destroy() throws Exception {
		factory.destroy();
	}


}
