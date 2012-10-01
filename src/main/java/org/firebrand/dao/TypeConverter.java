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

package org.firebrand.dao;

import java.nio.ByteBuffer;

public interface TypeConverter<T> {
    /* Misc */

	/**
	 *
	 * @param value the value as a byte buffer
	 * @return the value converted as an object
	 */
	T fromValue(ByteBuffer value) throws Exception;

	/**
	 *
	 *
	 * @param value the value as an object, invoked for mutate operations
	 * @return a byte buffer for the current object value
	 */
	ByteBuffer toValue(T value) throws Exception;
}
