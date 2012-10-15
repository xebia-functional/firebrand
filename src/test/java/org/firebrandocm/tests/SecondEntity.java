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

package org.firebrandocm.tests;

import org.firebrandocm.dao.annotations.ColumnFamily;
import org.firebrandocm.dao.annotations.Embedded;
import org.firebrandocm.dao.annotations.Key;
import org.firebrandocm.dao.annotations.Mapped;

@ColumnFamily
public class SecondEntity {

	@Key
	private String id;

	private String name;

	@Mapped
	private SecondEntity recursiveMapped;

	@Embedded
	private ThirdEntity embedEntityInRecursiveMappedEntity;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public SecondEntity getRecursiveMapped() {
		return recursiveMapped;
	}

	public void setRecursiveMapped(SecondEntity recursiveMapped) {
		this.recursiveMapped = recursiveMapped;
	}

	public ThirdEntity getEmbedEntityInRecursiveMappedEntity() {
		return embedEntityInRecursiveMappedEntity;
	}

	public void setEmbedEntityInRecursiveMappedEntity(ThirdEntity embedEntityInRecursiveMappedEntity) {
		this.embedEntityInRecursiveMappedEntity = embedEntityInRecursiveMappedEntity;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SecondEntity that = (SecondEntity) o;

		if (id != null ? !id.equals(that.id) : that.id != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return id != null ? id.hashCode() : 0;
	}

	@Override
	public String toString() {
		return "SecondEntity{" +
				"id='" + id + '\'' +
				'}';
	}
}
