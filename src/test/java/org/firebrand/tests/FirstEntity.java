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

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.firebrand.dao.annotations.*;
import org.firebrand.dao.events.Event;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

@ColumnFamily
@NamedQueries({
		@NamedQuery(name = FirstEntity.QUERY_ALL_ENTITIES, query = "select * from FirstEntity"),
		@NamedQuery(name = FirstEntity.QUERY_ALL_ENTITIES_WITH_PARAMS, query = "select * from FirstEntity where KEY = :key")
})
public class FirstEntity {
	
	public static final String QUERY_ALL_ENTITIES = "FirstEntity.QUERY_ALL_ENTITIES";

	public static final String QUERY_ALL_ENTITIES_WITH_PARAMS = "FirstEntity.QUERY_ALL_ENTITIES_WITH_PARAMS";

    @Key
    private String id;

	@Column(indexed = true)
    private String name;

	@Column(indexed = true)
    private String description;

	@Column(indexed = true, validationClass = LongType.class)
	private Long phone;

	@Column(indexed = true, validationClass = DoubleType.class)
	private Double score;

	@Column(indexed = true, validationClass = LongType.class)
	private Date date;

	@Column(validationClass = LongType.class)
	private Date otherDate;

	@Mapped
	private FirstEntityCounter counter;

	@Mapped
	private SecondEntity mappedEntity;

	@Mapped(lazy = true)
	private SecondEntity secondLazyMappedEntity;

	@MappedCollection
	private List<SecondEntity> listProperty;

	@MappedCollection(lazy = false)
	private List<SecondEntity> secondEagerListProperty;

	@Embedded
	private OtherEntity otherEntity;

	private String changedColumnName;

	@Column(validationClass = BytesType.class)
	private LinkedList<Object> listSerializedAsBytes;

	@Column(lazy = true)
	private String hugeDescription;
	
	private String prePersistProperty;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<SecondEntity> getListProperty() {
		return listProperty;
	}

	public void setListProperty(List<SecondEntity> listProperty) {
		this.listProperty = listProperty;
	}

	public SecondEntity getMappedEntity() {
		return mappedEntity;
	}

	public void setMappedEntity(SecondEntity mappedEntity) {
		this.mappedEntity = mappedEntity;
	}

	public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

	public Long getPhone() {
		return phone;
	}

	public void setPhone(Long phone) {
		this.phone = phone;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Date getOtherDate() {
		return otherDate;
	}

	public void setOtherDate(Date otherDate) {
		this.otherDate = otherDate;
	}

	public String getChangedColumnName() {
		return changedColumnName;
	}

	public void setChangedColumnName(String changedColumnName) {
		this.changedColumnName = changedColumnName;
	}

	public OtherEntity getOtherEntity() {
		return otherEntity;
	}

	public void setOtherEntity(OtherEntity otherEntity) {
		this.otherEntity = otherEntity;
	}

	public SecondEntity getSecondLazyMappedEntity() {
		return secondLazyMappedEntity;
	}

	public void setSecondLazyMappedEntity(SecondEntity secondLazyMappedEntity) {
		this.secondLazyMappedEntity = secondLazyMappedEntity;
	}

	public List<SecondEntity> getSecondEagerListProperty() {
		return secondEagerListProperty;
	}

	public void setSecondEagerListProperty(List<SecondEntity> secondEagerListProperty) {
		this.secondEagerListProperty = secondEagerListProperty;
	}

	public String getHugeDescription() {
		return hugeDescription;
	}

	public void setHugeDescription(String hugeDescription) {
		this.hugeDescription = hugeDescription;
	}

	public LinkedList<Object> getListSerializedAsBytes() {
		return listSerializedAsBytes;
	}

	public void setListSerializedAsBytes(LinkedList<Object> listSerializedAsBytes) {
		this.listSerializedAsBytes = listSerializedAsBytes;
	}

	public FirstEntityCounter getCounter() {
		return counter;
	}

	public void setCounter(FirstEntityCounter counter) {
		this.counter = counter;
	}

	public String getPrePersistProperty() {
		return prePersistProperty;
	}

	public void setPrePersistProperty(String prePersistProperty) {
		this.prePersistProperty = prePersistProperty;
	}

	@OnEvent(Event.Entity.PRE_PERSIST)
	public void onPrePersist() {
		setPrePersistProperty(UUID.randomUUID().toString());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof FirstEntity)) return false;

		FirstEntity that = (FirstEntity) o;

		if (id != null ? !id.equals(that.id) : that.id != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return id != null ? id.hashCode() : 0;
	}

	@Override
	public String toString() {
		return "FirstEntity{" +
				"id='" + id + '\'' +
				'}';
	}
}

