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

import org.firebrandocm.dao.annotations.Embedded;

public class ThirdEntity {

	private String someProperty;

	@Embedded
	private FourthEntity fourthEntity;

	@Embedded
	private FourthEntity fourthEntityNull;

	public FourthEntity getFourthEntity() {
		return fourthEntity;
	}

	public void setFourthEntity(FourthEntity fourthEntity) {
		this.fourthEntity = fourthEntity;
	}

	public FourthEntity getFourthEntityNull() {
		return fourthEntityNull;
	}

	public void setFourthEntityNull(FourthEntity fourthEntityNull) {
		this.fourthEntityNull = fourthEntityNull;
	}

	public String getSomeProperty() {
		return someProperty;
	}

	public void setSomeProperty(String someProperty) {
		this.someProperty = someProperty;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof ThirdEntity)) return false;

		ThirdEntity that = (ThirdEntity) o;

		if (fourthEntity != null ? !fourthEntity.equals(that.fourthEntity) : that.fourthEntity != null) return false;
		if (fourthEntityNull != null ? !fourthEntityNull.equals(that.fourthEntityNull) : that.fourthEntityNull != null)
			return false;
		if (someProperty != null ? !someProperty.equals(that.someProperty) : that.someProperty != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = someProperty != null ? someProperty.hashCode() : 0;
		result = 31 * result + (fourthEntity != null ? fourthEntity.hashCode() : 0);
		result = 31 * result + (fourthEntityNull != null ? fourthEntityNull.hashCode() : 0);
		return result;
	}
}
