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

public class OtherEntity {

	private String firstProperty;

	private String nullProperty;

	@Embedded
	private ThirdEntity nestedThirdProperty;

	public String getNullProperty() {
		return nullProperty;
	}

	public void setNullProperty(String nullProperty) {
		this.nullProperty = nullProperty;
	}

	public String getFirstProperty() {
		return firstProperty;
	}

	public void setFirstProperty(String firstProperty) {
		this.firstProperty = firstProperty;
	}

	public ThirdEntity getNestedThirdProperty() {
		return nestedThirdProperty;
	}

	public void setNestedThirdProperty(ThirdEntity nestedThirdProperty) {
		this.nestedThirdProperty = nestedThirdProperty;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof OtherEntity)) return false;

		OtherEntity that = (OtherEntity) o;

		if (firstProperty != null ? !firstProperty.equals(that.firstProperty) : that.firstProperty != null)
			return false;
		if (nestedThirdProperty != null ? !nestedThirdProperty.equals(that.nestedThirdProperty) : that.nestedThirdProperty != null)
			return false;
		if (nullProperty != null ? !nullProperty.equals(that.nullProperty) : that.nullProperty != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = firstProperty != null ? firstProperty.hashCode() : 0;
		result = 31 * result + (nullProperty != null ? nullProperty.hashCode() : 0);
		result = 31 * result + (nestedThirdProperty != null ? nestedThirdProperty.hashCode() : 0);
		return result;
	}
}
