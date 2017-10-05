/*
 *   Copyright 2012-2017 Hauser Olsson GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.agent.crnickl.mongodb;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.DBObjectId;


/**
 * JDBC databases provide an int32 as id of inserted rows.
 * 
 * @author Jean-Paul Vetterli
 *
 */
public class MongoDBObjectId implements DBObjectId {

	private ObjectId id;

	public MongoDBObjectId(ObjectId id) {
		this.id = id;
	}
	
	private static ObjectId asObjectId(Object object) throws T2DBMException {
		try {
			return (ObjectId) object;
		} catch (Throwable t) {
			throw T2DBMMsg.exception(t, D.D02105, object == null ? "null" : object.toString());
		}
	}

	/**
	 * Construct an object id from an object.
	 * 
	 * @param object
	 * @throws T2DBJException
	 */
	public MongoDBObjectId(Object object) throws T2DBMException {
		this(asObjectId(object));
	}
	
	public ObjectId value() {
		return id;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MongoDBObjectId other = (MongoDBObjectId) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return id == null ? "null" : id.toString();
	}


}
