/*
 *   Copyright 2012-2013 Hauser Olsson GmbH
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
 * 
 */
package ch.agent.crnickl.mongodb;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SchemaUpdatePolicy;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;

/**
 * A stateless object with methods providing write access to properties.
 * <p>
 * A property in MongoDB is stored as:
 * <blockquote><pre><code>
 * { _id : OID, 
 *   name : STRING, 
 *   type : OID, 
 *   indexed : BOOLEAN 
 * }
 * </code></pre></blockquote>
 * The <em>type</em> field identifies the value type.
 * <p>
 * @author Jean-Paul Vetterli
 */
public class WriteMethodsForProperty extends ReadMethodsForProperty {

	public WriteMethodsForProperty() {
	}
	
	/**
	 * Create a new property and return its key. 
	 * If creation fails throw an exception.
	 * 
	 * @param prop a property
	 * @throws T2DBException
	 */
	public <T>void createProperty(Property<T> prop) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.CREATE, prop);
			BasicDBObject dob = pack(prop);
			getMongoDB(prop).getProperties().insert(dob);
			ObjectId ox = getObjectId(dob);	
			surrogate = makeSurrogate(prop, new MongoDBObjectId(ox));
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E20114, prop.getName());
		prop.getSurrogate().upgrade(surrogate);
	}

	/**
	 * Delete the property.
	 * If deleting fails throw an exception.
	 * 
	 * @param prop a property
	 * @param policy a schema updating policy
	 * @throws T2DBException
	 */
	public <T>void deleteProperty(Property<T> prop, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = prop.getSurrogate();
		MongoDatabase database = (MongoDatabase) s.getDatabase();
		try {
			check(Permission.MODIFY, prop);
			// dangerous update! see comment in MongoDatabase.sleep
			policy.willDelete(prop);
			Property<T> original = database.getReadMethodsForProperty().getProperty(s);
			DBCollection coll = getMongoDB(s).getProperties();
			coll.remove(asQuery(s.getId()), WriteConcern.SAFE);
			database.sleep();
			try {
				policy.willDelete(original);
			} catch (T2DBException e) {
				createProperty(original);
				throw e;
			}
			done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E20115, prop.getName());
	}
	
	/**
	 * Update the name of the property.
	 * If updating fails throw an exception.
	 * 
	 * @param prop a property
	 * @param policy a schema updating policy
	 * @throws T2DBException
	 */
	public void updateProperty(Property<?> prop, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = prop.getSurrogate();
		try {
			check(Permission.MODIFY, prop);
			DBCollection coll = getMongoDB(s).getProperties();
			coll.update(asQuery(s.getId()), operation(Operator.SET, MongoDatabase.FLD_PROP_NAME, prop.getName()));
			done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E20116, prop.getName());
	}
	
	/**
	 * Packs a property into a BSON object.
	 * 
	 * @param prop a Property
	 * @return a BasicDBObject
	 */
	private <T>BasicDBObject pack(Property<T> prop) {
		com.mongodb.BasicDBObject bo = new BasicDBObject();
		if (!prop.getSurrogate().inConstruction())
			bo.put(MongoDatabase.FLD_ID, getId(prop));
		bo.put(MongoDatabase.FLD_PROP_NAME, prop.getName());
		bo.put(MongoDatabase.FLD_PROP_VT, getId(prop.getValueType()));
		bo.put(MongoDatabase.FLD_PROP_INDEXED, prop.isIndexed());
		return bo;
	}
	
}
