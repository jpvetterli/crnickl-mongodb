/*
 *   Copyright 2012 Hauser Olsson GmbH
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
 * Package: ch.agent.crnickl.jdbc
 * Type: WriteMethodsForValueType
 * Version: 1.0.0
 */
package ch.agent.crnickl.mongodb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.DBObjectId;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueScanner;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SchemaUpdatePolicy;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;

/**
 * A stateless object with methods providing write access to value types.
 * <p>
 * A value type in MongoDB is stored as:
 * <blockquote><pre><code>
 * { _id : OID, 
 *   name : STRING, 
 *   type : STRING, 
 *   values? : { 
 *      STRING : STRING, ... 
 *    } 
 * }
 * </code></pre></blockquote>
 * The <em>values</em> field exists only in restricted value types.
 * <p>
 * 
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class WriteMethodsForValueType extends MongoDatabaseMethods {

	public WriteMethodsForValueType() {
	}
	
	/**
	 * Create a value type in the database.
	 * Throw an exception if the operation fails.
	 * 
	 * @param vt a value type
	 * @throws T2DBException
	 */
	public <T>void createValueType(ValueType<T> vt) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.CREATE, vt);
			DBObjectId ox = insert(vt);
			surrogate = makeSurrogate(vt, ox);
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E10114, vt.getName());
		vt.getSurrogate().upgrade(surrogate);
	}

	/**
	 * Delete a value type from the database.
	 * Throw an exception if the operation fails.
	 * 
	 * @param vt a value type
	 * @param policy a schema udpdating policy
	 * @throws T2DBException
	 */
	public <T>void deleteValueType(ValueType<T> vt, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = vt.getSurrogate();
		MongoDatabase database = (MongoDatabase) s.getDatabase();
		try {
			check(Permission.MODIFY, vt);
			policy.willDelete(vt);
			ValueType<T> original = database.getReadMethodsForValueType().getValueType(s);
			DBCollection coll = getMongoDB(s).getValueTypes();
			coll.remove(asQuery(s.getId()), WriteConcern.SAFE); // ??? REPLICA_SAFE --> MongoException("norepl");
			database.sleep();
			try {
				policy.willDelete(original);
			} catch (T2DBException e) {
				// Oops! referential integrity broken!
				createValueType(original);
				throw e;
			}
			done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (!done || cause != null) {
			throw T2DBMsg.exception(cause, E.E10145, vt.getName());
		}
	}
	
	/**
	 * Update a value type in the database.
	 * Throw an exception if the operation cannot be done.
	 * Updating a value type is an expensive operation, because in case 
	 * 
	 * @param vt a value type
	 * @param policy a schema udpdating policy
	 * @throws T2DBException
	 */
	public <T>void updateValueType(ValueType<T> vt, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = vt.getSurrogate();
		MongoDatabase database = (MongoDatabase) s.getDatabase();
		try {
			check(Permission.MODIFY, vt);
			DBCollection coll = getMongoDB(s).getValueTypes();
			
			if (vt.isRestricted() && vt.getValues().size() > 0) {
				ValueType<T> original = database.getReadMethodsForValueType().getValueType(s);
				Set<T> deleted = deletedValues(original, vt);
				com.mongodb.DBObject operation = operation(Operator.SET,
						MongoDatabase.FLD_VT_NAME, vt.getName(),
						MongoDatabase.FLD_VT_VALUES, valueDescriptionsAsMap(vt));
				
				if (deleted.size() > 0) {
					// dangerous update! see comment in MongoDatabase.sleep
					Iterator<T> it = deleted.iterator();
					while (it.hasNext()) {
						policy.willDelete(vt, it.next());
					}
					coll.update(asQuery(s.getId()), operation, false, false, WriteConcern.SAFE);
					database.sleep();
					try {
						it = deleted.iterator();
						while (it.hasNext()) {
							policy.willDelete(vt, it.next());
						}
					} catch (T2DBException e) {
						// restore state and throw exception
						operation = operation(Operator.SET, 
								MongoDatabase.FLD_VT_NAME, original.getName(),
								MongoDatabase.FLD_VT_VALUES, valueDescriptionsAsMap(original));
						coll.update(asQuery(s.getId()), operation);
						throw e;
					}
				} else {
					coll.update(asQuery(s.getId()), operation);
				}
			} else
				coll.update(asQuery(s.getId()), operation(Operator.SET, MongoDatabase.FLD_VT_NAME, vt.getName()));
			done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E10146, vt.getName());
	}
	
	private <T>Set<T> deletedValues(ValueType<T> original, ValueType<T> vt) throws T2DBException {
		Set<T> deleted = new HashSet<T>(original.getValues());
		deleted.removeAll(vt.getValues());
		return deleted;
	}
	
	private <T>DBObjectId insert(ValueType<T> vt) throws T2DBException {
		Surrogate s = vt.getSurrogate();
		com.mongodb.BasicDBObject bo = new BasicDBObject();
		if (!s.inConstruction())
			bo.put(MongoDatabase.FLD_ID, getId(vt));
		bo.put(MongoDatabase.FLD_VT_NAME, vt.getName());
		bo.put(MongoDatabase.FLD_VT_TYPE, vt.getExternalRepresentation());
		if (vt.isRestricted())
			bo.put(MongoDatabase.FLD_VT_VALUES, valueDescriptionsAsMap(vt));
		getMongoDB(s).getValueTypes().insert(bo);
		ObjectId ox = getObjectId(bo);	
		return new MongoDBObjectId(ox);
	}
	
	private <T> Map<String, String> valueDescriptionsAsMap(ValueType<T> vt) throws T2DBException {
		Map<T, String> vd = vt.getValueDescriptions();
		ValueScanner<T> scanner = vt.getScanner();
		Map<String, String> map = new HashMap<String, String>(vd.size());
		for (Map.Entry<T, String> entry : vd.entrySet()) {
			map.put(scanner.toString(entry.getKey()), entry.getValue());
		}
		return map;
	}
	
}
