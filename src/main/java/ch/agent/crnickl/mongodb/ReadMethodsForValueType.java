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
 * Type: ReadMethodsForValueType
 * Version: 1.0.0
 */
package ch.agent.crnickl.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.ValueTypeImpl;
import ch.agent.crnickl.mongodb.T2DBMMsg.J;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * A stateless object with methods providing read access to value types.
 *  
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class ReadMethodsForValueType extends MongoDatabaseMethods {

	public ReadMethodsForValueType() {
	}

	/**
	 * Find a value type with a given name.
	 * 
	 * @param database a database
	 * @param name a string
	 * @return a value type or null
	 * @throws T2DBException
	 */
	public <T>ValueType<T> getValueType(Database database, String name) throws T2DBException {
		try {
			DBObject obj = (BasicDBObject) getMongoDB(database).getValueTypes().findOne(
					mongoObject(MongoDatabase.FLD_VT_NAME, name));
			if (obj != null)
				return unpack(database, (BasicDBObject) obj);
			else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E10104, name);
		}
	}
	
	/**
	 * Find a collection of value types with names matching a pattern.
	 * If the pattern is enclosed in slashes it is taken as a standard
	 * regexp pattern; the slashes will be removed. If it is not enclosed
	 * in slashes, it is taken as a minimal pattern and all occurrences of
	 * "*" will be replaced with ".*" (zero or more arbitrary characters). 
	 * 
	 * @param database a database
	 * @param pattern a simple pattern or a regexp pattern
	 * @return a collection of value types, possibly empty, never null
	 * @throws T2DBException
	 */
	public Collection<ValueType<?>> getValueTypes(Database database, String pattern) throws T2DBException {
		try {
			DBObject query = null;
			if (pattern != null && pattern.length() > 0) {
				String regexp = extractRegexp(pattern);
				if (regexp == null)
					pattern = pattern.replace("*", ".*");
				else
					pattern = regexp;
				query = mongoObject(MongoDatabase.FLD_VT_NAME, Pattern.compile(pattern));
			}
			DBCursor cursor = getMongoDB(database).getValueTypes().find(query);
			Collection<ValueType<?>> result = new ArrayList<ValueType<?>>();
			try {
				while (cursor.hasNext()) {
					result.add(unpack(database, (BasicDBObject) cursor.next()));
				}
			} finally {
				cursor.close();
			}
			return result;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E10106, pattern);
		}
	}
	
	/**
	 * Find a value type corresponding to a surrogate.
	 * 
	 * @param s a surrogate
	 * @return a value type or null
	 * @throws T2DBException
	 */
	public <T>ValueType<T> getValueType(Surrogate s) throws T2DBException {
		try {
			DBObject obj = getObject(s, false);
			if (obj != null)
				return unpack(s.getDatabase(), (BasicDBObject) obj);
			else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E10105, s.toString());
		}
	}
	
	@SuppressWarnings("unchecked")
	private <T>ValueType<T> unpack(Database db, BasicDBObject obj) throws T2DBException {
		try {
			ObjectId id = obj.getObjectId(MongoDatabase.FLD_ID);
			String name = obj.getString(MongoDatabase.FLD_VT_NAME);
			String type = obj.getString(MongoDatabase.FLD_VT_TYPE);
			Object values = obj.get(MongoDatabase.FLD_VT_VALUES);
			boolean restricted = values != null;
			Map<String, String> valueMap = null;
			if (restricted)
				valueMap = (Map<String, String>)((BasicDBObject) values).toMap();
			Surrogate s = makeSurrogate(db, DBObjectType.VALUE_TYPE, new MongoDBObjectId(id));
			return new ValueTypeImpl<T>(name, restricted, type, valueMap, s);
		} catch (ClassCastException e) {
			throw T2DBMMsg.exception(e, J.J81010, obj.toString());
		}
	}

}
