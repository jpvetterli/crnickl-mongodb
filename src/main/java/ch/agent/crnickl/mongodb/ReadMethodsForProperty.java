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
 * Type: ReadMethodsForProperty
 * Version: 1.0.0
 */
package ch.agent.crnickl.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.PropertyImpl;
import ch.agent.crnickl.mongodb.T2DBMMsg.J;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * A stateless object with methods providing read access to properties.
 *  
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class ReadMethodsForProperty extends MongoDatabaseMethods {

	public ReadMethodsForProperty() {
	}

	/**
	 * Find a property by its name in a database.
	 * 
	 * @param database a database 
	 * @param name a string 
	 * @return a property or null
	 * @throws T2DBException
	 */
	public <T>Property<T> getProperty(Database database, String name) throws T2DBException {
		try {
			DBCollection coll = getMongoDB(database).getProperties();
			DBObject obj = coll.findOne(mongoObject(MongoDatabase.FLD_PROP_NAME, name));
			if (obj != null)
				return unpack(database, (BasicDBObject) obj);
			else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E20104, name);
		}
	}
	
	/**
	 * Find a collection of properties with names matching a pattern.
	 * If the pattern is enclosed in slashes it is taken as a standard
	 * regexp pattern; the slashes will be removed. If it is not enclosed
	 * in slashes, it is taken as a minimal pattern and all occurrences of
	 * "*" will be replaced with ".*" (zero or more arbitrary characters). 
	 * 
	 * @param database a database
	 * @param pattern a simple pattern or a regexp pattern
	 * @return a collection of properties, possibly empty, never null
	 * @throws T2DBException
	 */
	public Collection<Property<?>> getProperties(Database database, String pattern) throws T2DBException {
		try {
			DBCollection coll = getMongoDB(database).getProperties();
			DBObject query = null;
			if (pattern != null && pattern.length() > 0) {
				String regexp = extractRegexp(pattern);
				if (regexp == null)
					pattern = pattern.replace("*", ".*");
				else
					pattern = regexp;
				query = mongoObject(MongoDatabase.FLD_PROP_NAME, Pattern.compile(pattern));
			}
			DBCursor cursor = coll.find(query);
			Collection<Property<?>> result = new ArrayList<Property<?>>();
			try {
				while (cursor.hasNext()) {
					result.add(unpack(database, (BasicDBObject) cursor.next()));
				}
			} finally {
				cursor.close();
			}
			return result;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E20106, pattern);
		}
	}
	
	/**
	 * Find a property corresponding to a surrogate.
	 * 
	 * @param s a surrogate
	 * @return a property or null
	 * @throws T2DBException
	 */
	public <T>Property<T> getProperty(Surrogate s) throws T2DBException {
		try {
			DBObject obj = getObject(s, false);
			if (obj != null)
				return unpack(s.getDatabase(), (BasicDBObject) obj);
			else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E20105, s.toString());
		}
	}
	
	private <T>Property<T> unpack(Database db, BasicDBObject obj) throws T2DBException {
		try {
			ObjectId id = obj.getObjectId(MongoDatabase.FLD_ID);
			String name = obj.getString(MongoDatabase.FLD_PROP_NAME);
			ObjectId vtId = obj.getObjectId(MongoDatabase.FLD_PROP_VT);
			Boolean indexed = obj.getBoolean(MongoDatabase.FLD_PROP_INDEXED);
			Surrogate vts = makeSurrogate(db, DBObjectType.VALUE_TYPE, new MongoDBObjectId(vtId));
			ValueType<T> vt = db.getValueType(vts);
			Surrogate s = makeSurrogate(db, DBObjectType.PROPERTY, new MongoDBObjectId(id));
			checkIntegrity(vt, vts, s);
			return new PropertyImpl<T>(name, vt, indexed, s);
		} catch (ClassCastException e) {
			throw T2DBMMsg.exception(e, J.J81010, obj.toString());
		}
	}

	
}
