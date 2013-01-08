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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.impl.ChronicleImpl;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SeriesImpl;
import ch.agent.crnickl.mongodb.T2DBMMsg.J;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * A stateless object with methods providing read access to chronicles and
 * series.
 * 
 * @author Jean-Paul Vetterli
 */
public class ReadMethodsForChroniclesAndSeries extends MongoDatabaseMethods {
	
	public ReadMethodsForChroniclesAndSeries() {
	}

	/**
	 * Find a chronicle corresponding to a surrogate. An
	 * exception is thrown if there is no such chronicle.
	 * 
	 * @param surrogate a surrogate
	 * @return a chronicle, never null
	 * @throws T2DBException
	 */
	public Chronicle getChronicle(Surrogate s) throws T2DBException {
		DBObject obj = getObject(s, false);
		Throwable cause = null;
		if (obj != null)
			try {
				Chronicle chronicle = unpack(s.getDatabase(), (BasicDBObject) obj);
				check(Permission.READ, chronicle);
				return chronicle;
			} catch (Exception e) {
				cause = e;
			}
		throw T2DBMsg.exception(cause, E.E40104, s.toString());
	}
	
	/**
	 * Find a chronicle with a given parent and name.
	 * 
	 * @param parent a chronicle
	 * @param name a string 
	 * @return a chronicle or null
	 * @throws T2DBException
	 */
	public Chronicle getChronicleOrNull(Chronicle parent, String name) throws T2DBException {
		try {
			Database database = parent.getDatabase();
			DBObject obj = getMongoDB(database).getChronicles().findOne(
					mongoObject(MongoDatabase.FLD_CHRON_PARENT, getIdOrZero(parent), 
					MongoDatabase.FLD_CHRON_NAME, name));
			if (obj != null) {
				Chronicle chronicle = unpack(database, (BasicDBObject) obj);
				check(Permission.READ, chronicle);
				return chronicle;
			} else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E40123, name, parent.getName(true));
		}
	}
	
	public Collection<Chronicle> getChroniclesByParent(Chronicle parent) throws T2DBException {
		Collection<Chronicle> result = new ArrayList<Chronicle>();
		if (check(Permission.DISCOVER, parent, false)) {
			try {
				Database db = parent.getDatabase();
				DBCursor cursor = getMongoDB(db).getChronicles().find(
						mongoObject(MongoDatabase.FLD_CHRON_PARENT, getIdOrZero(parent)));
				try {
					while (cursor.hasNext()) {
						Chronicle chronicle = unpack(db, (BasicDBObject) cursor.next());
						check(Permission.READ, chronicle);
						result.add(chronicle);
					}
				} finally {
					cursor.close();
				}
			} catch (Exception e) {
				throw T2DBMsg.exception(e, E.E40122, parent.getName(true));
			}
		}
		return result;
	}
	
	/**
	 * The method completes the attribute with the value found for one of the
	 * entities in the list and returns true. If no value was found, the method
	 * return false. If more than value was found, the one selected corresponds
	 * to the first entity encountered in the list.
	 * 
	 * @param chronicles a list of chronicles
	 * @param attribute an attribute 
	 * @return true if any value found
	 * @throws T2DBException
	 */
	public boolean getAttributeValue(List<Chronicle> chronicles, Attribute<?> attribute) throws T2DBException {
		boolean found = false;
		Surrogate s = attribute.getProperty().getSurrogate();
		Database db = s.getDatabase();
		try {
			ObjectId[] chrOids = new ObjectId[chronicles.size()];
			int offset = 0;
			for(Chronicle chronicle : chronicles) {
				chrOids[offset++] = getId(chronicle);
			}
			DBCursor cursor = getMongoDB(db).getAttributes().find(
					mongoObject(MongoDatabase.FLD_ATTR_PROP,
							getId(s), MongoDatabase.FLD_ATTR_CHRON,
							mongoObject(Operator.IN.op(), chrOids)));
			
			offset = Integer.MAX_VALUE;
			BasicDBObject objAtOffset = null;
			
			while (cursor.hasNext()) {
				BasicDBObject obj = (BasicDBObject) cursor.next();
				ObjectId chrOid = obj.getObjectId(MongoDatabase.FLD_ATTR_CHRON);
				int offset1 = findOffset(chrOid, chronicles);
				if (offset1 < offset) {
					offset = offset1;
					objAtOffset = obj;
				}
				if (offset == 0)
					break;
			}

            if (objAtOffset != null) {
            	ObjectId chrOid = objAtOffset.getObjectId(MongoDatabase.FLD_ATTR_CHRON);
            	Surrogate chronicle = makeSurrogate(db, DBObjectType.CHRONICLE, chrOid);
            	check(Permission.READ, chronicle);
            	attribute.scan(objAtOffset.getString(MongoDatabase.FLD_ATTR_VALUE));
				String description = objAtOffset.getString(MongoDatabase.FLD_ATTR_DESC);
				if (description.length() > 0)
					attribute.setDescription(description);
            	found = true;
            }
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E40120, attribute.getProperty().getName());
		}
		return found;
	}
	
	/**
	 * Return a list of chronicles with a given value for a given property.
	 * 
	 * @param property a property
	 * @param value a value
	 * @param maxSize the maximum size of the result or 0 for no limit
	 * @return a list of chronicles, possibly empty, never null
	 * @throws T2DBException
	 */
	public <T>List<Chronicle> getChroniclesByAttributeValue(Property<T> property, T value, int maxSize) throws T2DBException {
		List<Chronicle> result = new ArrayList<Chronicle>();
		Surrogate s = property.getSurrogate();
		Database db = s.getDatabase();
		try {
			DBCursor cursor = getMongoDB(db).getAttributes().find(
					mongoObject(MongoDatabase.FLD_ATTR_PROP,
							getId(s), MongoDatabase.FLD_ATTR_VALUE, 
							property.getValueType().toString(value))); 
			while (cursor.hasNext()) {
				BasicDBObject obj = (BasicDBObject) cursor.next();
				ObjectId chrOid = obj.getObjectId(MongoDatabase.FLD_ATTR_CHRON);
				Surrogate chronicle = makeSurrogate(db, DBObjectType.CHRONICLE, chrOid);
				try {
					check(Permission.READ, chronicle);
					result.add(new ChronicleImpl(chronicle));
				} catch (T2DBException e) {
					// ignore "unreadable" chronicles
				}
			}
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E40119, property.getName(), value);
		}
		return result;
	}
	
	/**
	 * Find a series corresponding to a surrogate.
	 * 
	 * @param surrogate a surrogate
	 * @return a series or null
	 * @throws T2DBException
	 */
	public <T>Series<T> getSeries(Surrogate s) throws T2DBException {
		BasicDBObject obj = (BasicDBObject) getObject(s, false);
		Throwable cause = null;
		if (obj != null)
			try {
				ObjectId chrOid = obj.getObjectId(MongoDatabase.FLD_SER_CHRON);
				int serNum = obj.getInt(MongoDatabase.FLD_SER_NUM);
				Chronicle chronicle = new ChronicleImpl(
						makeSurrogate(s.getDatabase(), DBObjectType.CHRONICLE, chrOid));
				check(Permission.READ, chronicle);
				Series<T> series = new SeriesImpl<T>(chronicle, null, serNum, s);
				return series;
			} catch (Exception e) {
				cause = e;
			}
		throw T2DBMsg.exception(cause, E.E40104, s.toString());
	}
	
	/**
	 * Return array of series in the positions corresponding to the
	 * requested numbers. These numbers are series numbers within the schema,
	 * not the series keys in the database. When a requested number in the array is
	 * non-positive, the corresponding series will be null in the result. A
	 * series can also be null when the requested number is positive but the
	 * series has no data and has not been set up in the database (and therefore
	 * has no series database key).
	 * <p>
	 * Note. There is no exception if the chronicle does not exist. This behavior makes
	 * it easier to implement the wishful transaction protocol (i.e. testing again if it
	 * was okay to delete a chronicle after it was deleted). 
	 * 
	 * @param chronicle a chronicle
	 * @param names an array of simple names to plug into the series  
	 * @param numbers an array of numbers
	 * @return an array of series
	 * @throws T2DBException
	 */
	public <T>Series<T>[] getSeries(Chronicle chronicle, String[] names, int[] numbers) throws T2DBException {

		if (names.length != numbers.length)
			throw new IllegalArgumentException("lengths of names[] and numbers[] differ");

		@SuppressWarnings("unchecked")
		Series<T>[] result = new SeriesImpl[numbers.length];
		if (result.length > 0) {
			Map<Integer, Integer> map = new HashMap<Integer, Integer>();
			for (int i = 0; i < numbers.length; i++) {
				if (numbers[i] > 0) {
					if (map.put(numbers[i], i) != null)
						throw new IllegalArgumentException("duplicate number: " + numbers[i]);
				}
			}
			Surrogate s = chronicle.getSurrogate();
			Database db = s.getDatabase();
			try {
				check(Permission.READ, chronicle);
			} catch (Exception e) {
				throw T2DBMsg.exception(e, E.E40104, s.toString());
			}
			try {
				DBCursor cursor = getMongoDB(db).getSeries().find(
						mongoObject(MongoDatabase.FLD_SER_CHRON,
								getId(chronicle), MongoDatabase.FLD_SER_NUM,
								mongoObject(Operator.IN.op(), numbers)),
						mongoObject(MongoDatabase.FLD_SER_NUM, 1));
				while (cursor.hasNext()) {
					BasicDBObject obj = (BasicDBObject) cursor.next();
					ObjectId serOid = obj.getObjectId(MongoDatabase.FLD_ID);
					int serNumber = obj.getInt(MongoDatabase.FLD_SER_NUM);
					int i = map.get(serNumber);
					result[i] = new SeriesImpl<T>(chronicle,
							names[i], numbers[i],
							makeSurrogate(db, DBObjectType.SERIES, serOid));
				}
			} catch (Exception e) {
				throw T2DBMsg.exception(e, E.E40121, chronicle.getName(true));
			}
		}
		return result;
	}

	private Chronicle unpack(Database db, BasicDBObject obj) throws T2DBException {
		try {
			ObjectId id = obj.getObjectId(MongoDatabase.FLD_ID);
			Surrogate s = makeSurrogate(db, DBObjectType.CHRONICLE, id);
			ChronicleImpl.RawData data = new ChronicleImpl.RawData();
			data.setSurrogate(s);
			data.setName(obj.getString(MongoDatabase.FLD_CHRON_NAME));
			data.setDescription(obj.getString(MongoDatabase.FLD_CHRON_DESC));
			ObjectId parentId = obj.getObjectId(MongoDatabase.FLD_CHRON_PARENT);
			data.setCollection(parentId == null ? db.getTopChronicle() : 
				new ChronicleImpl(makeSurrogate(db, DBObjectType.CHRONICLE, parentId)));
			ObjectId schemaId = obj.getObjectId(MongoDatabase.FLD_CHRON_SCHEMA);
			data.setSchema(schemaId == null ? null : makeSurrogate(db, DBObjectType.SCHEMA, schemaId));
			Chronicle chronicle = new ChronicleImpl(data);
			return chronicle;
		} catch (ClassCastException e) {
			throw T2DBMMsg.exception(e, J.J81010, obj.toString());
		}
	}
	
	private int findOffset(ObjectId id, List<Chronicle> chronicles) {
		int found = -1;
		int offset = 0;
		for(Chronicle chronicle : chronicles) {
			if (getId(chronicle).equals(id)) {
				found = offset;
				break;
			}
			offset++;
		}
		if (found < 0)
			throw new RuntimeException("chronicle not found: " + id);
		return found;
	}

}
