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
 * Type: WriteMethodsForChroniclesAndSeries
 * Version: 1.0.1
 */
package ch.agent.crnickl.mongodb;

import java.util.HashMap;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.DBObjectId;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.impl.ChronicleUpdatePolicy;
import ch.agent.crnickl.impl.Permission;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;

/**
 * A stateless object with methods providing write access to chronicles and
 * series.
 * To keep things as simple as possible in a first version,
 * chronicles and series values are stored separately, as in relational 
 * implementations. Changing this would require a non-negligible part of the
 * implementation.
 * 
 */
public class WriteMethodsForChroniclesAndSeries extends	ReadMethodsForChroniclesAndSeries {

	public WriteMethodsForChroniclesAndSeries() {
	}

	/**
	 * Create a chronicle in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @throws T2DBException
	 */
	public void createChronicle(Chronicle chronicle) throws T2DBException {
		createChronicle(chronicle, false);
	}
	private void createChronicle(Chronicle chronicle, boolean undoDelete) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			String name = chronicle.getName(false);
			chronicle.getSurrogate().getDatabase().getNamingPolicy().checkSimpleName(name, false); // defensive
			String description = chronicle.getDescription(false);
			if (description == null)
				description = "";
			Chronicle collection = chronicle.getCollection();
			Schema schema = chronicle.getSchema(false);
			if (collection != null) {
				check(Permission.MODIFY, collection);
				if (schema != null && schema.equals(collection.getSchema(true)))
					schema = null; // don't repeat yourself
			}
			if (schema != null)
				check(Permission.READ, schema);
			DBObjectId ox = insert(chronicle);
			surrogate = makeSurrogate(chronicle, ox);
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E40109, chronicle.getName(true));
		if (!undoDelete)
			chronicle.getSurrogate().upgrade(surrogate);
	}
	
	/**
	 * Delete a chronicle from the database. Also delete its attribute values
	 * and possibly other dependent objects. The chronicle update policy is
	 * supposed to forbid deleting when there are dependent chronicles or
	 * series, but to allow cascading delete of attribute values. Throw an
	 * exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @param policy a chronicle updating policy
	 * @throws T2DBException
	 */
	public void deleteChronicle(UpdatableChronicle chronicle, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = chronicle.getSurrogate();
		MongoDatabase db = (MongoDatabase) s.getDatabase();
		try {
			check(Permission.MODIFY, chronicle);
			policy.willDelete(chronicle);
			Chronicle original = getChronicle(s);
			getMongoDB(s).getChronicles().remove(asQuery(s.getId()), WriteConcern.SAFE);
			db.sleep();
			try {
				policy.willDelete(chronicle);
			} catch (T2DBException e) {
				// Oops! referential integrity broken!
				createChronicle(original, true);
				throw e;
			}
			deleteAttributes(chronicle);
			done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (!done || cause != null) {
			throw T2DBMsg.exception(cause, E.E40110, chronicle.getName(true));
		}
	}
	
	/**
	 * Update a chronicle. Currently only name and description can be updated.
	 * The schema and the parent chronicle cannot be updated. Throw an exception
	 * if the operation cannot be done.
	 * 
	 * @param chronicle
	 *            a chronicle
	 * @param policy
	 *            a chronicle updating policy
	 * @throws T2DBException
	 */
	public void updateChronicle(UpdatableChronicle chronicle, ChronicleUpdatePolicy policy) throws T2DBException {
		String name = chronicle.getName(false);
		String description = chronicle.getDescription(false);
		if (description == null)
			description = "";
		try {
			check(Permission.MODIFY, chronicle);
			policy.willUpdate(chronicle);
			getMongoDB(chronicle.getSurrogate()).getChronicles().update(
					mongoObject(MongoDatabase.FLD_ID, getId(chronicle)), 
					mongoObject(
							MongoDatabase.FLD_CHRON_NAME, name, 
							MongoDatabase.FLD_CHRON_DESC, description),
					true, false);
		} catch (Exception e) {
			throw T2DBMsg.exception(E.E40111, chronicle.getName(true));
		}
	}
	
	/**
	 * Update a chronicle attribute.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @param def an attribute definition
	 * @param value a value 
	 * @param description a string
	 * @throws T2DBException
	 */
	public void updateAttribute(UpdatableChronicle chronicle, AttributeDefinition<?> def, String value, String description) throws T2DBException {
		try {
			check(Permission.MODIFY, chronicle);
			getMongoDB(chronicle.getSurrogate()).getAttributes().update(
					mongoObject(MongoDatabase.FLD_ATTR_CHRON, getId(chronicle), 
							MongoDatabase.FLD_ATTR_PROP, getId(def.getProperty())), 
					mongoObject(
							MongoDatabase.FLD_ATTR_CHRON, getId(chronicle), 
							MongoDatabase.FLD_ATTR_PROP, getId(def.getProperty()),
							MongoDatabase.FLD_ATTR_VALUE, value,
							MongoDatabase.FLD_ATTR_DESC, description),
					true, false);
		} catch (Exception e) {
			throw T2DBMsg.exception(E.E40112, chronicle.getName(true), def.getNumber());
		}
	}
	
	/**
	 * Delete an attribute value from a chronicle.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @param def an attribute definition
	 * @throws T2DBException
	 */
	public void deleteAttribute(Chronicle chronicle, AttributeDefinition<?> def) throws T2DBException {
		try {
			check(Permission.MODIFY, chronicle);
			getMongoDB(chronicle.getSurrogate()).getAttributes().remove(
					mongoObject(MongoDatabase.FLD_ATTR_CHRON, getId(chronicle), 
							MongoDatabase.FLD_ATTR_PROP, getId(def.getProperty())), 
							WriteConcern.SAFE);
		} catch (Exception e) {
			throw T2DBMsg.exception(E.E40114, chronicle.getName(true), def.getNumber());
		}
	}
	
	/**
	 * Delete all attribute values of a chronicle.
	 * Return the list of attributes, in case they need to be recreated.
	 * 
	 * @param chronicle
	 * @throws T2DBException
	 */
	private void deleteAttributes(Chronicle chronicle) throws T2DBException {
		Surrogate s = chronicle.getSurrogate();
		Database db = s.getDatabase();
		try {
			getMongoDB(db).getAttributes().remove(
					mongoObject(MongoDatabase.FLD_ATTR_CHRON,getId(s)));
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E40124, chronicle.getName(true));
		}
	}

	/**
	 * Create an empty series. Throw an exception if the operation cannot be
	 * done.
	 * 
	 * @param series
	 *            a series
	 * @throws T2DBException
	 */
	public void createSeries(Series<?> series) throws T2DBException {
		createSeries(series, false);
	}
	private void createSeries(Series<?> series, boolean undoDelete) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, series.getChronicle());
			DBObjectId ox = insert(series);
			surrogate = makeSurrogate(series, ox);
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E50111, series.getName(true));
		if (!undoDelete)
			series.getSurrogate().upgrade(surrogate);
	}
	
	/**
	 * Delete a series. The policy typically forbids to delete a series which is
	 * not empty. Throw an exception if the operation cannot be done.
	 * 
	 * @param series
	 *            a series
	 * @param policy
	 *            a chronicle update policy
	 * @throws T2DBException
	 */
	public void deleteSeries(UpdatableSeries<?> series, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = series.getSurrogate();
		MongoDatabase db = (MongoDatabase) s.getDatabase();
		try {
			check(Permission.MODIFY, series);
			policy.willDelete(series);
			done = policy.deleteSeries(series);
			Series<?> original = db.getReadMethodsForChronicleAndSeries().getSeries(s);
			getMongoDB(db).getSeries().remove(asQuery(s.getId()), WriteConcern.SAFE);
			db.sleep();
			try {
				policy.willDelete(series);
			} catch (T2DBException e) {
				// Oops! referential integrity broken!
				createSeries(original, true);
				throw e;
			}
			done = true;
		} catch (Exception e) {
			cause = e;
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E50112, series.getName(true));
	}
	
	private DBObjectId insert(Chronicle chronicle) throws T2DBException {
		com.mongodb.BasicDBObject bo = new BasicDBObject();
		Surrogate s = chronicle.getSurrogate();
		if (!s.inConstruction()) 
			bo.put(MongoDatabase.FLD_ID, getId(chronicle)); // use case: re-creating
		bo.put(MongoDatabase.FLD_CHRON_NAME, chronicle.getName(false));
		bo.put(MongoDatabase.FLD_CHRON_DESC, chronicle.getDescription(false));
		bo.put(MongoDatabase.FLD_CHRON_PARENT, getIdOrZero(chronicle.getCollection()));
		bo.put(MongoDatabase.FLD_CHRON_SCHEMA, getIdOrZero(chronicle.getSchema(false)));
		getMongoDB(s).getChronicles().insert(bo);
		ObjectId ox = getObjectId(bo);	
		return new MongoDBObjectId(ox);
	}

	private <T>DBObjectId insert(Series<T> series) throws T2DBException {
		com.mongodb.BasicDBObject bo = new BasicDBObject();
		Surrogate s = series.getSurrogate();
		if (!s.inConstruction()) 
			bo.put(MongoDatabase.FLD_ID, getId(series)); // use case: re-creating
		bo.put(MongoDatabase.FLD_SER_CHRON, getIdOrZero(series.getChronicle()));
		bo.put(MongoDatabase.FLD_SER_NUM, series.getNumber());
		bo.put(MongoDatabase.FLD_SER_FIRST, 1);
		bo.put(MongoDatabase.FLD_SER_LAST, 0);
		bo.put(MongoDatabase.FLD_SER_VALUES, new HashMap<String, DBObjectId>());
		getMongoDB(s).getSeries().insert(bo);
		ObjectId ox = getObjectId(bo);	
		return new MongoDBObjectId(ox);
	}

}
