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

import java.util.Collection;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.DBObjectId;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.SeriesDefinition;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SchemaUpdatePolicy;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * A stateless object with methods providing write access to schemas.
 * <p>
 * A schema in MongoDB is stored as: <blockquote>
 * 
 * <pre>
 * <code>
 * { _id : OID,
 *   name : STRING, 
 *   base? : OID,
 *   attribs : [
 *     {
 *       number : NUMBER,
 *       erasing? : BOOLEAN
 *       prop? : OID,
 *       value? : STRING 
 *     }, ... 
 *   ]
 *   series : [
 *     {
 *       number : NUMBER,
 *       erasing? : BOOLEAN
 *       description? : STRING, 
 *       attribs : [
 *         ...
 *       ]
 *     }, ...
 *   ] 
 * }
 * </code>
 * </pre>
 * </blockquote> 
 * 
 * Note that attribute and series definitions are not keyed,
 * because they do not always have a name (erasing). The position inside the
 * array is independent of number. When erasing is present, it is true, and all
 * other fields except number are absent. When erasing is absent, the presence
 * of other fields denotes overriding behavior. When present, the <em>base</em>
 * field identifies the base schema. The attribs and series arrays can be empty
 * but are present.
 * 
 * @author Jean-Paul Vetterli
 */
public class WriteMethodsForSchema extends ReadMethodsForSchema {

	public WriteMethodsForSchema() {
	}
	
	/**
	 * Create an empty schema in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @throws T2DBException
	 */
	public void createSchema(UpdatableSchema schema) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.CREATE, schema);
			Schema base = schema.getBase();
			if (base != null)
				check(Permission.READ, base);
			DBObjectId ox = insert(schema);
			surrogate = makeSurrogate(schema, ox);
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E30122, schema.getName());
		schema.getSurrogate().upgrade(surrogate); 
	}
	
	/**
	 * Delete a schema from the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param policy a schema udpdating policy
	 * @throws T2DBException
	 */
	public void deleteSchema(UpdatableSchema schema, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = schema.getSurrogate();
		MongoDatabase database = (MongoDatabase) s.getDatabase();
		try {
			check(Permission.MODIFY, schema);
			policy.willDelete(schema);
			UpdatableSchema original = database.getReadMethodsForSchema().getSchema(s);
			DBCollection coll = getMongoDB(s).getSchemas();
			coll.remove(asQuery(s.getId()), WriteConcern.SAFE);
			database.sleep();
			try {
				policy.willDelete(schema);
			} catch (T2DBException e) {
				// Oops! referential integrity broken!
				createSchema(original);
				throw e;
			}
			done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (!done || cause != null) {
			throw T2DBMsg.exception(cause, E.E30123, schema.getName());
		}
	}

	/**
	 * Update the basic schema setup in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param policy 
	 * @return true if the schema was updated
	 * @throws T2DBException
	 */
	public boolean updateSchema(UpdatableSchema schema, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		Surrogate s = schema.getSurrogate();
		MongoDatabase database = (MongoDatabase) s.getDatabase();
		try {
			check(Permission.MODIFY, schema);
			UpdatableSchema original = database.getReadMethodsForSchema().getSchema(s);
			Schema base = schema.getBase();
			if (base != null && !base.equals(original.getBase()))
				check(Permission.READ, base);
			DBCollection coll = getMongoDB(s).getSchemas();
			// full replace (no need to set _id in full update) 
			com.mongodb.DBObject operation = mongoObject(
					MongoDatabase.FLD_SCHEMA_NAME, schema.getName(),
					MongoDatabase.FLD_SCHEMA_BASE, getIdOrZero(schema.getBase()),
					MongoDatabase.FLD_SCHEMA_ATTRIBS, 
					attributeDefinitions(schema.getAttributeDefinitions()),
					MongoDatabase.FLD_SCHEMA_SERIES, 
					seriesDefinitions(schema.getSeriesDefinitions()));
			coll.update(asQuery(s.getId()), operation, false, false, WriteConcern.SAFE);
			database.sleep();
			try {
				policy.willUpdate(schema);
			} catch (T2DBException e) {
				// Oops! referential integrity broken!
				operation = mongoObject(
						MongoDatabase.FLD_SCHEMA_NAME, original.getName(),
						MongoDatabase.FLD_SCHEMA_BASE, getId(original.getBase()),
						MongoDatabase.FLD_SCHEMA_ATTRIBS, original.getAttributeDefinitions(),
						MongoDatabase.FLD_SCHEMA_SERIES, original.getSeriesDefinitions());
				coll.update(asQuery(s.getId()), operation);
				throw e;
			}
			done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
		}
		if (cause != null)
			throw T2DBMsg.exception(cause, E.E30122, schema.getName());

		return done;
	}
	
	/**
	 * Find a chronicle referencing one of the schemas. 
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param schema a schema
	 * @return a surrogate or null
	 * @throws T2DBException
	 */
	public Surrogate findChronicle(Schema schema) throws T2DBException {
		Surrogate result = null;
		try {
			Database db = schema.getDatabase();
			DBObject query = new BasicDBObject();
			query.put(MongoDatabase.FLD_CHRON_SCHEMA, getId(schema));
			DBObject obj = getMongoDB(db).getChronicles().findOne(query);
			if (obj != null)
				result = makeSurrogate(db, DBObjectType.CHRONICLE, getObjectId((BasicDBObject)obj));
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30117);
		}
		return result;
	}
	
	/**
	 * Find a chronicle with an explicit attribute value for a given property and schemas. 
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param property a property
	 * @param schema a schema
	 * @return a surrogate or null
	 * @throws T2DBException
	 */
	public Surrogate findChronicle(Property<?> property, Schema schema) throws T2DBException {
		Surrogate result = null;
		DBCursor cursor = null;	
		try {
			Database db = schema.getDatabase();
			cursor = getMongoDB(db).getAttributes().find(mongoObject(
					MongoDatabase.FLD_ATTR_PROP, getId(property)));
			while (cursor.hasNext()) {
				ObjectId chrOid = ((BasicDBObject) cursor.next()).getObjectId(MongoDatabase.FLD_ATTR_CHRON);
				Surrogate entityKey = makeSurrogate(db, DBObjectType.CHRONICLE, chrOid);
				Schema s = db.getChronicle(entityKey).getSchema(true);
				if (s.dependsOnSchema(schema)) {
					result = entityKey;
					break;
				}
			}
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30117);
		} finally {
			if (cursor != null)
				cursor.close();
		}
		return result;
	}

	/**
	 * Find a chronicle with a given series in a collection of schemas.
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param ss a series definition
	 * @param schema a schema
	 * @return a surrogate or null
	 * @throws T2DBException
	 */
	public Surrogate findChronicle(SeriesDefinition ss, Schema schema) throws T2DBException {
		Surrogate result = null;
		DBCursor cursor1 = null;	
		DBCursor cursor2 = null;	
		try {
			Database db = schema.getDatabase();
			cursor1 = getMongoDB(db).getChronicles().find(mongoObject(
					MongoDatabase.FLD_CHRON_SCHEMA, getId(schema)));
			OUTER: while (cursor1.hasNext()) {
				ObjectId chronicleOid = getObjectId((BasicDBObject) cursor1.next());
				cursor2 = getMongoDB(db).getSeries().find(mongoObject(
						MongoDatabase.FLD_SER_CHRON, chronicleOid,
						MongoDatabase.FLD_SER_NUM, ss.getNumber()));
				while (cursor2.hasNext()) {
					Surrogate entityKey = makeSurrogate(db, DBObjectType.CHRONICLE, chronicleOid);
					Schema s = db.getChronicle(entityKey).getSchema(true);
					if (s.dependsOnSchema(schema)) {
						result = entityKey;
						break OUTER;
					}
				}
			}
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30117);
		} finally {
			if (cursor1 != null)
				cursor1.close();
			if (cursor2 != null)
				cursor2.close();
		}
		return result;
	}	
	
	private <T>DBObjectId insert(UpdatableSchema schema) throws T2DBException {
		com.mongodb.BasicDBObject bo = new BasicDBObject();
		Surrogate s = schema.getSurrogate();
		if (!s.inConstruction())
			bo.put(MongoDatabase.FLD_ID, getId(schema));
		bo.put(MongoDatabase.FLD_SCHEMA_NAME, schema.getName());
		bo.put(MongoDatabase.FLD_SCHEMA_BASE, getIdOrZero(schema.getBase()));
		bo.put(MongoDatabase.FLD_SCHEMA_ATTRIBS, attributeDefinitions(schema.getAttributeDefinitions()));
		bo.put(MongoDatabase.FLD_SCHEMA_SERIES, seriesDefinitions(schema.getSeriesDefinitions()));
		getMongoDB(s).getSchemas().insert(bo);
		ObjectId ox = getObjectId(bo);	
		return new MongoDBObjectId(ox);
	}

	private BasicDBList attributeDefinitions(
			Collection<AttributeDefinition<?>> defs) throws T2DBException {
		BasicDBList list = new BasicDBList();
		if (defs != null) {
			int i = 0;
			for (AttributeDefinition<?> def : defs) {
				list.put(i++, attributeDefinition(def));
			}
		}
		return list;
	}

	private BasicDBObject attributeDefinition(AttributeDefinition<?> def) throws T2DBException {
		com.mongodb.BasicDBObject bo = new BasicDBObject();
		bo.put(MongoDatabase.FLD_ATTRIBDEF_NUM, def.getNumber());
		bo.put(MongoDatabase.FLD_ATTRIBDEF_ERASING, def.isErasing());
		bo.put(MongoDatabase.FLD_ATTRIBDEF_PROP, def.isErasing() ? null : getId(def.getProperty()));
		bo.put(MongoDatabase.FLD_ATTRIBDEF_VAL, def.isErasing() ? null : 
			def.getProperty().getValueType().toString(def.getValue()));
		return bo;
	}
	
	private BasicDBList seriesDefinitions(Collection<SeriesDefinition> defs) throws T2DBException {
		BasicDBList list = new BasicDBList();
		if (defs != null) {
			int i = 0;
			for (SeriesDefinition def : defs) {
				list.put(i++, seriesDefinition(def));
			}
		}
		return list;
	}
	
	private BasicDBObject seriesDefinition(SeriesDefinition def) throws T2DBException {
		com.mongodb.BasicDBObject bo = new BasicDBObject();
		bo.put(MongoDatabase.FLD_SERIESDEF_NUM, def.getNumber());
		bo.put(MongoDatabase.FLD_SERIESDEF_ERASING, def.isErasing());
		bo.put(MongoDatabase.FLD_SERIESDEF_DESC, def.isErasing() ? null : def.getDescription());
		bo.put(MongoDatabase.FLD_SERIESDEF_ATTRIBS, def.isErasing() ? attributeDefinitions(null) : 
			attributeDefinitions(def.getAttributeDefinitions()));
		return bo;
	}
	
}
