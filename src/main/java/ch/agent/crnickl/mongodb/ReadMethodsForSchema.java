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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.SeriesDefinition;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.impl.AttributeDefinitionImpl;
import ch.agent.crnickl.impl.SeriesDefinitionImpl;
import ch.agent.crnickl.impl.UpdatableSchemaImpl;
import ch.agent.crnickl.mongodb.T2DBMMsg.J;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * A stateless object with methods providing read access to schemas.
 *  
 * @author Jean-Paul Vetterli
 */
public class ReadMethodsForSchema extends MongoDatabaseMethods {

	public ReadMethodsForSchema() {
	}
	
	/**
	 * Find a schema corresponding to a surrogate.
	 * 
	 * @param surrogate a surrogate
	 * @return a schema or null
	 * @throws T2DBException
	 */
	public UpdatableSchema getSchema(Surrogate surrogate) throws T2DBException {
		return getSchema(surrogate, null);
	}
	
	/**
	 * Find a collection of schema surrogates with labels matching a pattern.
	 * 
	 * @param db a database
	 * @param pattern a simple pattern where "*" stands for zero or more characters
	 * @return a collection of schema surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> getSchemaSurrogateList(Database db, String pattern) throws T2DBException {
		try {
			Collection<Surrogate> result = new ArrayList<Surrogate>();
			DBCollection coll = getMongoDB(db).getSchemas();
			DBObject query = null;
			if (pattern != null && pattern.length() > 0) {
				String regexp = extractRegexp(pattern);
				if (regexp == null) {
					regexp = pattern.replace("*", ".*");
					if (regexp.equals(pattern))
						regexp = null;
				}
				query = mongoObject(MongoDatabase.FLD_SCHEMA_NAME, 
						regexp == null ? pattern : Pattern.compile(regexp));
			}
			DBCursor cursor = coll.find(query);
			try {
				while (cursor.hasNext()) {
					ObjectId id = (ObjectId) cursor.next().get(MongoDatabase.FLD_ID);
					Surrogate s = makeSurrogate(db, DBObjectType.SCHEMA, new MongoDBObjectId(id));
					result.add(s);
				}
			} finally {
				cursor.close();
			}
			return result;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30105, pattern);
		}
	}		
	
	private UpdatableSchema getSchema(Surrogate s, Set<ObjectId> cycleDetector) throws T2DBException {
		try {
			DBObject obj = getObject(s, false);
			return obj == null ? null : unpack(s.getDatabase(), (BasicDBObject) obj, cycleDetector);
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30104, s.toString());
		}
	}
	
	private UpdatableSchema unpack(Database db, BasicDBObject obj, Set<ObjectId> cycleDetector) throws T2DBException {
		if (cycleDetector == null)
			cycleDetector = new HashSet<ObjectId>();
		try {
			ObjectId id = obj.getObjectId(MongoDatabase.FLD_ID);
			Surrogate s = makeSurrogate(db, DBObjectType.SCHEMA, new MongoDBObjectId(id));
			boolean cycleDetected = !cycleDetector.add(id);
			String name = obj.getString(MongoDatabase.FLD_SCHEMA_NAME);
			UpdatableSchema base = null;
			ObjectId baseId = obj.getObjectId(MongoDatabase.FLD_SCHEMA_BASE);
			if (baseId != null && !cycleDetected) {
				Surrogate baseSurr = makeSurrogate(db, DBObjectType.SCHEMA, new MongoDBObjectId(baseId));
				base = getSchema(baseSurr, cycleDetector);
			}
			Collection<AttributeDefinition<?>> attribs = attributeDefinitions(s, 0, db, (BasicDBList)obj.get(MongoDatabase.FLD_SCHEMA_ATTRIBS));
			Collection<SeriesDefinition> series = seriesDefinitions(s, db, (BasicDBList)obj.get(MongoDatabase.FLD_SCHEMA_SERIES));
			return new UpdatableSchemaImpl(name,  base,  attribs,  series,  s);
		} catch (ClassCastException e) {
			throw T2DBMMsg.exception(e, J.J81010, obj.toString());
		}
	}
	
	private Collection<AttributeDefinition<?>> attributeDefinitions(Surrogate schemaSurrogate, int seriesNr, Database db, BasicDBList list) throws T2DBException {
		Collection<AttributeDefinition<?>> result = new ArrayList<AttributeDefinition<?>>(list.size());
		for (int i = 0; i < list.size(); i++) {
			result.add(attributeDefinition(schemaSurrogate, seriesNr, db, (BasicDBObject)list.get(i)));
		}
		return result;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private AttributeDefinition<?> attributeDefinition(Surrogate schemaSurrogate, int seriesNr, Database db, BasicDBObject bo) throws T2DBException {
		int number = bo.getInt(MongoDatabase.FLD_ATTRIBDEF_NUM);
		boolean era = bo.getBoolean(MongoDatabase.FLD_ATTRIBDEF_ERASING);
		AttributeDefinitionImpl<?> def = null;
		if (era) {
			def = new AttributeDefinitionImpl(seriesNr, number, null, null);
			def.edit();
			def.setErasing(true);
		} else {
			ObjectId propId =  bo.getObjectId(MongoDatabase.FLD_ATTRIBDEF_PROP);
			Surrogate propSurr = makeSurrogate(db, DBObjectType.PROPERTY, new MongoDBObjectId(propId));
			Property<?> prop = ((MongoDatabase) db).getReadMethodsForProperty().getProperty(propSurr);
			String val =  bo.getString(MongoDatabase.FLD_ATTRIBDEF_VAL);
			checkIntegrity(prop, propSurr, schemaSurrogate);
			def = new AttributeDefinitionImpl(seriesNr, number, prop, prop.getValueType().scan(val));
		}
		return def;
	}
	
	private Collection<SeriesDefinition> seriesDefinitions(Surrogate schemaSurrogate, Database db, BasicDBList list) throws T2DBException {
		Collection<SeriesDefinition> result = new ArrayList<SeriesDefinition>(list.size());
		for (int i = 0; i < list.size(); i++) {
			result.add(seriesDefinition(schemaSurrogate, db, (BasicDBObject)list.get(i)));
		}
		return result;
	}
	
	private SeriesDefinition seriesDefinition(Surrogate schemaSurrogate, Database db, BasicDBObject bo) throws T2DBException {
		int number = bo.getInt(MongoDatabase.FLD_SERIESDEF_NUM);
		boolean era = bo.getBoolean(MongoDatabase.FLD_SERIESDEF_ERASING);
		SeriesDefinitionImpl def = null;
		if (era) {
			def = new SeriesDefinitionImpl(number, null, null);
			def.edit();
			def.setErasing(true);
		} else {
			String desc =  bo.getString(MongoDatabase.FLD_SERIESDEF_DESC);
			BasicDBList list = (BasicDBList)bo.get(MongoDatabase.FLD_SERIESDEF_ATTRIBS);
			Collection<AttributeDefinition<?>> attribs = attributeDefinitions(schemaSurrogate, number, db, list);
			def = new SeriesDefinitionImpl(number, desc, attribs);
		}
		return def;
	}

}