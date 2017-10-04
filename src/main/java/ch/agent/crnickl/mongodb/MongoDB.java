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
 * 
 */
package ch.agent.crnickl.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.DatabaseConfiguration;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.mongodb.T2DBMMsg.J;
import ch.agent.t2.applied.DateTime;
import ch.agent.t2.applied.Month;
import ch.agent.t2.applied.Workday;
import ch.agent.t2.applied.Year;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.TimeDomain;

/**
 * MongoDB is a singleton encapsulating the MongoDB connection and the 
 * base collections used by CrNiCKL.
 * 
 * @author Jean-Paul Vetterli
 */
public class MongoDB {

	private static class Singleton {
		private static MongoDB mongo_connection;
		static {
			mongo_connection = new MongoDB();
		};
	}

	private enum WriteConcernKeyword {
		NORMAL, SAFE, MAJORITY, FSYNC_SAFE, JOURNAL_SAFE, REPLICAS_SAFE
	}
	
	private DatabaseConfiguration configuration;
	private Mongo connection = null;
	private DBCollection properties;
	private DBCollection valueTypes;
	private DBCollection schemas;
	private DBCollection chronicles;
	private DBCollection series;
	private DBCollection attributes;
	private String user;
	
	public static final String MONGODB_HOST = "mongodb.host";
	public static final String MONGODB_PORT = "mongodb.port";
	public static final String MONGODB_DB = "mongodb.db";
	public static final String MONGODB_WRITE_CONCERN = "mongodb.writeConcern";
	public static final String MONGODB_USER = "mongodb.user";
	public static final String MONGODB_PASSWORD = "mongodb.password";

	private MongoDB() {
	}

	/**
	 * Construct a MongoDB session. This constructor can be invoked only once.
	 * 
	 * @param configuration a database configuration
	 */
	public MongoDB(Database database, DatabaseConfiguration configuration) throws T2DBException {
		if (Singleton.mongo_connection.configuration != null)
			throw new IllegalStateException("already initialized");
		Singleton.mongo_connection.configuration = configuration;
		Singleton.mongo_connection.open(database);
	}
	
	/**
	 * Return the MongoDB connection.
	 * 
	 * @return the MongoDB connection
	 */
	public static MongoDB getInstance() {
		if (Singleton.mongo_connection.configuration == null)
			throw new IllegalStateException("not initialized");
		return Singleton.mongo_connection;
	}
	
	private void open(Database database) throws T2DBException {
		try {
			String host = configuration.getParameter(MONGODB_HOST, false);
			String port = configuration.getParameter(MONGODB_PORT, false);
			
			if (host != null) {
				if (port != null) {
					connection = new MongoClient(host, Integer.parseInt(port));
				} else {
					connection = new MongoClient(host);
				}
			} else {
				connection = new MongoClient();
			}
			
			initialize(connection, database);
			
			// TODO: implement secure mode
			user = configuration.getParameter(MONGODB_USER, true);
			configuration.getParameter(MONGODB_PASSWORD, true);
			configuration.setParameter(MONGODB_USER, "zzzzz");
			configuration.setParameter(MONGODB_PASSWORD, "zzzzz");
		} catch (Exception e) {
			throw T2DBMMsg.exception(e, J.J80050, toString());
		}
	}
	
	private WriteConcern getWriteConcernFromKeyword(String keyword) throws T2DBException  {
		WriteConcern wc = null;
		if (keyword == null)
			keyword = "SAFE";
		WriteConcernKeyword k = null;
		try {
			k = WriteConcernKeyword.valueOf(keyword);
		} catch (IllegalArgumentException e) {
			throw T2DBMMsg.exception(e, J.J81020, keyword);
		}
		switch (k) {
		case NORMAL:
			wc =  WriteConcern.NORMAL;
			break;
		case SAFE:
			wc =  WriteConcern.SAFE;
			break;
		case MAJORITY:
			wc =  WriteConcern.MAJORITY;
			break;
		case FSYNC_SAFE:
			wc =  WriteConcern.FSYNC_SAFE;
			break;
		case JOURNAL_SAFE:
			wc =  WriteConcern.JOURNAL_SAFE;
			break;
		case REPLICAS_SAFE:
			wc =  WriteConcern.REPLICAS_SAFE;
			break;
		default:
			throw new RuntimeException("bug: " + k.name());
		}
		if (wc != WriteConcern.SAFE)
			throw T2DBMMsg.exception(J.J81021, keyword);
		return wc;
	}
	
	private void initialize(Mongo mongo, Database database) throws T2DBException {
		DB db = mongo.getDB(configuration.getParameter(MONGODB_DB, true));
		db.setWriteConcern(getWriteConcernFromKeyword(
				configuration.getParameter(MONGODB_WRITE_CONCERN, false)));
		if (!db.collectionExists(MongoDatabase.COLL_VT)) {
			valueTypes = createCollection(db, MongoDatabase.COLL_VT, MongoDatabase.FLD_VT_NAME);
			properties = createCollection(db, MongoDatabase.COLL_PROP, MongoDatabase.FLD_PROP_NAME);
			schemas = createCollection(db, MongoDatabase.COLL_SCHEMA, MongoDatabase.FLD_SCHEMA_NAME);
			chronicles = createCollection(db, MongoDatabase.COLL_CHRON,
					MongoDatabase.FLD_CHRON_PARENT, MongoDatabase.FLD_CHRON_NAME);
			series = createCollection(db, MongoDatabase.COLL_SER,
					MongoDatabase.FLD_SER_CHRON, MongoDatabase.FLD_SER_NUM);
			/* can't index on variable keys ... this is yet another NO GO for MONGO)
			createIndex(series, MongoDatabase.FLD_SER_CHRON, MongoDatabase.FLD_SER_NUM, 
					MongoDatabase.FLD_SER_VALUES ... errr); */
			attributes = createCollection(db, MongoDatabase.COLL_ATTR,
					MongoDatabase.FLD_ATTR_CHRON, MongoDatabase.FLD_ATTR_PROP);
			createIndex(attributes, MongoDatabase.FLD_ATTR_PROP, MongoDatabase.FLD_ATTR_VALUE);
			createBuiltInValueTypes(database);
		} else {
			valueTypes = db.getCollection(MongoDatabase.COLL_VT);
			properties = db.getCollection(MongoDatabase.COLL_PROP);
			schemas = db.getCollection(MongoDatabase.COLL_SCHEMA);
			chronicles = db.getCollection(MongoDatabase.COLL_CHRON);
			series = db.getCollection(MongoDatabase.COLL_SER);
			attributes = db.getCollection(MongoDatabase.COLL_ATTR);
		}
	}
	private void createBuiltInValueTypes(Database db) throws T2DBException {
		UpdatableValueType<String> nameVT = db.createValueType("name", false, "NAME");
		nameVT.applyUpdates();
		UpdatableValueType<ValueType<?>> typeVT = db.createValueType("type", true, "TYPE");
		typeVT.applyUpdates();
		UpdatableValueType<TimeDomain> tdVT = db.createValueType("timedomain", true, "TIMEDOMAIN");
		tdVT.addValue(Day.DOMAIN, "daily");
		tdVT.addValue(DateTime.DOMAIN, "date and time with second precision");
		tdVT.addValue(Month.DOMAIN, "monthly");
		tdVT.addValue(Workday.DOMAIN, "working days Monday-Friday");
		tdVT.addValue(Year.DOMAIN, "yearly");
		tdVT.applyUpdates();
		UpdatableValueType<Boolean> binaryVT = db.createValueType("binary", false, "BOOLEAN");
		binaryVT.applyUpdates();
		
		UpdatableProperty<String> symbolProp = db.createProperty("Symbol", nameVT, false);
		symbolProp.applyUpdates();
		UpdatableProperty<ValueType<?>> typeProp = db.createProperty("Type", typeVT, false);
		typeProp.applyUpdates();
		UpdatableProperty<TimeDomain> calendarProp = db.createProperty("Calendar", tdVT, false);
		calendarProp.applyUpdates();
		UpdatableProperty<Boolean> sparseProp = db.createProperty("Sparsity", binaryVT, false);
		sparseProp.applyUpdates();
	}
	
	private DBCollection createCollection(DB db, String name, String... keys) throws T2DBException {
		DBCollection coll = db.getCollection(name);
		if (keys.length > 0) {
			DBObject index = new BasicDBObject();
			DBObject options = new BasicDBObject();
			for (String key : keys) {
				index.put(key, 1);
			}
			options.put("unique", 1);
			coll.createIndex(index, options);
		}
		return coll;
	}
	
	private void createIndex(DBCollection coll, String... keys) throws T2DBException {
		if (keys.length > 0) {
			DBObject index = new BasicDBObject();
			for (String key : keys)
				index.put(key, 1);
			coll.createIndex(index);
		}
	}

	/**
	 * Close the MongoDB connection if it is open.
	 */
	public void close(boolean ignoreException) throws T2DBException {
		try {
			if (connection != null)
				connection.close();
			connection = null;
		} catch (Exception e) {
			if (!ignoreException)
				throw T2DBMsg.exception(E.E00110, toString());
		}
	}

	/**
	 * Operation not supported by MongoDB.
	 * Always throws an exception.
	 * 
	 * @throws T2DBException
	 */
	public void commit() throws T2DBException {
		throw T2DBMMsg.exception(J.J80100);
	}
	
	/**
	 * Operation not supported by MongoDB.
	 * Always throws an exception.
	 * 
	 * @throws T2DBException
	 */
	public void rollback() throws T2DBException {
		throw T2DBMMsg.exception(J.J80100);
	}
	
	public String getUser() {
		return user;
	}

	public DBCollection getValueTypes() {
		return valueTypes;
	}
	
	public DBCollection getProperties() {
		return properties;
	}
	
	public DBCollection getSchemas() {
		return schemas;
	}
	
	public DBCollection getChronicles() {
		return chronicles;
	}
	
	public DBCollection getSeries() {
		return series;
	}
	
	public DBCollection getAttributes() {
		return attributes;
	}
	
	public DBCollection getCollection(Surrogate s) throws T2DBException {
		switch (s.getDBObjectType()) {
		case CHRONICLE:
			return chronicles;
		case SERIES:
			return series;
		case SCHEMA:
			return schemas;
		case PROPERTY:
			return properties;
		case VALUE_TYPE:
			return valueTypes;
		default:
			throw new RuntimeException("bug " + s.getDBObjectType());
		}
	}
	
	/**
	 * Return a string displaying the session with the connection and the user id.
	 * 
	 * @return a string displaying the session
	 */
	@Override
	public String toString() {
		return String.format("%s@%s", user, connection.toString());
	}
	
}
