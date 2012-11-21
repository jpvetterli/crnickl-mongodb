package ch.agent.crnickl.mongodb;

import java.util.ArrayList;
import java.util.Collection;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.SchemaUpdatePolicyImpl;
import ch.agent.crnickl.mongodb.MongoDatabaseMethods.Operator;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoDBSchemaUpdatePolicy extends SchemaUpdatePolicyImpl {

	private MongoDatabaseMethods methods;
	
	public MongoDBSchemaUpdatePolicy(MongoDatabase database) {
		super(database);
		methods = database.getReadMethodsForProperty();
	}

	@Override
	public <T> void willDelete(Property<T> property) throws T2DBException {
		super.willDelete(property);
		if (atLeastOneSchema(property))
			throw T2DBMsg.exception(E.E20119, property.getName());
	}

	@Override
	public <T> void willDelete(ValueType<T> valueType) throws T2DBException {
		super.willDelete(valueType);
		if (atLeastOneProperty(valueType))
			throw T2DBMsg.exception(E.E10149, valueType.getName());
	}

	@Override
	public <T> void willDelete(ValueType<T> vt, T value)	throws T2DBException {
		super.willDelete(vt, value);
		if (atLeastOneActualValue(vt, value))
			throw T2DBMsg.exception(E.E10158, vt.getName(), vt.toString(value));
		if (atLeastOneDefaultValue(vt, value))
			throw T2DBMsg.exception(E.E10157, vt.getName(), vt.toString(value));
	}
	
	private <T>boolean atLeastOneProperty(ValueType<T> vt) throws T2DBException {
		return getProperties(vt).size() > 0;
	}
	
	private Collection<ObjectId> getProperties(ValueType<?> vt) throws T2DBException {
		try {
			Database database = vt.getDatabase();
			DBCollection coll = methods.getMongoDB(database).getProperties();
			DBObject query = methods.mongoObject(MongoDatabase.FLD_PROP_VT, methods.getId(vt));
			DBCursor cursor = coll.find(query);
			Collection<ObjectId> result = new ArrayList<ObjectId>();
			try {
				while (cursor.hasNext()) {
					result.add(((BasicDBObject) cursor.next()).getObjectId(MongoDatabase.FLD_ID));
				}
			} finally {
				cursor.close();
			}
			return result;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E10107, vt.getName());
		}
	}

	private <T> boolean atLeastOneDefaultValue(ValueType<T> vt, T value) throws T2DBException {
		boolean foundOne = false;
		try {
			Collection<ObjectId> propOids = getProperties(vt);
			Database database = vt.getDatabase();
			DBCollection coll = methods.getMongoDB(database).getSchemas();
			for (ObjectId propOid : propOids) {
				foundOne = coll.findOne(methods.mongoObject(Operator.OR.op(),
					new com.mongodb.DBObject[] {
						methods.mongoObject(methods.compositeName(
							MongoDatabase.FLD_SCHEMA_ATTRIBS, 
							MongoDatabase.FLD_ATTRIBDEF_PROP), propOid,
						methods.compositeName(
							MongoDatabase.FLD_SCHEMA_ATTRIBS, 
							MongoDatabase.FLD_ATTRIBDEF_VAL), value),
						methods.mongoObject(methods.compositeName(
							MongoDatabase.FLD_SCHEMA_SERIES,
							MongoDatabase.FLD_SERIESDEF_ATTRIBS,
							MongoDatabase.FLD_ATTRIBDEF_PROP), propOid,
						methods.compositeName(
								MongoDatabase.FLD_SCHEMA_SERIES,
								MongoDatabase.FLD_SERIESDEF_ATTRIBS,
								MongoDatabase.FLD_ATTRIBDEF_VAL), value)
					})) != null;
				if (foundOne)
					break;
			}
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E10112, vt.toString(), value);
		}
		return foundOne;
	}

	private <T>boolean atLeastOneActualValue(ValueType<T> vt, T value) throws T2DBException {
		boolean foundOne = false;
		try {
			Collection<ObjectId> propOids = getProperties(vt);
			Database database = vt.getDatabase();
			DBCollection coll = methods.getMongoDB(database).getAttributes();
			for (ObjectId propOid : propOids) {
				foundOne = coll.findOne(methods.mongoObject( 
							MongoDatabase.FLD_ATTR_PROP, propOid,
							MongoDatabase.FLD_ATTR_VALUE, value)) != null;
				if (foundOne)
					break;
			}
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E10113, vt.toString(), value);
		}
		return foundOne;
	}
	
	private <T>boolean atLeastOneSchema(Property<T> prop) throws T2DBException {
		try {
			Database database = prop.getDatabase();
			DBCollection coll = methods.getMongoDB(database).getSchemas();
			ObjectId propOid = methods.getId(prop);
			return coll.findOne (methods.mongoObject(Operator.OR.op(),
					new com.mongodb.DBObject[] {
						methods.mongoObject(methods.compositeName(
								MongoDatabase.FLD_SCHEMA_ATTRIBS,
								MongoDatabase.FLD_ATTRIBDEF_PROP), propOid),
						methods.mongoObject(methods.compositeName(
								MongoDatabase.FLD_SCHEMA_SERIES,
								MongoDatabase.FLD_SERIESDEF_ATTRIBS,
								MongoDatabase.FLD_ATTRIBDEF_PROP), propOid)
					})
				) != null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E20107, prop.toString());
		}
	}
	
}
