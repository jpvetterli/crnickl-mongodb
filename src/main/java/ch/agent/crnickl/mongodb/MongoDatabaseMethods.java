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
 */
package ch.agent.crnickl.mongodb;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.DBObject;
import ch.agent.crnickl.api.DBObjectId;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.impl.DatabaseMethodsImpl;
import ch.agent.crnickl.mongodb.T2DBMMsg.J;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;


/**
 * A MongoDatabaseMethods object provides implementation support. It is
 * meant as base class for actual access methods. 
 * 
 * @author Jean-Paul Vetterli
 */
/**
 * @author jp
 *
 */
public class MongoDatabaseMethods extends DatabaseMethodsImpl {

	public enum Operator {
		AND("$and"),
		OR("$or"),
		IN("$in"),
		INC("$inc"),
		SET("$set"),
		PUSH("$push");
	    private String op;
	    private Operator(String op) {
	        this.op = op;
	    }
	    public String op() {
	        return op;
	    }
	}
	
	private static Matcher regex;

	public MongoDB getMongoDB(Database db) {
		return ((MongoDatabase) db).getMongoDB();
	}
	
	public MongoDB getMongoDB(DBObject dbObj) {
		return ((MongoDatabase) dbObj.getSurrogate().getDatabase()).getMongoDB();
	}
	
	public MongoDB getMongoDB(Surrogate surr) {
		return ((MongoDatabase) surr.getDatabase()).getMongoDB();
	}
	
	/**
	 * Return the MongoDB database object corresponding to a surrogate.
	 * When <code>mustExist</code> is false the method returns null if the object cannot be found,
	 * else an exception is thrown.
	 * 
	 * @param s a surrogate
	 * @param mustExist if true throw an exception when the object cannot be found
	 * @return a DBObject object or null
	 * @throws T2DBException
	 */
	public com.mongodb.DBObject getObject(Surrogate s, boolean mustExist) throws T2DBException {
		DBCollection coll = getMongoDB(s).getCollection(s);
		Throwable cause = null;
		com.mongodb.DBObject obj = null;
		try {
			obj = coll.findOne(asQuery(s.getId()));
		} catch (Exception e) {
			cause = e;
		}
		if (cause != null || (obj == null && mustExist))
			throw T2DBMMsg.exception(cause, J.J80020, s.toString());
		return obj;
	}

	/**
	 * @param bdo a {@link BasicDBObject}
	 * @return the argument's {@link ObjectId} 
	 */
	public ObjectId getObjectId(BasicDBObject bdo) {
		return bdo.getObjectId(MongoDatabase.FLD_ID);
	}
	
	/**
	 * Return the internal ID of a database object or 0 if the object is null or 
	 * is <em>in construction</em>.
	 * The internal ID is not exposed to clients.
	 * <p>
	 * This method is for use inside the MongoDB implementation
	 * and its argument must have a {@link DBObjectId} implemented
	 * by {@link MongoDBObjectId}.
	 * 
	 * @param dBObject a database object or null
	 * @return an id or null
	 */
	public ObjectId getIdOrZero(DBObject dBObject) {
		try {
			if (dBObject != null && !dBObject.inConstruction())
				return ((MongoDBObjectId) dBObject.getId()).value();
			else
				return null;
		} catch(ClassCastException e) {
			throw new RuntimeException("bug: " + dBObject.toString(), e);
		}
	}
	
	/**
	 * Return the internal ID of a database object.
	 * The internal ID is not exposed to clients.
	 * <p>
	 * This method is for use inside the MongoDB implementation
	 * and its argument must have a {@link DBObjectId} implemented
	 * by {@link MongoDBObjectId}.
	 * 
	 * @param dBObject a non-null database object
	 * @return a non-null id
	 */
	protected ObjectId getId(DBObject dBObject) {
		if (dBObject == null)
			throw new IllegalArgumentException();
		ObjectId id = getIdOrZero(dBObject);
		if (id == null)
			throw new RuntimeException("bug (database integrity violation)");
		return id;
	}

	/**
	 * Extract the internal ID of a database object from its surrogate.
	 * The internal ID is not exposed to clients.
	 * <p>
	 * This method is for use inside the MongoDB implementation
	 * and its argument must have a {@link DBObjectId} implemented
	 * by {@link MongoDBObjectId}.
	 * 
	 * @param surrogate the surrogate of a database object
	 * @return a non-null id
	 */
	public ObjectId getId(Surrogate surrogate) {
		return getId(surrogate.getObject());
	}
	
	/**
	 * Create a surrogate for a database object.
	 * 
	 * @param db the database of the object
	 * @param dot the type of the object
	 * @param id the internal ID of the database object
	 * @return a surrogate
	 */
	public Surrogate makeSurrogate(Database db, DBObjectType dot, ObjectId id) {
		return super.makeSurrogate(db,  dot, new MongoDBObjectId(id));
	}

	protected <T>com.mongodb.DBObject asQuery(DBObjectId id) throws T2DBException {
		com.mongodb.DBObject bo = new BasicDBObject(1);
		try {
			bo.put(MongoDatabase.FLD_ID, ((MongoDBObjectId)id).value());
		} catch (ClassCastException e) {
			throw T2DBMMsg.exception(e, J.J81012, id.toString());
		}
		return bo;
	}
	
	/**
	 * Remove enclosing slashes from the input pattern. If there are leading and
	 * trailing slashes, the method returns the enclosed string, else it returns
	 * null.
	 * 
	 * @param pattern
	 * @return the pattern without the leading and trailing slashes or null
	 */
	protected String extractRegexp(String pattern) {
		if (regex == null)
			regex = Pattern.compile("^/(.*)/$").matcher("");
		regex.reset(pattern);
		if (regex.matches())
			return regex.group(1);
		else
			return null;
	}
	
	/**
	 * Add an update operation to a DBObject.
	 * The operation is defined by an operator and a list of arguments. 
	 * See {@link #mongoObject(Object...)} for restrictions on the list of arguments.
	 * 
	 * @param operation the operation object
	 * @param op the operator
	 * @param arg the list of arguments
	 * @throws IllegalArgumentException
	 */
	protected void addOperation(com.mongodb.DBObject operation, Operator op, Object ... arg) {
		operation.put(op.op(), mongoObject(arg));
	}	
	
	/**
	 * Return a com.mongodb.DbObject representing an operation object for an
	 * operator and list of arguments. See {@link #mongoObject(Object...)} for
	 * restrictions on the list of arguments.
	 * 
	 * @param op
	 *            the operator
	 * @param arg
	 *            the list of arguments
	 * @return an operation
	 * @throws IllegalArgumentException
	 */
	protected com.mongodb.DBObject operation(Operator op, Object ... arg) {
		com.mongodb.DBObject operation = new BasicDBObject();
		addOperation(operation, op, arg);
		return operation;
	}	
	
	/**
	 * Return an array of key-value pairs as a DBObject. The array must be
	 * non-empty and its length must be even. Non-even elements (the keys) must
	 * be non-null Strings. Even elements must be BSON compatible (this is not
	 * enforced by this method but violations are detected later).
	 * 
	 * @param arg
	 *            an non-empty array of arguments of even length
	 * @return a DBObject
	 * @throws IllegalArgumentException
	 */
	protected com.mongodb.DBObject mongoObject(Object ... arg) {
		if (arg.length == 0 || arg.length % 2 != 0)
			throw new IllegalArgumentException(new T2DBMMsg(J.J81014, arg.length).toString());
		com.mongodb.DBObject operand = new BasicDBObject(arg.length / 2);
		for (int i = 0; i < arg.length; i++) {
			if (arg[i] == null)
				throw new IllegalArgumentException(new T2DBMMsg(J.J81016, i).toString());
			try {
				operand.put((String) arg[i], arg[++i]);
			} catch (ClassCastException e) {
				throw new IllegalArgumentException(new T2DBMMsg(J.J81016, i).toString(), e);
			}
		}
		return operand;
	}	
	
	/**
	 * Return a composite mongodb identifier.
	 * @param names series of zero or more names
	 * @return a composite name
	 */
	protected String compositeName(String... names) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < names.length; i++) {
			if (i > 0)
				b.append('.');
			b.append(names[i]);
		}
		return b.toString();
	}

}
