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
 * Type: JDBCDatabase
 * Version: 1.1.0
 */
package ch.agent.crnickl.mongodb;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.DatabaseConfiguration;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.SeriesDefinition;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.UpdateEventOperation;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.DatabaseBackendImpl;
import ch.agent.crnickl.impl.SchemaUpdatePolicy;
import ch.agent.crnickl.impl.UpdateEventImpl;
import ch.agent.crnickl.impl.UpdateEventPublisherImpl;

/**
 * A JDBC implementation of {@link DatabaseBackendImpl}. 
 * 
 * @author Jean-Paul Vetterli
 * @version 1.1.0
 */
public class MongoDatabase extends DatabaseBackendImpl {

	public static final String FLD_ID = "_id";
	
	public static final String COLL_CHRON = "CHRON";
	public static final String FLD_CHRON_NAME = "name";
	public static final String FLD_CHRON_DESC = "desc";
	public static final String FLD_CHRON_PARENT = "parent";
	public static final String FLD_CHRON_SCHEMA = "schema";
	
	public static final String COLL_SER = "SER";
	public static final String FLD_SER_CHRON = "chron";
	public static final String FLD_SER_NUM = "number";
	public static final String FLD_SER_FIRST = "first";
	public static final String FLD_SER_LAST = "last";
	public static final String FLD_SER_VALUES = "values";
	
	public static final String COLL_ATTR = "ATTR";
	public static final String FLD_ATTR_CHRON = "chron";
	public static final String FLD_ATTR_PROP = "prop";
	public static final String FLD_ATTR_VALUE = "val";
	public static final String FLD_ATTR_DESC = "descr";
	
	public static final String COLL_PROP = "PROP";
	public static final String FLD_PROP_NAME = "name";
	public static final String FLD_PROP_VT = "type";
	public static final String FLD_PROP_INDEXED = "indexed";
	
	public static final String COLL_SCHEMA = "SCH";
	public static final String FLD_SCHEMA_NAME = "name";
	public static final String FLD_SCHEMA_BASE = "base";
	public static final String FLD_SCHEMA_ATTRIBS = "attribs";
	public static final String FLD_SCHEMA_SERIES = "series";
	
	public static final String FLD_ATTRIBDEF_NUM = "num";
	public static final String FLD_ATTRIBDEF_PROP = "prop";
	public static final String FLD_ATTRIBDEF_VAL = "val";
	public static final String FLD_ATTRIBDEF_ERASING = "erasing";
	
	public static final String FLD_SERIESDEF_NUM = "num";
	public static final String FLD_SERIESDEF_DESC = "desc";
	public static final String FLD_SERIESDEF_ATTRIBS = "attribs";
	public static final String FLD_SERIESDEF_ERASING = "erasing";
	
	public static final String COLL_VT = "VT";
	public static final String FLD_VT_NAME = "name";
	public static final String FLD_VT_TYPE = "type";
	public static final String FLD_VT_VALUES = "values";

	/** 
	 * The name of the external parameter specifying the waiting delay range 
	 * for dangerous updates. The parameter value is a range of milliseconds
	 * specified with two numbers separated by a hyphen. The default range is
	 * {@link #DB_PARAM_IntInt_WAITING_DELAY_RANGE_DEFAULT}.
	 * <p>
	 * @see #getWaitingDelay()
	 */
	public static final String DB_PARAM_IntInt_WAITING_DELAY_RANGE = "dbWaitingDelayRange";
	public static final String DB_PARAM_IntInt_WAITING_DELAY_RANGE_DEFAULT = "3000-6000";
	
	/*
	 * TODO: investigate pulling interfaces and some for ReadMethodsForFoo and
	 * WriteMethodsForBar into the "impl" layer. Is the separation in read and
	 * write methods still meaningful? Why not simply "access methods"?
	 */
	
	private ReadMethodsForChroniclesAndSeries esRMethods;
	private WriteMethodsForChroniclesAndSeries esWMethods;
	private ReadMethodsForValueType vtRMethods;
	private WriteMethodsForValueType vtWMethods;
	private ReadMethodsForProperty pRMethods;
	private WriteMethodsForProperty pWMethods;
	private ReadMethodsForSchema sRMethods;
	private WriteMethodsForSchema sWMethods;
	private MongoDB mongoDB;
	private MongoDBSchemaUpdatePolicy msup;
	
	private static Random random;
	private int minDelay = 3000;
	private int maxDelay = 6000;
		
	/**
	 * Construct a {@link DatabaseBackend}.
	 * 
	 * @param name the name of the database
	 */
	public MongoDatabase(String name) {
		super(name);
	}
	
	@Override
	protected boolean isChronicleUpdatePolicyExtensionAllowed() {
		/*
		 * Completely impossible to undo work without rollback,
		 * so just forbid extensions.
		 */
		return false;
	}



	@Override
	public void configure(DatabaseConfiguration configuration) throws T2DBException {
		super.configure(configuration);
		new MongoDB(this, configuration);
		setAccessMethods(ValueType.StandardValueType.NUMBER.name(), new AccessMethodsForNumber());

		String parameter = configuration.getParameter(DB_PARAM_IntInt_WAITING_DELAY_RANGE, false);
		if (parameter == null)
			parameter = DB_PARAM_IntInt_WAITING_DELAY_RANGE_DEFAULT;
		try {
			String[] bounds = parameter.split("-");
			switch(bounds.length) {
			case 1: 
				minDelay = new Integer(bounds[0]);
				maxDelay = minDelay;
				break;
			case 2: 
				minDelay = new Integer(bounds[0]);
				maxDelay = new Integer(bounds[1]);
				break;
			default:
				throw new IllegalArgumentException("min[-max] ?");
			}
			if (minDelay > maxDelay)
				throw new IllegalArgumentException("min<=max ?");
		} catch (Exception e) {
			throw T2DBMsg.exception(e, D.D00108, DB_PARAM_IntInt_WAITING_DELAY_RANGE, parameter);
		}
	}
	
	@Override
	public SchemaUpdatePolicy getSchemaUpdatePolicy() {
		if (msup == null)
			msup = new MongoDBSchemaUpdatePolicy(this);
		return msup;
	}


	
	public MongoDB getMongoDB() {
		if (mongoDB == null)
			mongoDB = MongoDB.getInstance();
		return mongoDB;
	}
	
	/**
	 * Sleep a number of milliseconds before trying to detect violations of
	 * referential integrity. Return true if there was no
	 * {@link InterruptedException} else return false. This is a random number
	 * in the range specified with the the
	 * {@link #DB_PARAM_IntInt_WAITING_DELAY_RANGE} configuration parameter.
	 * <p>
	 * A update is said to be dangerous when there is a risk of leaving the
	 * database in a inconsistent state. The underlying cause is lack of support
	 * for transactions or referential integrity. Example a.b == b._id and b is
	 * deleted. The following pseudo code shows how dangerous updates are
	 * performed. <blockquote>
	 * 
	 * <pre>
	 * <code>
	 * count references of document
	 * if (count > 0)
	 *     fail
	 * memo := get prior state of document to be updated
	 * update document
	 * sleep(getWaitingDelay())
	 * count references again
	 * if (count > 0) 
	 * do
	 *     restore document to its prior state using memo
	 *     fail
	 * done
	 * </code>
	 * </pre>
	 * 
	 * </blockquote>
	 * @throws InterruptedException
	 */
	public void sleep() throws InterruptedException {
		int delay = maxDelay - minDelay;
		if (delay > 0 && random == null)
			random = new Random();
		int millis = minDelay + (delay > 0 ? random.nextInt(maxDelay - minDelay) : 0);
		Thread.sleep(millis);
	}
	
	/*** manage back end store ***/

	@Override
	public void commit() throws T2DBException {
		((UpdateEventPublisherImpl)getUpdateEventPublisher()).release();
	}

	@Override
	public void rollback() throws T2DBException {
		// ignore silently
	}

	/*** Chronicle and Series ***/
	
	/**
	 * Return the object providing read methods for chronicles and series.
	 * 
	 * @return the object providing read methods for chronicles and series
	 */
	protected ReadMethodsForChroniclesAndSeries getReadMethodsForChronicleAndSeries() {
		if (esRMethods == null)
			esRMethods = new ReadMethodsForChroniclesAndSeries();
		return esRMethods;
	}
	
	/**
	 * Return the object providing write methods for chronicles and series.
	 * 
	 * @return the object providing write methods for chronicles and series
	 */
	protected WriteMethodsForChroniclesAndSeries getWriteMethodsForChroniclesAndSeries() {
		if (esWMethods == null)
			esWMethods = new WriteMethodsForChroniclesAndSeries();
		return esWMethods;
	}
	
	@Override
	public void create(UpdatableChronicle entity) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().createChronicle(entity);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, entity).withComment(entity.getDescription(false)));
	}
	
	@Override
	public void update(UpdatableChronicle entity) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().updateChronicle(entity, getChronicleUpdatePolicy());
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, entity));
	}
	
	@Override
	public void deleteAttributeValue(UpdatableChronicle entity, AttributeDefinition<?> def) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().deleteAttribute(entity, def);
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, entity).withComment("delete attribute #" + def.getNumber()));
	}

	@Override
	public void update(UpdatableChronicle entity, AttributeDefinition<?> def, String value, String description) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().updateAttribute(entity, def, value, description);
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, entity).withComment(String.format("%s=%s", def.getProperty().getName(), value)));
	}

	
	@Override
	public void deleteChronicle(UpdatableChronicle entity) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().deleteChronicle(entity, getChronicleUpdatePolicy());
		// pass name and description as comment, because they are lost when logger gets them
		String comment = getNamingPolicy().joinValueAndDescription(entity.getName(true), entity.getDescription(false));
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, entity).withComment(comment));
	}
	
	@Override
	public <T>void create(UpdatableSeries<T> series) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().createSeries(series);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, series));
	}

	@Override
	public <T>void deleteSeries(UpdatableSeries<T> series) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().deleteSeries(series, getChronicleUpdatePolicy());
		String comment = getNamingPolicy().joinValueAndDescription(series.getName(true), series.getDescription(false));
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, series).withComment(comment));
	}
	
	@Override
	public Chronicle getChronicle(Chronicle chronicle) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChronicle(chronicle.getSurrogate());
	}

	@Override
	public Chronicle getChronicleOrNull(Chronicle parent, String simpleName) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChronicleOrNull(parent, simpleName);
	}
	
	@Override
	public Collection<Chronicle> getChroniclesByParent(Chronicle parent) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChroniclesByParent(parent);
	}
	
	@Override
	public <T> List<Chronicle> getChroniclesByAttributeValue(Property<T> property, T value, int maxSize) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChroniclesByAttributeValue(property, value, maxSize);
	}

	@Override
	public boolean getAttributeValue(List<Chronicle> chronicles, Attribute<?> attribute)	throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getAttributeValue(chronicles, attribute);
	}

	@Override
	public <T> Series<T>[] getSeries(Chronicle chronicle, String[] names, int[] numbers)	throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getSeries(chronicle, names, numbers);
	}
	
	@Override
	public <T>Series<T> getSeries(Surrogate surrogate) throws T2DBException {
		checkSurrogate(surrogate, DBObjectType.SERIES);
		Series<T> series = getReadMethodsForChronicleAndSeries().getSeries(surrogate);
		if (series == null)
			throw T2DBMsg.exception(E.E50104, surrogate.toString());
		return series;

	}

	/*** Property ***/
	
	/**
	 * Return the object providing read methods for properties.
	 * 
	 * @return the object providing read methods for properties
	 */
	protected ReadMethodsForProperty getReadMethodsForProperty() {
		if (pRMethods == null)
			pRMethods = new ReadMethodsForProperty();
		return pRMethods;
	}

	/**
	 * Return the object providing write methods for properties.
	 * 
	 * @return the object providing write methods for properties
	 */
	protected WriteMethodsForProperty getWriteMethodsForProperty() {
		if (pWMethods == null)
			pWMethods = new WriteMethodsForProperty();
		return pWMethods;
	}
	
	@Override
	public Collection<Property<?>> getProperties(String pattern) throws T2DBException {
		return getReadMethodsForProperty().getProperties(this, pattern);
	}
	
	@Override
	public Property<?> getProperty(Surrogate surrogate) throws T2DBException {
		checkSurrogate(surrogate, DBObjectType.PROPERTY);
		Property<?> vt = getReadMethodsForProperty().getProperty(surrogate);
		if (vt == null)
			throw T2DBMsg.exception(E.E20109, surrogate.toString());
		return vt;
	}

	@Override
	public Property<?> getProperty(String name) throws T2DBException {
		return getReadMethodsForProperty().getProperty(this, name);
	}

	@Override
	public void create(UpdatableProperty<?> property) throws T2DBException {
		getWriteMethodsForProperty().createProperty(property);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, property));
	}

	@Override
	public void deleteProperty(UpdatableProperty<?> property) throws T2DBException {
		getWriteMethodsForProperty().deleteProperty(property, getSchemaUpdatePolicy());
		String comment = property.getName();
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, property).withComment(comment));
	}

	@Override
	public void update(UpdatableProperty<?> property) throws T2DBException {
		getWriteMethodsForProperty().updateProperty(property, getSchemaUpdatePolicy());
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, property));
	}
	
	/*** ValueType ***/
	
	/**
	 * Return the object providing read methods for value types.
	 * 
	 * @return the object providing read methods for value types
	 */
	protected ReadMethodsForValueType getReadMethodsForValueType() {
		if (vtRMethods == null)
			vtRMethods = new ReadMethodsForValueType();
		return vtRMethods;
	}

	/**
	 * Return the object providing write methods for value types.
	 * 
	 * @return the object providing write methods for value types
	 */
	protected WriteMethodsForValueType getWriteMethodsForValueType() {
		if (vtWMethods == null)
			vtWMethods = new WriteMethodsForValueType();
		return vtWMethods;
	}

	@Override
	public Collection<ValueType<?>> getValueTypes(String pattern) throws T2DBException {
		return getReadMethodsForValueType().getValueTypes(this, pattern);
	}

	@Override
	public <T>ValueType<T> getValueType(String name) throws T2DBException {
		ValueType<T> vt = getReadMethodsForValueType().getValueType(this, name);
		if (vt == null)
			throw T2DBMsg.exception(E.E10109, name);
		return vt;
	}

	@Override
	public <T>ValueType<T> getValueType(Surrogate surrogate) throws T2DBException {
		checkSurrogate(surrogate, DBObjectType.VALUE_TYPE);
		ValueType<T> vt = getReadMethodsForValueType().getValueType(surrogate);
		if (vt == null)
			throw T2DBMsg.exception(E.E10110, surrogate.toString());
		return vt;
	}


	@Override
	public <T>void create(UpdatableValueType<T> valueType) throws T2DBException {
		getWriteMethodsForValueType().createValueType(valueType);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, valueType));
	}

	@Override
	public void deleteValueType(UpdatableValueType<?> valueType) throws T2DBException {
		getWriteMethodsForValueType().deleteValueType(valueType, getSchemaUpdatePolicy());
		String comment = valueType.getName();
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, valueType).withComment(comment));
	}

	@Override
	public void update(UpdatableValueType<?> valueType) throws T2DBException {
		getWriteMethodsForValueType().updateValueType(valueType, getSchemaUpdatePolicy());
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, valueType));
	}

	/*** Schemas ***/

	/**
	 * Return the object providing read methods for schemas.
	 * 
	 * @return the object providing read methods for schemass
	 */
	protected ReadMethodsForSchema getReadMethodsForSchema() {
		if (sRMethods == null)
			sRMethods = new ReadMethodsForSchema();
		return sRMethods;
	}
	
	/**
	 * Return the object providing write methods for schemas.
	 * 
	 * @return the object providing write methods for schemas
	 */
	protected WriteMethodsForSchema getWriteMethodsForSchema() {
		if (sWMethods == null)
			sWMethods = new WriteMethodsForSchema();
		return sWMethods;
	}
	
	@Override
	public Collection<Surrogate> getSchemaSurrogates(String pattern) throws T2DBException {
		return getReadMethodsForSchema().getSchemaSurrogateList(this, pattern);
	}

	@Override
	public UpdatableSchema getUpdatableSchema(Surrogate surrogate) throws T2DBException {
		UpdatableSchema schema = getReadMethodsForSchema().getSchema(surrogate);
		if (schema == null)
			throw T2DBMsg.exception(E.E30109, surrogate.toString());
		return schema;
	}

	@Override
	public void create(UpdatableSchema schema) throws T2DBException {
		getWriteMethodsForSchema().createSchema(schema);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, schema));
	}

	@Override
	public void update(UpdatableSchema schema) throws T2DBException {
		getWriteMethodsForSchema().updateSchema(schema, getSchemaUpdatePolicy());
		((UpdateEventPublisherImpl)getUpdateEventPublisher()).publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, schema), false);
	}

	@Override
	public void deleteSchema(UpdatableSchema schema) throws T2DBException {
		getWriteMethodsForSchema().deleteSchema(schema, getSchemaUpdatePolicy());
		String comment = schema.getName();
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, schema).withComment(comment));
	}
	
	@Override
	public Surrogate findChronicle(Schema schema) throws T2DBException {
		return getWriteMethodsForSchema().findChronicle(schema);
	}

	@Override
	public Surrogate findChronicle(Property<?> property, Schema schema) throws T2DBException {
		return getWriteMethodsForSchema().findChronicle(property, schema);
	}

	@Override
	public Surrogate findChronicle(SeriesDefinition ss, Schema schema) throws T2DBException {
		return getWriteMethodsForSchema().findChronicle(ss, schema);
	}

	@Override
	public String toString() {
		return MongoDB.getInstance().toString();
	}

}
