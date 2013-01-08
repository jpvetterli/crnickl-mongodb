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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.impl.ChronicleUpdatePolicy;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.ValueAccessMethods;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeDomain;
import ch.agent.t2.time.TimeIndex;
import ch.agent.t2.timeseries.Observation;
import ch.agent.t2.timeseries.TimeAddressable;
import ch.agent.t2.timeseries.TimeSeriesFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * An implementation of {@link ValueAccessMethods} for numeric data using {@link java.lang.Double}.
 *
 * @author Jean-Paul Vetterli
 */
public class AccessMethodsForNumber extends MongoDatabaseMethods implements ValueAccessMethods<Double> {
	
	/**
	 * Construct an access method object.
	 */
	public AccessMethodsForNumber() {
	}

	@Override
	public Range getRange(Series<Double> series) throws T2DBException {
		Range range = null;
		try {
			check(Permission.READ, series);
			Surrogate s = series.getSurrogate();
			BasicDBObject obj = (BasicDBObject) getObject(s, false);
			if (obj != null) {
				long first = obj.getLong(MongoDatabase.FLD_SER_FIRST);
				long last = obj.getLong(MongoDatabase.FLD_SER_LAST);
				range = new Range(series.getTimeDomain(), first, last);
			}
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E50122, series.getName(true));
		}
		if (range == null)
			range = new Range(series.getTimeDomain());
		return range;
	}
	
	@Override
	public long getValues(Series<Double> series, Range range, TimeAddressable<Double> ts) throws T2DBException {
		if (range != null && range.isEmpty())
			return 0;
		long count = 0;
		try {
			check(Permission.READ, series);
			Surrogate s = series.getSurrogate();
			DBObject obj = getObject(s, true);
			count = extractValues(obj, range, ts);
		} catch (Exception e) {
			if (range == null)
				throw T2DBMsg.exception(e, E.E50121, series.getName(true));
			else
				throw T2DBMsg.exception(e, E.E50120, series.getName(true), range.toString());
		}
		return count;
	}
	
	private long extractValues(DBObject obj, Range range, TimeAddressable<Double> ts) throws Exception {
		if (range != null && range.isEmpty())
			return 0;
		long count = 0;
		@SuppressWarnings("unchecked")
		Map<String, Double> values = (Map<String, Double>) obj.get(MongoDatabase.FLD_SER_VALUES);
		for (String k : values.keySet()) {
			long t = Long.valueOf(k);
			if (range == null || range.isInRange(t)) {
				ts.put(t, values.get(k));
				count++;
			}
		}
		return count;
	}
	
	private Observation<Double> extractLastValue(BasicDBObject obj, TimeDomain domain, TimeIndex t) throws Exception {
		long first = obj.getLong(MongoDatabase.FLD_SER_FIRST);
		long last = obj.getLong(MongoDatabase.FLD_SER_LAST);
		Observation<Double> obs = null;
		long time = t == null ? last : t.asLong();
		if (last >= first && time >= first) {
			@SuppressWarnings("unchecked")
			Map<String, Double> values = (Map<String, Double>) obj.get(MongoDatabase.FLD_SER_VALUES);
			String key = null;
			if (time  >= last)
				key = Long.toString(last);
			else {
				TreeMap<String, Double> sorted = new TreeMap<String, Double>(values);
				key = sorted.headMap(Long.toString(time + 1)).lastKey();
			}
			obs = new Observation<Double>(domain.time(Long.valueOf(key)), values.get(key));
		}
		return obs;
	}
	
	private Observation<Double> extractFirstValue(BasicDBObject obj, TimeDomain domain, TimeIndex t) throws Exception {
		long first = obj.getLong(MongoDatabase.FLD_SER_FIRST);
		long last = obj.getLong(MongoDatabase.FLD_SER_LAST);
		Observation<Double> obs = null;
		long time = t == null ? first : t.asLong();
		if (last >= first && time <= last) {
			@SuppressWarnings("unchecked")
			Map<String, Double> values = (Map<String, Double>) obj.get(MongoDatabase.FLD_SER_VALUES);
			String key = null;
			if (time  <= first)
				key = Long.toString(first);
			else {
				TreeMap<String, Double> sorted = new TreeMap<String, Double>(values);
				key = sorted.tailMap(Long.toString(time)).firstKey();
			}
			obs = new Observation<Double>(domain.time(Long.valueOf(key)), values.get(key));
		}
		return obs;
	}
	
	@Override
	public Observation<Double> getFirst(Series<Double> series, TimeIndex time) throws T2DBException {
		try {
			check(Permission.READ, series);
			Surrogate s = series.getSurrogate();
			DBObject obj = getObject(s, true);
			return extractFirstValue((BasicDBObject) obj, series.getTimeDomain(),  time);
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E50123, series.getName(true), time.toString());
		}
	}
	
	@Override
	public Observation<Double> getLast(Series<Double> series, TimeIndex time) throws T2DBException {
		try {
			check(Permission.READ, series);
			Surrogate s = series.getSurrogate();
			DBObject obj = getObject(s, true);
			return extractLastValue((BasicDBObject) obj, series.getTimeDomain(),  time);
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E50124, series.getName(true), time.toString());
		}
	}

	@Override
	public boolean deleteValue(UpdatableSeries<Double> series, TimeIndex t, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		try {
			check(Permission.MODIFY, series);
			policy.willDelete(series, t);
			policy.deleteValue(series, t);
			
			Surrogate s = series.getSurrogate();
			DBObject obj = getObject(s, true);
			// force sparse, so it's always possible to repair when there are excessive gaps
			TimeAddressable<Double> values = TimeSeriesFactory.make(series.getTimeDomain(), Double.class, true);
			extractValues(obj, null, values);
			if (values.getRange().isInRange(t)) {
				values.put(t, values.getMissingValue());
				done = true;
			}
			update(series, values);
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E50113, series.getName(true), t.toString());
		} finally {
		}
		return done;
	}
	
	@Override
	public boolean updateSeries(UpdatableSeries<Double> series, Range range, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		try {
			check(Permission.MODIFY, series);
			policy.willUpdate(series, range);
			done = policy.update(series, range);
			Surrogate s = series.getSurrogate();
			DBObject obj = getObject(s, true);
			// force sparse, so it's always possible to repair when there are excessive gaps
			TimeAddressable<Double> values = TimeSeriesFactory.make(series.getTimeDomain(), Double.class, true);
			extractValues(obj, null, values);
			if (values.setRange(range))
				done = true;
			update(series, values);
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E50109, series.getName(true));
		} finally {
		}
		return done;
	}
	
	@Override
	public long updateValues(UpdatableSeries<Double> series, TimeAddressable<Double> values, ChronicleUpdatePolicy policy) throws T2DBException {
		long count = 0;
		
		try {
			check(Permission.MODIFY, series);
			Surrogate s = series.getSurrogate();
			DBObject obj = getObject(s, true);
			// force sparse, so it's always possible to repair when there are excessive gaps
			TimeAddressable<Double> current = TimeSeriesFactory.make(series.getTimeDomain(), Double.class, true);
			extractValues(obj, null, current);
			for(Observation<Double> obs : values) {
				current.put(obs.getIndex(), obs.getValue());
				count++;
			}
			update(series, current);
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E50114, series.getName(true));
		}
		return count;
	}
	
	private <T>void update(Series<T> series, TimeAddressable<Double> values) throws T2DBException {
		com.mongodb.DBObject operation = null;
		Range range = values.getRange();
		Map<String, Double> data = new HashMap<String, Double>();
		for (Observation<Double> obs : values) {
			data.put(Long.toString(obs.getIndex()), obs.getValue());
		}
		operation = operation(Operator.SET, 
				MongoDatabase.FLD_SER_FIRST, range.getFirstIndex(),
				MongoDatabase.FLD_SER_LAST, range.getLastIndex(),
				MongoDatabase.FLD_SER_VALUES, data);
		
		Surrogate s = series.getSurrogate();
		getMongoDB(s).getSeries().update(asQuery(s.getId()), operation);
	}

	
}
