package ch.agent.crnickl.mongodb.junit;

import ch.agent.crnickl.junit.Context;

public class T041_UpdatableSeriesTest extends ch.agent.crnickl.junit.T041_UpdatableSeriesTest {
	@Override
	protected Context getContext() {
		return MongoDBContext.getInstance();
	}
}