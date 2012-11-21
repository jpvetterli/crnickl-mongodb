package ch.agent.crnickl.mongodb.junit;

import ch.agent.crnickl.junit.Context;

public class T050_ChronicleTest extends ch.agent.crnickl.junit.T050_ChronicleTest {
	@Override
	protected Context getContext() {
		return MongoDBContext.getInstance();
	}
}