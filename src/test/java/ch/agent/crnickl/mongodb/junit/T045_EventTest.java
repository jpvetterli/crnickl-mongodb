package ch.agent.crnickl.mongodb.junit;

import ch.agent.crnickl.junit.Context;

public class T045_EventTest extends ch.agent.crnickl.junit.T045_EventTest {
	@Override
	protected Context getContext() {
		return MongoDBContext.getInstance();
	}
}

