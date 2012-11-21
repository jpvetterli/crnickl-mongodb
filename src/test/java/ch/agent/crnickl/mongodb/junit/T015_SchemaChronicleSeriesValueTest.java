package ch.agent.crnickl.mongodb.junit;

import ch.agent.crnickl.junit.Context;

public class T015_SchemaChronicleSeriesValueTest extends ch.agent.crnickl.junit.T015_SchemaChronicleSeriesValueTest {
	@Override
	protected Context getContext() {
		return MongoDBContext.getInstance();
	}
}
