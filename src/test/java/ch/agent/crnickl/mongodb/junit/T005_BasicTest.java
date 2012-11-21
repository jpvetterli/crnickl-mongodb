package ch.agent.crnickl.mongodb.junit;

import org.bson.types.ObjectId;

import ch.agent.crnickl.api.DBObjectId;
import ch.agent.crnickl.junit.Context;
import ch.agent.crnickl.mongodb.MongoDBObjectId;

public class T005_BasicTest extends ch.agent.crnickl.junit.T005_BasicTest {
	
	protected DBObjectId id(int id) {
		return new MongoDBObjectId(ObjectId.get());
	}

	@Override
	protected Context getContext() {
		return MongoDBContext.getInstance();
	}

}
