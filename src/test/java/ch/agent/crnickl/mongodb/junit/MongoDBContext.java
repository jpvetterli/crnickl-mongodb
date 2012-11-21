package ch.agent.crnickl.mongodb.junit;

public class MongoDBContext extends ch.agent.crnickl.junit.Context {

	private static class Singleton {
		private static MongoDBContext instance = new MongoDBContext();
	}	
	
	public static MongoDBContext getInstance() {
		return Singleton.instance;
	}

}
