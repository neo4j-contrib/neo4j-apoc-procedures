package apoc.mongodb;

import org.junit.BeforeClass;

/**
 * To check that, with the latest mongodb java driver,
 * the {@link MongoDBTest} works correctly with mongoDB 4
 */
public class MongoDBVersion4Test extends MongoDBTest {
    
    @BeforeClass
    public static void setUp() throws Exception {
        MongoDBTest.beforeClassCommon(MongoVersion.FOUR);
    }
}
