package apoc.mongodb;

import org.junit.BeforeClass;

/**
 * To check that, with the latest mongodb java driver,
 * the {@link MongoTest} works correctly with mongoDB 4
 */
public class MongoVersion4Test extends MongoTest {
    
    @BeforeClass
    public static void setUp() throws Exception {
        beforeClassCommon(MongoVersion.FOUR);
    }
}
