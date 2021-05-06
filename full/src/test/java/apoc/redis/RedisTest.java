package apoc.redis;

import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RedisTest {
    
    // TODO
    // TODO
    // TODO : TEST NUMERO DI CONNESSIONI!!
    // TODO
    // TODO

    private static int REDIS_DEFAULT_PORT = 6379;

//    private RedisBackedCache underTest;
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @Rule
    public GenericContainer redis = new GenericContainer("redis:6.2.3")
            // TODO - METTERE ANCHE LE CREDENZIALI e vedere se va...
//            .withCommand("redis-server --requirepass SUPER_SECRET")// command: redis-server --requirepass SUPER_SECRET
            .withExposedPorts(REDIS_DEFAULT_PORT);
    
    // todo
    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, Redis.class);
    }

    @Before
    public void setUp() {
        String address = redis.getHost();
        Integer port = redis.getFirstMappedPort();

        // Now we have an address and port for Redis, no matter where it is running
//        underTest = new RedisBackedCache(address, port);
        System.out.println("port - address" + address + " . " + port);

        assertEquals(1, getNumConnections());
    }
    
    @After
    public void after() {
        assertEquals(1, getNumConnections());
//        try {
//            final Container.ExecResult execResult = redis.execInContainer("redis-cli CLIENT LIST");
//            System.out.println(execResult);
//            final String stdout = redis.execInContainer("redis-cli", "CLIENT", "LIST").getStdout();
//            final int length = stdout.split(System.lineSeparator()).length;
//            System.out.println(matches);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }

    private int getNumConnections() {
        try {
            return StringUtils.countMatches(redis.execInContainer("redis-cli", "CLIENT", "LIST").getStdout(), 
                    System.lineSeparator());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    private int 

    
    @Test
    public void testSimplePutAndGet() {
        TestUtil.testCall(db, "CALL apoc.redis.set($host, '1', '2')", Map.of("host", getUrl()), r -> {
//            Map doc = (Map) r.get("value");
//            assertNotNull(doc.get("1"));
            assertEquals("OK", r.get("value"));
        });
        
//        underTest.put("test", "example");

//        String retrieved = underTest.get("test");
//        assertEquals("example", retrieved);
    }


    private String getUrl() {
//        return String.format("redis://%s@%s:%s", "SUPER_SECRET", redis.getHost(), redis.getMappedPort(REDIS_DEFAULT_PORT));
        return String.format("redis://%s:%s", redis.getHost(), redis.getMappedPort(REDIS_DEFAULT_PORT));
    }
}
