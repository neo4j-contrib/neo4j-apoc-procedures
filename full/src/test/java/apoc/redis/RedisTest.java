package apoc.redis;

import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.util.TestUtil.isTravis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class RedisTest {
    // todo
    // todo - vedere se riesco a fare un test con timeout...
    // todo

    private static int REDIS_DEFAULT_PORT = 6379;
    private static String URI;

    private static GenericContainer redis;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void beforeClass() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            redis = new GenericContainer("redis:6.2.3")
                    .withCommand("redis-server --requirepass SUPER_SECRET")
                    .withExposedPorts(REDIS_DEFAULT_PORT);
            redis.start();
        }, Exception.class);
        TestUtil.registerProcedure(db, Redis.class);
        assumeNotNull(redis);
        assumeTrue("Redis must be running", redis.isRunning());
        URI = String.format("redis://%s@%s:%s", "SUPER_SECRET", redis.getHost(), redis.getMappedPort(REDIS_DEFAULT_PORT));
    }

    @AfterClass
    public static void tearDown() {
        if (redis != null) {
            redis.stop();
        }
    }

    @Before
    public void setUp() {
        assertEquals(1, getNumConnections());
    }

    @After
    public void after() {
        assertEquals(1, getNumConnections());
    }

    @Test
    public void testStringsCommands() {
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'myKey', 'myValue')", Map.of("uri", URI),
                r -> assertEquals("myValue", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.get($uri, 'myKey'')", Map.of("uri", URI),
                r -> assertEquals("myValue", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.append($uri, 'myKey', '2')", Map.of("uri", URI),
                r -> assertEquals("myValue2".length(), r.get("value")));

        // incrby - decrby
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'myKey', '1')", Map.of("uri", URI),
                r -> assertEquals("1", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.incrby($uri, 'myKey', 2)", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.decrby($uri, 'myKey', 2)", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));
    }

    @Test
    public void testListsCommands() {
        TestUtil.testCall(db, "CALL apoc.redis.push($uri, 'myListKey', ['foo','bar','baz'])", Map.of("uri", URI),
                r -> assertEquals(1, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("foo", "bar", "baz"), r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.push($uri, 'myListKey', ['prefix1', 'prefix2'], {right: false})", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("prefix1", "prefix2", "foo", "bar", "baz"), r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pop($uri, 'myListKey')", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("prefix1", "prefix2", "foo", "bar"), r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.pop($uri, 'myListKey', {right: false})", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("prefix2", "foo", "bar"), r.get("value")));
    }

    @Test
    public void testSetsCommands() {
        TestUtil.testCall(db, "CALL apoc.redis.sadd($uri, 'mySetKey', ['foo','bar','baz'])", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.sadd($uri, 'mySetKeyTwo', ['alpha', 'beta'])", Map.of("uri", URI),
                r -> assertEquals(2L, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.sunion($uri, ['mySetKey', 'mySetKeyTwo'])", Map.of("uri", URI),
                r -> assertEquals(Set.of("foo", "bar", "baz", "alpha", "beta"), new HashSet<>((List<String>) r.get("value"))));
        
        TestUtil.testCall(db, "CALL apoc.redis.scard($uri, 'mySetKey')", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.smembers($uri, 'mySetKey')", Map.of("uri", URI),
                r -> assertEquals(Set.of("foo", "bar", "baz"), new HashSet<>((List<String>) r.get("value"))));

        TestUtil.testCall(db, "CALL apoc.redis.spop($uri, 'mySetKey')", Map.of("uri", URI),
                r -> assertTrue(List.of("foo", "bar", "baz").contains(r.get("value"))));

        TestUtil.testCall(db, "CALL apoc.redis.scard($uri, 'mySetKey')", Map.of("uri", URI),
                r -> assertEquals(2L, r.get("value")));
    }

    @Test
    public void testSortedSetsCommands() {
        TestUtil.testCall(db, "CALL apoc.redis.zadd($uri, 'mySortedSetKey', [0, 'first', 100, 'third', 1, 'second'])", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.zcard($uri, 'mySortedSetKey')", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.zrangebyscore($uri, 'mySortedSetKey', 0, 100)", Map.of("uri", URI),
                r -> assertEquals(List.of("first", "second", "third"), r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.zrem($uri, 'mySortedSetKey', ['first', 'second'])", Map.of("uri", URI),
                r -> assertEquals(2L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.zrangebyscore($uri, 'mySortedSetKey', 0, 100)", Map.of("uri", URI),
                r -> assertEquals(List.of("third"), r.get("value")));
    }

    @Test
    public void testHashesCommands() {
        TestUtil.testCall(db, "CALL apoc.redis.hset($uri, 'mapKey', {alpha: 'beta', gamma, 'delta', epsilon, 'zeta', number, '1'})", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hdel($uri, 'mapKey', ['alpha', 'beta'])", Map.of("uri", URI),
                r -> assertEquals(2L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hexists($uri, 'mapKey', 'epsilon')", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hget($uri, 'mapKey', 'epsilon')", Map.of("uri", URI),
                r -> assertEquals("zeta", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hincrby($uri, 'mapKey', 'number', 3)", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hgetall($uri, 'mapKey')", Map.of("uri", URI),
                r -> assertEquals(Map.of("epsilon", "zeta", "number", "4"), r.get("value")));
    }

    @Test
    public void testDispatch() {
        TestUtil.testCall(db, "CALL apoc.redis.dispatch($uri, 'LPUSH', 'IntegerOutput', ['key'], ['valueList'])", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'key', 0, 1)", Map.of("uri", URI),
                r -> assertEquals(List.of("valueList"), r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.dispatch($uri, 'MSET', 'BooleanOutput', null, null, {keyOne: 'valueOne', keyTwo: 'valueTwo'})", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.get($uri, 'keyOne')", Map.of("uri", URI), r -> assertEquals("valueOne", r.get("value")));
        TestUtil.testCall(db, "CALL apoc.redis.get($uri, 'keyTwo')", Map.of("uri", URI), r -> assertEquals("valueTwo", r.get("value")));
    }

    @Test
    public void testEvalCommand() {
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'testEval', '1')", Map.of("uri", URI),
                r -> assertEquals("1", r.get("value")));
        TestUtil.testCall(db, "CALL apoc.redis.eval($uri, 'return redis.call('get','foo')', ['0'])", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));
    }

    @Test
    public void testKeysCommand() {
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'from', 'one')", Map.of("uri", URI),
                r -> assertEquals("one", r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.copy($uri, 'from', 'to')", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.get($uri, 'to')", Map.of("uri", URI),
                r -> assertEquals("one", r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.exists($uri, 'to')", Map.of("uri", URI),
                r -> assertEquals("one", r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pexpire($uri, 'to', 100, {expireAt: false})", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pttl($uri, 'to')", Map.of("uri", URI),
                r -> {
                    final long value = (long) r.get("value");
                    assertTrue( value <= 100L && value > 0L);
                });

        TestUtil.testCall(db, "CALL apoc.redis.persist($uri, 'to')", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pttl($uri, 'to')", Map.of("uri", URI),
                r -> assertTrue((long) r.get("value") == 0L));
    }

    @Test
    public void testServersCommand() {
        TestUtil.testCall(db, "CALL apoc.redis.info($uri)", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));

        final String keyConfig = "always-show-logo";
        TestUtil.testCall(db, "CALL apoc.redis.configGet($uri, $keyConfig)", Map.of("uri", URI, "keyConfig", keyConfig),
                r -> assertEquals(Map.of(keyConfig, "no"), r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.configSet($uri, $keyConfig, 'yes')", Map.of("uri", URI, "keyConfig", keyConfig),
                r -> assertEquals("OK", r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.configGet($uri, $keyConfig)", Map.of("uri", URI, "keyConfig", keyConfig),
                r -> assertEquals(Map.of(keyConfig, "yes"), r.get("value")));
    }

    private int getNumConnections() {
        try {
            return StringUtils.countMatches(redis.execInContainer("redis-cli", "CLIENT", "LIST").getStdout(),
                    System.lineSeparator());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
