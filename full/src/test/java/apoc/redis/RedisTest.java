package apoc.redis;

import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class RedisTest {
    private static int BEFORE_CONNECTION = 0;
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
        BEFORE_CONNECTION = getNumConnections();
    }

    @AfterClass
    public static void tearDown() {
        if (redis != null) {
            redis.stop();
        }
    }

    @After
    public void after() {
        assertEquals(BEFORE_CONNECTION, getNumConnections());
    }

    @Test
    public void testStringsCommands() {
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'myKey', 'myValue')", Map.of("uri", URI),
                r -> assertNull(r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'myKey', 'myNewValue')", Map.of("uri", URI),
                r -> assertEquals("myValue", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.get($uri, 'myKey')", Map.of("uri", URI),
                r -> assertEquals("myNewValue", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.append($uri, 'myKey', '2')", Map.of("uri", URI),
                r -> assertEquals((long) "myNewValue2".length(), r.get("value")));

        // incrby
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'myKey', '1')", Map.of("uri", URI),
                r -> assertEquals("myNewValue2", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.incrby($uri, 'myKey', 2)", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));
    }

    @Test
    public void testListsCommands() {
        TestUtil.testCall(db, "CALL apoc.redis.push($uri, 'myListKey', ['foo','bar','baz'])", Map.of("uri", URI),
                r -> assertEquals(3L, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("foo", "bar", "baz"), r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.push($uri, 'myListKey', ['prefix1'], {right: false})", Map.of("uri", URI),
                r -> assertEquals(4L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("prefix1", "foo", "bar", "baz"), r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pop($uri, 'myListKey')", Map.of("uri", URI),
                r -> assertEquals("baz", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("prefix1", "foo", "bar"), r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.pop($uri, 'myListKey', {right: false})", Map.of("uri", URI),
                r -> assertEquals("prefix1", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.lrange($uri, 'myListKey', 0 , 10)", Map.of("uri", URI),
                r -> assertEquals(List.of("foo", "bar"), r.get("value")));
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
        TestUtil.testCall(db, "CALL apoc.redis.hset($uri, 'mapKey', {alpha: 'beta', gamma: 'delta', epsilon: 'zeta', number: '1'})", Map.of("uri", URI),
                r -> assertEquals(4L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hdel($uri, 'mapKey', ['alpha', 'gamma'])", Map.of("uri", URI),
                r -> assertEquals(2L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hexists($uri, 'mapKey', 'epsilon')", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hget($uri, 'mapKey', 'epsilon')", Map.of("uri", URI),
                r -> assertEquals("zeta", r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hincrby($uri, 'mapKey', 'number', 3)", Map.of("uri", URI),
                r -> assertEquals(4L, r.get("value")));

        TestUtil.testCall(db, "CALL apoc.redis.hgetall($uri, 'mapKey')", Map.of("uri", URI),
                r -> assertEquals(Map.of("epsilon", "zeta", "number", "4"), r.get("value")));
    }

    @Test
    public void testEvalCommand() {
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'testEval', 'valueEval')", Map.of("uri", URI), r -> assertNull(r.get("value")));
        TestUtil.testCall(db, "CALL apoc.redis.eval($uri, 'return redis.call(\"get\", KEYS[1])', 'VALUE', ['testEval'], ['key:name'])", Map.of("uri", URI),
                r -> assertEquals("valueEval", r.get("value")));
    }

    @Test
    public void testKeysCommand() {
        TestUtil.testCall(db, "CALL apoc.redis.setGet($uri, 'from', 'one')", Map.of("uri", URI),
                r -> assertNull(r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.copy($uri, 'from', 'to')", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.get($uri, 'to')", Map.of("uri", URI),
                r -> assertEquals("one", r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.exists($uri, ['to'])", Map.of("uri", URI),
                r -> assertEquals(1L, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pexpire($uri, 'to', 100, false)", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pttl($uri, 'to')", Map.of("uri", URI),
                r -> {
                    final long value = (long) r.get("value");
                    assertTrue( value <= 100L && value > 0L);
                });

        TestUtil.testCall(db, "CALL apoc.redis.persist($uri, 'to')", Map.of("uri", URI),
                r -> assertEquals(true, r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.pttl($uri, 'to')", Map.of("uri", URI),
                r -> assertEquals(-1L, (long) r.get("value")));
    }

    @Test
    public void testServersCommand() {
        TestUtil.testCall(db, "CALL apoc.redis.info($uri)", Map.of("uri", URI),
                r -> assertTrue(((String) r.get("value")).contains("redis_version:6.2.3")));

        final String keyConfig = "slowlog-max-len";
        TestUtil.testCall(db, "CALL apoc.redis.configGet($uri, $keyConfig)", Map.of("uri", URI, "keyConfig", keyConfig),
                r -> assertEquals(Map.of(keyConfig, "128"), r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.configSet($uri, $keyConfig, '64')", Map.of("uri", URI, "keyConfig", keyConfig),
                r -> assertEquals("OK", r.get("value")));
        
        TestUtil.testCall(db, "CALL apoc.redis.configGet($uri, $keyConfig)", Map.of("uri", URI, "keyConfig", keyConfig),
                r -> assertEquals(Map.of(keyConfig, "64"), r.get("value")));
    }

    private static int getNumConnections() {
        try {
            return StringUtils.countMatches(redis.execInContainer("redis-cli", "CLIENT", "LIST").getStdout(),
                    System.lineSeparator());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
