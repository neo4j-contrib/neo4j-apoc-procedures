package apoc.redis;

import apoc.result.BooleanResult;
import apoc.result.DoubleResult;
import apoc.result.ListResult;
import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import apoc.util.MissingDependencyException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO -> creare classe RedisConnection
public class Redis {

    // -- String
    @Procedure
    @Description("apoc.redis.setGet(uri, key, value, {config}) | Execute the 'SET key value' command and return old value stored (or null if did not exists)")
    public Stream<StringResult> setGet(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.setGet(key, value))));
    }

    @Procedure
    @Description("apoc.redis.get(uri, key, {config}) | Execute the 'GET key' command")
    public Stream<StringResult> get(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // String mset(Map<K, V> map);
        // String msetnx(Map<K, V> map); --> config
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.get(key))));
    }

    @Procedure
    @Description("apoc.redis.append(uri, key, value, {config}) | Execute the 'APPEND key value' command")
    public Stream<LongResult> append(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.append(key, value))));
    }

    @Procedure
    @Description("apoc.redis.incrby(uri, key, amount, {config}) | Execute the 'INCRBY key increment' command")
    public Stream<LongResult> incrby(@Name("uri") String uri, @Name("key") String key, @Name("amount") long amount, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.incrby(key, amount))));
    }

    @Procedure
    @Description("apoc.redis.decrby(uri, key, amount, {config}) | Execute the 'DECRBY key increment' command")
    public Stream<LongResult> decrby(@Name("uri") String uri, @Name("key") String key, @Name("amount") long amount, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.decrby(key, amount))));
    }

    // -- Hashes
    @Procedure
    @Description("apoc.redis.hdel(uri, key, fields, {config}) | Execute the 'HDEL key fields' command")
    public Stream<LongResult> hdel(@Name("uri") String uri, @Name("key") String key, @Name("fields") List<String> fields, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.hdel(key, fields.toArray(String[]::new)))));
    }

    @Procedure
    @Description("apoc.redis.hexists(uri, key, field, {config}) | Execute the 'HDEL key field' command")
    public Stream<BooleanResult> hexists(@Name("uri") String uri, @Name("key") String key, @Name("field") String field, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.hexists(key, field))));
    }

    @Procedure
    @Description("apoc.redis.hget(uri, key, field, {config}) | Execute the 'HGET key field' command")
    public Stream<StringResult> hget(@Name("uri") String uri, @Name("key") String key, @Name("field") String field, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.hget(key, field))));
    }

    @Procedure
    @Description("apoc.redis.hincrby(uri, key, field, amount, {config}) | Execute the 'HINCRBY key field amount' command")
    public Stream<LongResult> hincrby(@Name("uri") String uri, @Name("key") String key, @Name("field") String field, @Name("amount") long amount, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.hincrby(key, field, amount))));
    }

    @Procedure
    @Description("apoc.redis.hgetall(uri, key, {config}) | Execute the 'HGETALL key' command")
    public Stream<MapResult> hgetall(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new MapResult(connection.hgetall(key))));
    }

    @Procedure
    @Description("apoc.redis.hset(uri, key, value, {config}) | Execute the 'HSET key mapFields' command, where mapFields is a map of field1, value1, field2, value2,...")
    public Stream<LongResult> hset(@Name("uri") String uri, @Name("key") String key, @Name("field") Map<String, String> mapFields, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.hset(key, mapFields))));
    }
    
    // -- Lists
    @Procedure
    @Description("apoc.redis.push(uri, key, values, {config}) | Execute the 'LPUSH key field values' command, or the 'LPUSH' if config right=true (default)")
    public Stream<LongResult> push(@Name("uri") String uri, @Name("key") String key, @Name("value") List<String> values, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.push(key, values.toArray(String[]::new)))));
    }

    @Procedure
    @Description("apoc.redis.pop(uri, key, {config}) | Execute the 'LPOP key' command, or the 'RPOP' if config right=true (default)")
    public Stream<StringResult> pop(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.pop(key))));
    }

    @Procedure
    @Description("apoc.redis.lrange(uri, key, start, stop, {config}) | Execute the 'LRANGE key start stop' command")
    public Stream<ListResult> lrange(@Name("uri") String uri, @Name("key") String key, @Name("start") long start, @Name("stop") long stop, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.lrange(key, start, stop))));
    }

    // -- Sets
    @Procedure
    @Description("apoc.redis.sadd(uri, key, members, {config}) | Execute the 'SADD key members' command")
    public Stream<LongResult> sadd(@Name("uri") String uri, @Name("key") String key, @Name("members") List<String> members, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.sadd(key, members.toArray(String[]::new)))));
    }

    @Procedure
    @Description("apoc.redis.scard(uri, key, {config}) | Execute the 'SCARD key' command")
    public Stream<LongResult> scard(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.scard(key))));
    }

    @Procedure
    @Description("apoc.redis.spop(uri, key, {config}) | Execute the 'SPOP key' command")
    public Stream<StringResult> spop(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.spop(key))));
    }

    @Procedure
    @Description("apoc.redis.smembers(uri, key, {config}) | Execute the 'SMEMBERS key' command")
    public Stream<ListResult> smembers(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.smembers(key))));
    }

    @Procedure
    @Description("apoc.redis.sunion(uri, keys, {config}) | Execute the 'SUNION keys' command")
    public Stream<ListResult> sunion(@Name("uri") String uri, @Name("keys") List<String> keys, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.sunion(keys.toArray(String[]::new)))));
    }

    // -- Sorted Sets
    @Procedure
    @Description("apoc.redis.zadd(uri, keys, scoresAndMembers, {config}) | Execute the 'ZADD key scoresAndMembers' command, where scoresAndMembers is a list of score,member,score,member,...")
    public Stream<LongResult> zadd(@Name("uri") String uri, @Name("key") String key, @Name("value") List<Object> scoresAndMembers, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.zadd(key,
                scoresAndMembers.stream().map(score -> score instanceof Number ? ((Number) score).doubleValue() : score).toArray()))));
    }

    @Procedure
    @Description("apoc.redis.zcard(uri, key, {config}) | Execute the 'ZCARD key' command")
    public Stream<LongResult> zcard(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.zcard(key))));
    }

    @Procedure
    @Description("apoc.redis.zrangebyscore(uri, key, min, max, {config}) | Execute the 'ZRANGEBYSCORE key min max' command")
    public Stream<ListResult> zrangebyscore(@Name("uri") String uri, @Name("key") String key, @Name("min") long min, @Name("max") long max, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.zrangebyscore(key, min, max))));
    }

    @Procedure
    @Description("apoc.redis.zrem(uri, key, members, {config}) | Execute the 'ZREM key members' command")
    public Stream<LongResult> zrem(@Name("uri") String uri, @Name("key") String key, @Name("members") List<String> members, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.zrem(key, members.toArray(String[]::new)))));
    }

    // -- Script
    @Procedure
    @Description("apoc.redis.eval(uri, script, keys, {config}) | Execute the 'EVAL script' command")
    public Stream<BooleanResult> eval(@Name("uri") String uri, @Name("script") String script,  @Name("keys") List<String> keys, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.eval(script, keys.toArray(String[]::new)))));
    }

    @Procedure
    @Description("apoc.redis.dispatch(uri, command, output, keys, values, arguments, {config}) | Execute a custom command based on https://lettuce.io/ BaseRedisCommands.dispatch(...)") // TODO - PASSARE ANCHE IL COMMAND TYPE
    public Stream<ObjectResult> dispatch(@Name("uri") String uri, @Name("command") String command, @Name("output") String output, 
                                         @Name(value = "keys", defaultValue = "[]") List<String> keys, 
                                         @Name(value = "values", defaultValue = "[]") List<String> values, 
                                         @Name(value = "arguments", defaultValue = "{}") Map<String, String> arguments, 
                                         @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> {
            try {
                return Stream.of(new ObjectResult(connection.dispatch(command, output, keys, values, arguments)));
            } catch (Exception e) {
                throw new RuntimeException("Dispatch config error", e);
            }
        });
    }

    // -- Key
    @Procedure
    @Description("apoc.redis.copy(uri, source, destination, {config}) | Execute the 'COPY source destination' command")
    public Stream<BooleanResult> copy(@Name("uri") String uri, @Name("source") String source, @Name("destination") String destination, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.copy(source, destination))));
    }

    @Procedure
    @Description("apoc.redis.exists(uri, keys, {config}) | Execute the 'EXISTS keys' command")
    public Stream<LongResult> exists(@Name("uri") String uri, @Name("keys") List<String> keys, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.exists(keys.toArray(String[]::new)))));
    }

    @Procedure
    @Description("apoc.redis.pexpire(uri, key, time, {config}) | Execute the 'PEXPIRE key time' command, or the 'PEPXPIREAT' if the config expireAt=true (default)")
    public Stream<BooleanResult> pexpire(@Name("uri") String uri, @Name("key") String key, @Name("time") long time, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.pexpire(key, time))));
    }

    @Procedure
    @Description("apoc.redis.persist(uri, key, {config}) | Execute the 'PERSIST key' command")
    public Stream<BooleanResult> persist(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.persist(key))));
    }

    @Procedure
    @Description("apoc.redis.pttl(uri, key, {config}) | Execute the 'PTTL key' command")
    public Stream<LongResult> pttl(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.pttl(key))));
    }
    
    // -- Server
    @Procedure
    @Description("apoc.redis.info(uri, {config}) | Execute the 'INFO' command")
    public Stream<StringResult> info(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.info())));
    }

    @Procedure
    @Description("apoc.redis.configGet(uri, parameter, {config}) | Execute the 'CONFIG GET parameter' command")
    public Stream<MapResult> configGet(@Name("uri") String uri, @Name("parameter") String parameter, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new MapResult(connection.configGet(parameter))));
    }

    @Procedure
    @Description("apoc.redis.configSet(uri, parameter, {config}) | Execute the 'CONFIG SET parameter value' command")
    public Stream<StringResult> configSet(@Name("uri") String uri, @Name("parameter") String parameter, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.configSet(parameter, value))));
    }
    

    private <T> T withConnection(String uri, Map<String, Object> config, Function<RedisConnection, T> action) {
        try (RedisConnection connection = getRedisConnection(uri, config)) {
            return action.apply(connection);
        }
    }

    private RedisConnection getRedisConnection(String uri, Map<String, Object> config) {
        try {
            return new RedisConnection(uri, config);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException("Cannot find the Redis client jar. \n" +
                    "Please put the lettuce-core-6.1.1.RELEASE.jar into plugin folder. \n" +
                    "See the documentation: https://neo4j.com/labs/apoc/4.1/database-integration/redis/");
        }
    }

}

