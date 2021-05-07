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

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

// TODO -> creare classe RedisConnection
public class Redis {

    // -- String
    @Procedure
    @Description("TODO")
    public Stream<StringResult> setGet(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.setGet(key, value))));
    }

    @Procedure
    @Description("TODO")
    public Stream<StringResult> mget(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // String mset(Map<K, V> map);
        // String msetnx(Map<K, V> map); --> config
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.get(key))));
    }

    @Procedure
    @Description("TODO")
    public Stream<LongResult> append(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.append(key, value))));
    }

    @Procedure
    @Description("TODO")
    public Stream<LongResult> incrby(@Name("uri") String uri, @Name("key") String key, @Name("amount") long amount, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.incrby(key, amount))));
    }

    @Procedure
    @Description("TODO")
    public Stream<LongResult> decrby(@Name("uri") String uri, @Name("key") String key, @Name("amount") long amount, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.decrby(key, amount))));
    }

    // -- Hashes
    @Procedure
    @Description("TODO")
    public Stream<LongResult> hdel(@Name("uri") String uri, @Name("key") String key, @Name("fields") List<String> fields, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.hdel(key, fields.toArray(String[]::new)))));
    }

    @Procedure
    @Description("TODO")
    public Stream<BooleanResult> hexists(@Name("uri") String uri, @Name("key") String key, @Name("field") String field, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.hexists(key, field))));
    }

    // che succede con obj result?
    @Procedure
    @Description("TODO")
    public Stream<ObjectResult> hget(@Name("uri") String uri, @Name("key") String key, @Name("field") String field, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ObjectResult(connection.hget(key, field))));
    }

    @Procedure
    @Description("TODO")
    public Stream<DoubleResult> hincrby(@Name("uri") String uri, @Name("key") String key, @Name("field") String field, @Name("amount") long amount, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new DoubleResult(connection.hincrby(key, field, amount))));
    }

    @Procedure
    @Description("TODO")
    public Stream<MapResult> hgetall(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new MapResult(connection.hgetall(key))));
    }

    @Procedure
    @Description("TODO")
    public Stream<BooleanResult> hset(@Name("uri") String uri, @Name("key") String key, @Name("field") String field, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.hset(key, field, value))));
    }


    // -- Lists
    @Procedure
    @Description("TODO")
    public Stream<LongResult> lpush(@Name("uri") String uri, @Name("key") String key, @Name("value") List<String> values, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // lpush - lpushx - rpush - rpushx
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.push(key, values.toArray(String[]::new)))));
    }

    @Procedure
    @Description("TODO")
    public Stream<StringResult> pop(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // lpop - rpop
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.pop(key))));
    }

    @Procedure
    @Description("TODO")
    public Stream<ListResult> lrange(@Name("uri") String uri, @Name("key") String key, @Name("start") long start, @Name("stop") long stop, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // lpop - rpop
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.lrange(key, start, stop))));
    }

    // -- Sets
    @Procedure
    @Description("TODO")
    public Stream<LongResult> sadd(@Name("uri") String uri, @Name("key") String key, @Name("value") List<String> value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.sadd(key, value.toArray(String[]::new)))));
    }

    @Procedure
    @Description("TODO")
    public Stream<LongResult> scard(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.scard(key))));
    }

//    @Procedure
//    @Description("TODO")
//    public Stream<MapResult> spop(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
//        // TODO posso mettere spop e lpop/rpop insieme??
//        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.spop(key, value))));
//    }

    @Procedure
    @Description("TODO")
    public Stream<ListResult> smembers(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.smembers(key))));
    }

    @Procedure
    @Description("TODO")
    public Stream<ListResult> sunion(@Name("uri") String uri, @Name("keys") List<String> keys, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.sunion(keys.toArray(String[]::new)))));
    }

    // -- Sorted Sets
    @Procedure
    @Description("TODO")
    public Stream<LongResult> zadd(@Name("uri") String uri, @Name("key") String key, @Name("value") List<Object> scoresAndValues, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.zadd(key, scoresAndValues.toArray()))));
    }

    @Procedure
    @Description("TODO")
    public Stream<LongResult> zcard(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.zcard(key))));
    }

    @Procedure
    @Description("TODO")
    public Stream<ListResult> zrangebyscore(@Name("uri") String uri, @Name("key") String key, @Name("lower") long lower, @Name("upper") long upper, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new ListResult(connection.zrangebyscore(key, lower, upper))));
    }


    // -- Script
    @Procedure
    @Description("TODO")
    public Stream<BooleanResult> eval(@Name("uri") String uri, @Name("script") String script, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.eval(script))));
    }

    @Procedure
    @Description("TODO") // TODO - PASSARE ANCHE IL COMMAND TYPE
    public Stream<ObjectResult> dispatch(@Name("uri") String uri, @Name("command") String command, @Name("output") String output, @Name(value = "keys", defaultValue = "[]") List<String> keys, @Name(value = "values", defaultValue = "[]") List<String> values, @Name(value = "arguments", defaultValue = "{}") Map<String, String> arguments, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return withConnection(uri, config, connection -> {
            try {
                return Stream.of(new ObjectResult(connection.dispatch(command, output, keys, values, arguments)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


    // -- Key
    @Procedure
    @Description("TODO")
    public Stream<BooleanResult> copy(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.copy(key, value))));
    }

    @Procedure
    @Description("TODO")
    public Stream<LongResult> exists(@Name("uri") String uri, @Name("key") List<String> key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.exists(key.toArray(String[]::new)))));
    }

    @Procedure
    @Description("TODO")
    public Stream<BooleanResult> pexpire(@Name("uri") String uri, @Name("key") String key, @Name("time") long time, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // pexpire e pexpireat
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.pexpire(key, time))));
    }

    @Procedure
    @Description("TODO")
    public Stream<BooleanResult> persist(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new BooleanResult(connection.persist(key))));
    }

    @Procedure
    @Description("TODO")
    public Stream<LongResult> pttl(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new LongResult(connection.pttl(key))));
    }


    // -- Server
    @Procedure
    @Description("TODO")
    public Stream<StringResult> info(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.info())));
    }

    @Procedure
    @Description("TODO")
    public Stream<StringResult> bgSave(@Name("uri") String uri, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.bgSave())));
    }

    @Procedure
    @Description("TODO")
    public Stream<MapResult> configGet(@Name("uri") String uri, @Name("key") String key, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new MapResult(connection.configGet(key))));
    }

    @Procedure
    @Description("TODO")
    public Stream<StringResult> configSet(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return withConnection(uri, config, connection -> Stream.of(new StringResult(connection.configSet(key, value))));
    }


    // TODO -> STO COSO CHE FA? this.commands.dispatch()


    // INCR -> incrementa atomicamente numero, INCRBY --> incrementa numero di n, DECR , DECRBY

    // DEL --> DELETE A KAY


    // TTL -> 
    /*
    Redis can be told that a key should only exist for a certain length of time. 
    This is accomplished with the EXPIRE and TTL commands, and by the similar PEXPIRE and PTTL commands that operate using time in milliseconds instead of seconds.
    
    

     */


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


/*
 * DECR, DECRBY, DEL, EXISTS, EXPIRE, GET, GETSET, HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HKEYS, HLEN, HMGET, HMSET, HSET, HVALS, INCR, INCRBY, KEYS, LINDEX, LLEN, LPOP, LPUSH, LRANGE, LREM, LSET, LTRIM, MGET, MSET, MSETNX, MULTI, PEXPIRE, RENAME, RENAMENX, RPOP, RPOPLPUSH, RPUSH, SADD, SCARD, SDIFF, SDIFFSTORE, SET, SETEX, SETNX, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS, SMOVE, SORT, SPOP, SRANDMEMBER, SREM, SUNION, SUNIONSTORE, TTL, TYPE, ZADD, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYSCORE, ZREVRANGE, ZSCORE
 * */