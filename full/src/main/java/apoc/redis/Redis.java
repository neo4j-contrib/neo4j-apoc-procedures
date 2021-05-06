package apoc.redis;

import apoc.result.MapResult;
import apoc.result.StringResult;
import apoc.util.MissingDependencyException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

// TODO -> creare classe RedisConnection
public class Redis {
    
    
    
    // todo - get, count, first, insert, delete, update, exists

    @Procedure
    @Description("TODO")
    public Stream<StringResult> set(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {

        return withConnection(uri, connection -> Stream.of(new StringResult(connection.set(key, value))));

//        System.out.println("key2 - " + key2);

//        final String key1 = syncCommands.set("key", "Hello, Redis!");

//        connection.close();
//        redisClient.shutdown(); // si può chiudere con try with rea
        
//        return Stream.of(new MapResult(Map.of("1", "2"))); // ritornare il risultato --> TODO TODO TODO RedisCommands (vedere quanta roba c'è)
    }

    @Procedure
    @Description("TODO")
    public Stream<MapResult> set2(@Name("uri") String uri, @Name("key") String key, @Name("value") String value, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return Stream.empty();
    }


        // INCR -> incrementa atomicamente numero, INCRBY --> incrementa numero di n, DECR , DECRBY
    
    // DEL --> DELETE A KAY
    
    
    // TTL -> 
    /*
    Redis can be told that a key should only exist for a certain length of time. 
    This is accomplished with the EXPIRE and TTL commands, and by the similar PEXPIRE and PTTL commands that operate using time in milliseconds instead of seconds.
    
    

     */
    
    
    private <T> T withConnection(String uri, Function<RedisConnection, T> action) {
        // TODO - CONTROLARE SE SERVE QUALCHE JAR....
        try (RedisConnection connection = getRedisConnection(uri)) {
            return action.apply(connection);
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO - SERVE STO COSO?
        }
    }
    
    private RedisConnection getRedisConnection(String uri) {
        try {
            return new RedisConnection(uri);
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