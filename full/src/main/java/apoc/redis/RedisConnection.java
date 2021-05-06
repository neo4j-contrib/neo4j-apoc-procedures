package apoc.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class RedisConnection implements AutoCloseable {
    
    private final String uri;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisClient client;
    private final RedisCommands<String, String> commands;

    public RedisConnection(String uri) {
        this.uri = uri;
        this.client = RedisClient.create(uri);
        this.connection = client.connect();
        this.commands = connection.sync();
    }

    @Override
    public void close() throws Exception {
        this.client.shutdown();
        // todo - controllare che chiuda veramente anche la connection
    }


    // todo - conversione in qualche modo?
    public Object get(String key) {
        return this.commands.get(key);
    }
    
    public String set(String key, String value) {
        // FORSE E MEGLIO METTERE SETGET E BASTA...
        return this.commands.set(key, value);
    }
    
    // todo -serve?
//    public String auth(String key, String value) {
//        return this.commands.
//    }
    
    
    /*
    REDIS STRING COMMANDS
        Long append(K key, V value);
    Long bitcount(K key, long start, long end);
    List<Long> bitfield(K key, BitFieldArgs bitFieldArgs);
    Long bitpos(K key, boolean state, long start, long end);
    
    
    ??     Long bitopAnd(K destination, K... keys);
    ??      Long bitopNot(K destination, K source);
?    Long bitopOr(K destination, K... keys);
?    Long bitopXor(K destination, K... keys);

    Long decrby(K key, long amount);
    V get(K key);
    Long getbit(K key, long offset);

    V getex(K key, GetExArgs args);
    V getrange(K key, long start, long end);



    V getset(K key, V value);


    Double incrbyfloat(K key, double amount);


// todo - forse bastano questi...
    List<KeyValue<K, V>> mget(K... keys);
    String mset(Map<K, V> map);

    // forse con config??
    Boolean msetnx(Map<K, V> map);


    V setGet(K key, V value, SetArgs setArgs);
    String psetex(K key, long milliseconds, V value);

    // in pratica questo è un insert... faccio con config?
    Boolean setnx(K key, V value);


    Long setrange(K key, long offset, V value);


    Long strlen(K key);

     */
    
    
    /*
    REDIS Hashes COMMANDS
    Long hdel(K key, K... fields);
    Boolean hexists(K key, K field);
        V hget(K key, K field);
    Double hincrbyfloat(K key, K field, double amount);
    Map<K, V> hgetall(K key);
    List<KeyValue<K, V>> hmget(K key, K... fields);
    
    // todo - simili...
    String hmset(K key, Map<K, V> map);
    Long hset(K key, Map<K, V> map);

     */
        
    /*
    REDIS Lists COMMANDS
        Long lpush(K key, V... values);
        Long lpushx(K key, V... values);
    Long lrange(ValueStreamingChannel<V> channel, K key, long start, long stop);
        List<V> lpop(K key, long count);
    List<V> rpop(K key, long count);

    Long rpush(K key, V... values);
    Long rpushx(K key, V... values);
     */
        
    
    
    /*
    REDIS Sets COMMANDS
    Long sadd(K key, V... members);
        Long scard(K key);
    Set<V> spop(K key, long count);
    Set<V> smembers(K key);
    Set<V> sunion(K... keys);

     */
        
    /*
    REDIS Sorted Sets COMMANDS
            Long zadd(K key, Object... scoresAndValues);
    Long zcard(K key);
    List<V> zrangebyscore(K key, Range<? extends Number> range);


     */
        
    
    /*
    REDIS SCRIPT COMMANDS
        <T> T eval(String script, ScriptOutputType type, K... keys);
     */
        
        
    /*
    REDIS KEY COMMANDS
        Boolean copy(K source, K destination, CopyArgs copyArgs);
        Long unlink(K... keys);
    Long exists(K... keys);
    Boolean pexpire(K key, long milliseconds);
    Boolean pexpireat(K key, long timestamp);
       Boolean persist(K key);
    Long pttl(K key);
    Boolean renamenx(K key, K newKey);
    List<V> sort(K key);



     */
        
    /*
    REDIS SERVER COMMANDS
    bgsave
        Map<String, String> configGet(String parameter);
    String configSet(String parameter, String value);
    String info();
    String bgsave();

     */
    
    
}
