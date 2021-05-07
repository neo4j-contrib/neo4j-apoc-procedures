package apoc.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// TODO - MAGARI EVAL LO METTO IN UN ALTRO .. poi vedo

public class RedisConnection implements AutoCloseable {
    
    private final RedisClient client;
    private final RedisCommands<String, String> commands;
    private final RedisConfig conf;
    private final RedisCodec<String, String> codec;

    public RedisConnection(String uri, Map<String, Object> config) { // todo - magari metto il Charset configurabile...
        // todo - così se mi serve in altri metodi ce l'ho
        this.conf = new RedisConfig(config);
        this.client = RedisClient.create(uri);
        this.client.setDefaultTimeout(conf.getTimeout());
        this.client.setOptions(ClientOptions.builder()
                .scriptCharset(conf.getScriptCharset())
                .autoReconnect(conf.isAutoReconnect())
                .build());
        
        this.codec = new StringCodec(conf.getCharset());
        
        StatefulRedisConnection<String, String> connection = client.connect(new StringCodec(conf.getCharset()));
        this.commands = connection.sync();
    }

    @Override
    public void close() {
        this.client.shutdown();
    }


    // todo - conversione in qualche modo?
    public String get(String key) {
        return this.commands.get(key);
    }
    
    public String setGet(String key, String value) {
        // FORSE E MEGLIO METTERE SETGET E BASTA..., metto SetArgs con degli if vari e faccio un test...

        return this.commands.setGet(key, value);
    }

    public long append(String key, String value) {
        return this.commands.append(key, value);
    }

    public long incrby(String key, long amount) {
        return this.commands.incrby(key, amount);
    }

    public long decrby(String key, long amount) {
        return this.commands.decrby(key, amount);
    }
    

    
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

    
    public long hdel(String key, String... fields) {
        return this.commands.hdel(key, fields);
    }

    public boolean hexists(String key, String field) {
        return this.commands.hexists(key, field);
    }

    public String hget(String key, String field) {
        return this.commands.hget(key, field);
    }

    public double hincrby(String key, String field, long amount) {
        return this.commands.hincrby(key, field, amount);
    }

    public Boolean hset(String key, String field, String value) {
        return this.commands.hset(key, field, value);
    }

    public Map<String, Object> hgetall(String key) {
        return Collections.unmodifiableMap(this.commands.hgetall(key));
    }

//    public Long lpush(String key, String... values) {
//        return this.commands.lpush(key, values);
//    }

    public String pop(String key) {
        // TODO - SWITCH CASE !!! spop, lpop, rpop
        return this.commands.spop(key);
    }
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

    public Long push(String key, String... values) {
        // TODO TODO swith case
        return this.commands.rpush(key, values);
    }

//    public String pop(String key) {
//        // TODO TODO swith case
//        return this.commands.rpop(key);
//    }

    public Long sadd(String key, String... members) {
        return this.commands.sadd(key, members);
    }

    public Long scard(String key) {
        return this.commands.scard(key);
    }

    public List<Object> smembers(String key) {
        return new ArrayList<>(this.commands.smembers(key));
    }
    
    public List<Object> sunion(String... keys) {
        return new ArrayList<>(this.commands.sunion(keys));
    }
    
    public List<Object> lrange(String key, long start, long stop) {
        return new ArrayList<>(this.commands.lrange(key, start, stop));
    }
    /*
    REDIS Sorted Sets COMMANDS
            Long zadd(K key, Object... scoresAndValues);
    Long zcard(K key);
    List<V> zrangebyscore(K key, Range<? extends Number> range);


     */


    public long zadd(String key, Object... scoresAndValues) {
        return this.commands.zadd(key, scoresAndValues);
    }

    // todo - non ho capito perché range è parametrizzato, un long non va bene?
    public Long zcard(String key) {
        return this.commands.zcard(key);
    }


    // todo - non ho capito perché range è parametrizzato, un long non va bene?
    public List<Object> zrangebyscore(String source, long lower, long upper) {
        return new ArrayList<>(this.commands.zrangebyscore(source, Range.create(lower, upper)));
    }
    
    /*
    REDIS SCRIPT COMMANDS
        <T> T eval(String script, ScriptOutputType type, K... keys);
     */

    
    public boolean eval(String script) {
        /*
        *  * Synchronous executed commands for Scripting. {@link java.lang.String Lua scripts} are encoded by using the configured
         * {@link io.lettuce.core.ClientOptions#getScriptCharset() charset}.
        * */
        
        // mettere un config con expire at?
        return this.commands.eval(script, conf.getScriptOutputType());
    }
    
    public Object dispatch(String command, String output, List<String> keys, List<String> values, Map<String, String> arguments) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        // TODO - LINKO STA PAGINA https://lettuce.io/core/6.0.0.RELEASE/api/io/lettuce/core/output/CommandOutput.html E BUONANOTTE
        
        Class<?> clazz = Class.forName(CommandOutput.class.getPackageName() + "." + output);
        CommandOutput cons = (CommandOutput) clazz.getConstructor(RedisCodec.class).newInstance(this.codec);

        final CommandArgs<String, String> commandArgs = new CommandArgs<>(this.codec)
                .addKeys(keys)
                .addValues(values)
                .add(arguments);
        
        return this.commands.dispatch(CommandType.valueOf(command), cons, commandArgs);
    }
        
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



    public String configSet(String key, String fields) {
        return this.commands.configSet(key, fields);
    }

     */


    public boolean copy(String source, String destination) {
        return this.commands.copy(source, destination);
    }

    public long exists(String... key) {
        // mettere un config con expire at?
        return this.commands.exists(key);
    }

    public boolean pexpire(String key, long time) {
        // mettere un config con expire at?
        return this.commands.pexpire(key, time);
    }
    
    public boolean persist(String key) {
        return this.commands.persist(key);
    }

    public long pttl(String key) {
        return this.commands.pttl(key);
    }
    
        
    /*
    REDIS SERVER COMMANDS
    bgsave
        Map<String, String> configGet(String parameter);
    String configSet(String parameter, String value);
    String info();
    String bgsave();

     */
    
    public String info() {
        return this.commands.info();
    }

    // todo -- sto coso che fa?
    public String bgSave() {
        return this.commands.bgsave();
    }

    public String configSet(String key, String fields) {
        return this.commands.configSet(key, fields);
    }

    public Map<String, Object> configGet(String key) {
        return Collections.unmodifiableMap(this.commands.configGet(key));
    }
    
}
