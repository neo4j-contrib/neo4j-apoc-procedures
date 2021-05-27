package apoc.redis;

//import com.google.common.reflect.ClassPath;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.Reflection;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.ListUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class RedisConnection implements AutoCloseable {
    
    private final RedisClient client;
    private final RedisCommands<String, String> commands;
    private final RedisConfig conf;
    private final RedisCodec<String, String> codec;

    public RedisConnection(String uri, Map<String, Object> config) {
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

    // -- String
    public String get(String key) {
        return this.commands.get(key);
    }
    
    public String setGet(String key, String value) {
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

    // -- Hashes
    public long hdel(String key, String... fields) {
        return this.commands.hdel(key, fields);
    }

    public boolean hexists(String key, String field) {
        return this.commands.hexists(key, field);
    }

    public String hget(String key, String field) {
        return this.commands.hget(key, field);
    }

    public long hincrby(String key, String field, long amount) {
        return this.commands.hincrby(key, field, amount);
    }

    public Long hset(String key, Map<String, String> mapFields) {
        return this.commands.hset(key, mapFields);
    }

    public Map<String, Object> hgetall(String key) {
        return Collections.unmodifiableMap(this.commands.hgetall(key));
    }

    // -- Lists
    public Long push(String key, String... values) {
        return this.conf.isRight() 
                ? this.commands.rpush(key, values) 
                : this.commands.lpush(key, values);
    }

    public String pop(String key) {
        return this.conf.isRight()
                ? this.commands.rpop(key)
                : this.commands.lpop(key);
    }

    public List<Object> lrange(String key, long start, long stop) {
        return new ArrayList<>(this.commands.lrange(key, start, stop));
    }

    // -- Sets
    public Long sadd(String key, String... members) {
        return this.commands.sadd(key, members);
    }

    public String spop(String key) {
        return this.commands.spop(key);
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

    // -- Sorted Sets
    public long zadd(String key, Object... scoresAndMembers) {
        return this.commands.zadd(key, scoresAndMembers);
    }
    
    public Long zcard(String key) {
        return this.commands.zcard(key);
    }
    
    public List<Object> zrangebyscore(String source, long min, long max) {
        return new ArrayList<>(this.commands.zrangebyscore(source, Range.create(min, max)));
    }
    
    public long zrem(String source, String... members) {
        return this.commands.zrem(source, members);
    }
    
    // -- Script
    public Object eval(String script, ScriptOutputType outputType, String[] keys, String... values) {
        return this.commands.eval(script, outputType, keys, values);
    }
    
    public Object dispatch(String command, String output, List<String> keys, List<String> values, Map<String, String> arguments) {
        CommandOutput cons;
        try {
            Class<?> clazz = Class.forName(CommandOutput.class.getPackageName() + "." + output);
            cons = (CommandOutput) clazz.getConstructor(RedisCodec.class).newInstance(this.codec);
        } catch (Exception e) {
            throw new UnsupportedOperationException("Output type not supported: " + output);
        }
        final CommandArgs<String, String> commandArgs = new CommandArgs<>(this.codec);
        if (CollectionUtils.isNotEmpty(keys)) {
            commandArgs.addKeys(keys);
        }
        if (CollectionUtils.isNotEmpty(values)) {
            commandArgs.addValues(values);
        }
        if (MapUtils.isNotEmpty(arguments)) {
            commandArgs.add(arguments);
        }
        
        return this.commands.dispatch(CommandType.valueOf(command), cons, commandArgs);
    }

    // -- Key
    public boolean copy(String source, String destination) {
        return this.commands.copy(source, destination);
    }

    public long exists(String... key) {
        return this.commands.exists(key);
    }

    public boolean pexpire(String key, long time, boolean isExpireAt) {
        return isExpireAt
                ? this.commands.pexpireat(key, time)
                : this.commands.pexpire(key, time);
    }
    
    public boolean persist(String key) {
        return this.commands.persist(key);
    }

    public long pttl(String key) {
        return this.commands.pttl(key);
    }

    // -- Server
    public String info() {
        return this.commands.info();
    }

    public String configSet(String parameter, String value) {
        return this.commands.configSet(parameter, value);
    }

    public Map<String, Object> configGet(String parameter) {
        return Collections.unmodifiableMap(this.commands.configGet(parameter));
    }
}
