/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.redis;

import apoc.util.MissingDependencyException;
import apoc.util.Util;

import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static io.lettuce.core.RedisURI.DEFAULT_TIMEOUT;

public class RedisConfig {
    public enum Codec { 
        STRING(StringRedisConnection.class), BYTE_ARRAY(ByteArrayRedisConnection.class);

        private Class<? extends RedisConnection> redisConnectionClass;

        Codec(Class<? extends RedisConnection> redisConnectionClass) {
            this.redisConnectionClass = redisConnectionClass;
        }

        public RedisConnection getRedisConnection(String uri, Map<String, Object> config) {
            try {
                RedisConfig redisConfig = new RedisConfig(config);
                Constructor<?> constructor = redisConnectionClass.getConstructor(String.class, RedisConfig.class);
                return (RedisConnection) constructor.newInstance(uri, redisConfig);
            } catch (NoClassDefFoundError e) {
                throw new MissingDependencyException("Cannot find the Redis client jar. \n" +
                        "Please put the lettuce-core-6.1.9.RELEASE.jar into plugin folder. \n" +
                        "See the documentation: https://neo4j.com/labs/apoc/4.1/database-integration/redis/");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private final Charset charset;
    private final Duration timeout;

    private final boolean autoReconnect;
    private final boolean right;

    private final Charset scriptCharset;
    private final Codec codec;

    public RedisConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.charset = Charset.forName((String) config.getOrDefault("charset", "UTF-8"));
        this.timeout = Duration.ofSeconds((long) config.getOrDefault("timeout", DEFAULT_TIMEOUT));
        this.scriptCharset = Charset.forName((String) config.getOrDefault("scriptCharset", "UTF-8"));
        this.autoReconnect = Util.toBoolean(config.getOrDefault("autoReconnect", true));
        this.right = Util.toBoolean(config.getOrDefault("right", true));
        this.codec = Codec.valueOf((config.getOrDefault("codec", Codec.STRING.name()).toString().toUpperCase()));
    }

    public boolean isRight() {
        return right;
    }

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public Charset getScriptCharset() {
        return scriptCharset;
    }

    public Charset getCharset() {
        return charset;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public Codec getCodec() {
        return codec;
    }
}
