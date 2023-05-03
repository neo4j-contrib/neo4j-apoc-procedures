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

import io.lettuce.core.ScriptOutputType;

import java.util.List;
import java.util.Map;

public interface IRedisConnection<T> extends AutoCloseable {
    // -- String
    T get(T key);
    T getSet(T key, T value);
    long append(T key, T value);
    long incrby(T key, long amount);

    // -- Hashes
    long hdel(T key, List<Object> fields);
    boolean hexists(T key, T field);
    T hget(T key, T field);
    long hincrby(T key, T field, long amount);
    boolean hset(T key, T field, T value);
    Map<String, Object> hgetall(T key);

    // -- Lists
    long push(T key, List<Object> values);
    T pop(T key);
    List<Object> lrange(T key, long start, long stop);

    // -- Sets
    long sadd(T key, List<Object> members);
    T spop(T key);
    long scard(T key);
    List<Object> smembers(T key);
    List<Object> sunion(List<Object> keys);

    // -- Sorted Sets
    long zadd(T key, Object... scoresAndMembers);
    long zcard(T key);
    List<Object> zrangebyscore(T source, long min, long max);
    long zrem(T source, List<Object> members);

    // -- Script
    T eval(String script, ScriptOutputType outputType, List<Object> keys, List<Object> values);

    // -- Key
    boolean copy(T source, T destination);
    long exists(List<Object> key);
    boolean pexpire(T key, long time, boolean isExpireAt);
    boolean persist(T key);
    long pttl(T key);

    // -- Server
    String info();
    String configSet(String parameter, String value);
    Map<String, Object> configGet(String parameter);
}
