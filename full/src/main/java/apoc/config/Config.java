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
package apoc.config;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.MapResult;
import apoc.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_CONFIG_JOBS_POOL_NUM_THREADS;
import static apoc.ApocConfig.APOC_CONFIG_JOBS_QUEUE_SIZE;
import static apoc.ApocConfig.APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS;
import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.APOC_TTL_ENABLED;
import static apoc.ApocConfig.APOC_TTL_LIMIT;
import static apoc.ApocConfig.APOC_TTL_SCHEDULE;
import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.APOC_UUID_FORMAT;
import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;

/**
 * @author mh
 * @since 28.10.16
 */
@Extended
public class Config {
    
    // some config keys are hard-coded because belong to `core`, which is no longer accessed from `extended`
    private static final Set<String> WHITELIST_CONFIGS = Set.of(
            // apoc.import.
            APOC_IMPORT_FILE_ENABLED,
            APOC_IMPORT_FILE_USE_NEO4J_CONFIG,
            APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM,

            // apoc.export.
            APOC_EXPORT_FILE_ENABLED,
            
            // apoc.trigger.
            APOC_TRIGGER_ENABLED,
            "apoc.trigger.refresh",

            // apoc.uuid.
            APOC_UUID_ENABLED,
            APOC_UUID_FORMAT,
            
            // apoc.ttl.
            APOC_TTL_SCHEDULE,
            APOC_TTL_ENABLED,
            APOC_TTL_LIMIT,
            
            // apoc.jobs.
            APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS,
            APOC_CONFIG_JOBS_POOL_NUM_THREADS,
            APOC_CONFIG_JOBS_QUEUE_SIZE,
            
            // apoc.http.
            "apoc.http.timeout.connect",
            "apoc.http.timeout.read",
            
            // apoc.custom.
            CUSTOM_PROCEDURES_REFRESH,

            // apoc.spatial. - other configs can have sensitive credentials
            "apoc.spatial.geocode.osm.throttle",
            "apoc.spatial.geocode.google.throttle"
    );
    
    public static class ConfigResult {
        public final String key;
        public final Object value;

        public ConfigResult(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    @Context
    public SecurityContext securityContext;

    @Context
    public ProcedureCallContext callContext;

    @Context
    public DependencyResolver dependencyResolver;

    @Description("apoc.config.list | Lists the Neo4j configuration as key,value table")
    @Procedure
    public Stream<ConfigResult> list() {
        Util.checkAdmin(securityContext, callContext,"apoc.config.list");
        Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();
        return getApocConfigs(config)
                .map(s -> new ConfigResult(s, config.getString(s)));
    }

    @Description("apoc.config.map | Lists the Neo4j configuration as map")
    @Procedure
    public Stream<MapResult> map() {
        Util.checkAdmin(securityContext,callContext, "apoc.config.map");
        Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();
        Map<String, Object> configMap = getApocConfigs(config)
                .collect(Collectors.toMap(s -> s, s -> config.getString(s)));
        return Stream.of(new MapResult(configMap));
    }

    private static Stream<String> getApocConfigs(Configuration config) {
        // we use startsWith(..) because we can have e.g. a config `apoc.uuid.enabled.<dbName>`
        return Iterators.stream(config.getKeys())
                .filter(conf -> WHITELIST_CONFIGS.stream().anyMatch(conf::startsWith));
    }
}
