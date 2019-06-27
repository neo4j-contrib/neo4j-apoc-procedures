package apoc.config;

import apoc.ApocConfiguration;
import apoc.result.MapResult;
import apoc.util.Util;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

/**
 * @author mh
 * @since 28.10.16
 */
public class Config {

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

    @Description("apoc.config.list | Lists the Neo4j configuration as key,value table")
    @Procedure
    public Stream<ConfigResult> list() {
        Util.checkAdmin(securityContext, "apoc.config.list");
        return ApocConfiguration.list().entrySet().stream().map(e -> new ConfigResult(e.getKey(), e.getValue()));
    }

    @Description("apoc.config.map | Lists the Neo4j configuration as map")
    @Procedure
    public Stream<MapResult> map() {
        Util.checkAdmin(securityContext, "apoc.config.map");
        return Stream.of(new MapResult(ApocConfiguration.list()));
    }
}
