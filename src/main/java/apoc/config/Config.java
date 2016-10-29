package apoc.config;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Revertable;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.procedure.Context;
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
    public KernelTransaction tx;

    @Description("apoc.config.list | Lists the Neo4j configuration as key,value table")
    @Procedure
    public Stream<ConfigResult> list() {
        try (Revertable t = tx.overrideWith(AccessMode.Static.FULL)) {
            return ApocConfiguration.list().entrySet().stream().map(e -> new ConfigResult(e.getKey(), e.getValue()));
        }
    }

    @Description("apoc.config.map | Lists the Neo4j configuration as map")
    @Procedure
    public Stream<MapResult> map() {
        try (Revertable t = tx.overrideWith(AccessMode.Static.FULL)) {
            return Stream.of(new MapResult(ApocConfiguration.list()));
        }
    }
}
