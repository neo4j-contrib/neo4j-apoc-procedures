package apoc.dv;

import org.neo4j.internal.helpers.collection.Pair;

import java.util.HashMap;
import java.util.Map;

public class CSVResource extends VirtualizedResource {

    public CSVResource(String name, Map<String, Object> config) {
        super(name, config, "CSV");
    }

    @Override
    public Pair<String, Map<String, Object>> getProcedureCallWithParams(Object queryParams, Map<String, Object> config) {
        final Map<String, Object> base = Map.of("url", this.url, "config", config, "labels", labels);
        Map<String, Object> map = new HashMap<>();
        map.putAll((Map<String, Object>) queryParams);
        map.putAll(base);
        return Pair.of("CALL apoc.load.csv($url, $config) YIELD map WHERE " + query
                    + " RETURN apoc.create.vNode($labels, map) AS node",
                map);
    }

}
