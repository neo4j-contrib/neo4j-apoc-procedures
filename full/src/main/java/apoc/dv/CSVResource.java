package apoc.dv;

import java.util.HashMap;
import java.util.Map;

public class CSVResource extends VirtualizedResource {

    public CSVResource(String name, Map<String, Object> config) {
        super(name, config, "CSV");
    }

    @Override
    protected Map<String, Object> getProcedureParameters(Object queryParams, Map<String, Object> config) {
        final Map<String, Object> base = Map.of("url", this.url, "config", config, "labels", labels);
        Map<String, Object> map = new HashMap<>();
        map.putAll((Map<String, Object>) queryParams);
        map.putAll(base);
        return map;
    }

    @Override
    protected String getProcedureCall(Map<String, Object> config) {
        return "CALL apoc.load.csv($url, $config) YIELD map WHERE " + query
                + " RETURN apoc.create.vNode($labels, map) AS node";
    }

}
