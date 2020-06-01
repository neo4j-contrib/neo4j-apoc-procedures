package apoc;

import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Map;

public class AsyncIndexingGraphDatabaseState extends SyncIndexingGraphDatabaseState {

    @Override
    public Map<String, String> getGraphDatabaseConfig() {
        return MapUtil.genericMap("apoc.autoIndex.enabled", "true",
                "apoc.autoIndex.async", "true",
                "apoc.autoIndex.configUpdateInterval", "-1");
    }

}
