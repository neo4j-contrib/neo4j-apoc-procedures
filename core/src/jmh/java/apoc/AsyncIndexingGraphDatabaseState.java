package apoc;

import java.util.Map;
import org.neo4j.internal.helpers.collection.MapUtil;

public class AsyncIndexingGraphDatabaseState extends SyncIndexingGraphDatabaseState {

    @Override
    public Map<String, String> getGraphDatabaseConfig() {
        return MapUtil.genericMap(
                "apoc.autoIndex.enabled",
                "true",
                "apoc.autoIndex.async",
                "true",
                "apoc.autoIndex.configUpdateInterval",
                "-1");
    }
}
