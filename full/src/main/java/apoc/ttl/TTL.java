package apoc.ttl;

import apoc.Extended;
import apoc.TTLConfig;
import apoc.util.MapUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.Map;

import static apoc.date.Date.unit;

@Extended
public class TTL {

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.ttl.expire(node,time,'time-unit') - expire node at specified time by setting :TTL label and `ttl` property")
    public void expire(@Name("node") Node node, @Name("time") long time, @Name("timeUnit") String timeUnit) {
        node.addLabel(Label.label("TTL"));
        node.setProperty("ttl",unit(timeUnit).toMillis(time));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.ttl.expireIn(node,timeDelta,'time-unit') - expire node after specified length of time time by setting :TTL label and `ttl` property")
    public void expireIn(@Name("node") Node node, @Name("timeDelta") long time, @Name("timeUnit") String timeUnit) {
        node.addLabel(Label.label("TTL"));
        node.setProperty("ttl",System.currentTimeMillis() + unit(timeUnit).toMillis(time));
    }

    @Context
    public TTLConfig ttlConfig;

    @Context
    public GraphDatabaseAPI db;

    @UserFunction
    public Map<String, Object> config() {
        TTLConfig.Values values = ttlConfig.configFor(db);
        return MapUtil.map(
                "enabled", values.enabled,
                "schedule", values.schedule,
                "limit", values.limit
        );
    }
}
