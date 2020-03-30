package apoc.ttl;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.date.Date.unit;

public class TTL {

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.ttl.expireAtInstant(node,time,'time-unit') - expire node at specified time by setting :TTL label and `ttl` property")
    public void expire(@Name("node") Node node, @Name("time") long time, @Name("timeUnit") String timeUnit) {
        node.addLabel(Label.label("TTL"));
        node.setProperty("ttl",unit(timeUnit).toMillis(time));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.ttl.expireAfterTimeLength(node,timeDelta,'time-unit') - expire node after specified length of time time by setting :TTL label and `ttl` property")
    public void expireIn(@Name("node") Node node, @Name("timeDelta") long time, @Name("timeUnit") String timeUnit) {
        node.addLabel(Label.label("TTL"));
        node.setProperty("ttl",System.currentTimeMillis() + unit(timeUnit).toMillis(time));
    }
}
