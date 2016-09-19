package apoc.monitor;

import org.neo4j.procedure.Description;
import apoc.result.IdsResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import javax.management.ObjectName;
import java.util.stream.Stream;

import static org.neo4j.jmx.JmxUtils.getAttribute;
import static org.neo4j.jmx.JmxUtils.getObjectName;

public class Ids {

    private static final String JMX_OBJECT_NAME = "Primitive count";
    private static final String NODE_IDS_KEY = "NumberOfNodeIdsInUse";
    private static final String REL_IDS_KEY = "NumberOfRelationshipIdsInUse";
    private static final String PROP_IDS_KEY = "NumberOfPropertyIdsInUse";
    private static final String REL_TYPE_IDS_KEY = "NumberOfRelationshipTypeIdsInUse";

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.monitor.ids() returns the object ids in use for this neo4j instance")
    public Stream<IdsResult> ids() {
        return Stream.of(getIdsInUse());
    }

    private IdsResult getIdsInUse() {
        ObjectName objectName = getObjectName(db, JMX_OBJECT_NAME);

        return new IdsResult(
                getAttribute(objectName, NODE_IDS_KEY),
                getAttribute(objectName, REL_IDS_KEY),
                getAttribute(objectName, PROP_IDS_KEY),
                getAttribute(objectName, REL_TYPE_IDS_KEY)
        );
    }
}
