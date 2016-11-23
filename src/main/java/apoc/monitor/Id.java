package apoc.monitor;

import apoc.Description;
import apoc.result.IdsResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import javax.management.ObjectName;
import java.util.stream.Stream;

public class Id extends Monitor {

    private static final String JMX_OBJECT_NAME = "Primitive count";
    private static final String NODE_IDS_KEY = "NumberOfNodeIdsInUse";
    private static final String REL_IDS_KEY = "NumberOfRelationshipIdsInUse";
    private static final String PROP_IDS_KEY = "NumberOfPropertyIdsInUse";
    private static final String REL_TYPE_IDS_KEY = "NumberOfRelationshipTypeIdsInUse";

    @Context
    public GraphDatabaseService database;

    @Procedure
    @Description("apoc.monitor.ids() returns the object ids in use for this neo4j instance")
    public Stream<IdsResult> ids() {
        return Stream.of(getIdsInUse());
    }

    private IdsResult getIdsInUse() {
        ObjectName objectName = getObjectName(database, JMX_OBJECT_NAME);

        return new IdsResult(
                (long) getAttribute(objectName, NODE_IDS_KEY),
                (long) getAttribute(objectName, REL_IDS_KEY),
                (long) getAttribute(objectName, PROP_IDS_KEY),
                (long) getAttribute(objectName, REL_TYPE_IDS_KEY)
        );
    }
}
