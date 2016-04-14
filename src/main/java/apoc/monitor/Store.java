package apoc.monitor;

import apoc.Description;
import apoc.result.StoreInfoResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import javax.management.ObjectName;
import java.util.stream.Stream;

public class Store extends Monitor {

    private static final String JMX_OBJECT_NAME = "Store file sizes";
    private static final String LOG_SIZE = "LogicalLogSize";
    private static final String STRING_SIZE = "StringStoreSize";
    private static final String ARRAY_SIZE = "ArrayStoreSize";
    private static final String REL_SIZE = "RelationshipStoreSize";
    private static final String PROP_SIZE = "PropertyStoreSize";
    private static final String TOTAL_SIZE = "TotalStoreSize";
    private static final String NODE_SIZE = "NodeStoreSize";

    @Context
    public GraphDatabaseService database;

    @Procedure
    @Description("apoc.monitor.store() returns informations about the sizes of the different parts of the neo4j graph store")
    public Stream<StoreInfoResult> store() {
        return Stream.of(getStoreInfo());
    }

    private StoreInfoResult getStoreInfo() {
        ObjectName objectName = getObjectName(database, JMX_OBJECT_NAME);

        return new StoreInfoResult(
                (long) getAttribute(objectName, LOG_SIZE),
                (long) getAttribute(objectName, STRING_SIZE),
                (long) getAttribute(objectName, ARRAY_SIZE),
                (long) getAttribute(objectName, REL_SIZE),
                (long) getAttribute(objectName, PROP_SIZE),
                (long) getAttribute(objectName, TOTAL_SIZE),
                (long) getAttribute(objectName, NODE_SIZE)
        );
    }


}
