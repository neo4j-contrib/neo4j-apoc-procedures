package apoc.monitor;

import apoc.result.StoreInfoResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class Store  {

    private static final String JMX_OBJECT_NAME = "Store file sizes";
    private static final String LOG_SIZE = "LogicalLogSize";
    private static final String STRING_SIZE = "StringStoreSize";
    private static final String ARRAY_SIZE = "ArrayStoreSize";
    private static final String REL_SIZE = "RelationshipStoreSize";
    private static final String PROP_SIZE = "PropertyStoreSize";
    private static final String TOTAL_SIZE = "TotalStoreSize";
    private static final String NODE_SIZE = "NodeStoreSize";

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.monitor.store() returns informations about the sizes of the different parts of the neo4j graph store")
    public Stream<StoreInfoResult> store() {

        // TODO: fix JMX
        return Stream.empty();
        /*ObjectName objectName = getObjectName(db, JMX_OBJECT_NAME);

        StoreInfoResult storeInfo = new StoreInfoResult(
                getAttribute(objectName, LOG_SIZE),
                getAttribute(objectName, STRING_SIZE),
                getAttribute(objectName, ARRAY_SIZE),
                getAttribute(objectName, REL_SIZE),
                getAttribute(objectName, PROP_SIZE),
                getAttribute(objectName, TOTAL_SIZE),
                getAttribute(objectName, NODE_SIZE));

        return Stream.of(storeInfo);*/
    }


}
