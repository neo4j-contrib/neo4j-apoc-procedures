package apoc.monitor;

import org.neo4j.procedure.Description;
import apoc.result.KernelInfoResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import javax.management.ObjectName;
import java.util.stream.Stream;

import static org.neo4j.jmx.JmxUtils.getAttribute;
import static org.neo4j.jmx.JmxUtils.getObjectName;

public class Kernel {

    private static final String JMX_OBJECT_NAME = "Kernel";
    private static final String READ_ONLY = "ReadOnly";
    private static final String KERNEL_VERSION = "KernelVersion";
    private static final String STORE_ID = "StoreId";
    private static final String START_TIME = "KernelStartTime";
    private static final String DB_NAME = "DatabaseName";
    private static final String STORE_LOG_VERSION = "StoreLogVersion";
    private static final String STORE_CREATION_DATE = "StoreCreationDate";


    @Context
    public GraphDatabaseService database;

    @Procedure
    @Description("apoc.monitor.kernel() returns informations about the neo4j kernel")
    public Stream<KernelInfoResult> kernel() {
        ObjectName objectName = getObjectName(database, JMX_OBJECT_NAME);

        KernelInfoResult info = new KernelInfoResult(
                getAttribute(objectName, READ_ONLY),
                getAttribute(objectName, KERNEL_VERSION),
                getAttribute(objectName, STORE_ID),
                getAttribute(objectName, START_TIME),
                getAttribute(objectName, DB_NAME),
                getAttribute(objectName, STORE_LOG_VERSION),
                getAttribute(objectName, STORE_CREATION_DATE));

        return Stream.of(info);
    }

}
