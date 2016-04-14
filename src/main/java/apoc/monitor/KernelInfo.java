package apoc.monitor;

import apoc.Description;
import apoc.result.KernelInfoResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import javax.management.ObjectName;
import java.util.Date;
import java.util.stream.Stream;

public class KernelInfo extends Monitor {

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
    @Description("apoc.monitor.kernelInfo() returns informations about the neo4j kernel")
    public Stream<KernelInfoResult> kernelInfo() {
        Date time = (Date) getAttribute(getObjectName(database, JMX_OBJECT_NAME), START_TIME);
        return Stream.of(getKernelInfo());
    }

    private KernelInfoResult getKernelInfo() {
        ObjectName objectName = getObjectName(database, JMX_OBJECT_NAME);

        return new KernelInfoResult(
                (Boolean) getAttribute(objectName, READ_ONLY),
                String.valueOf(getAttribute(objectName, KERNEL_VERSION)),
                String.valueOf(getAttribute(objectName, STORE_ID)),
                ((Date) getAttribute(objectName, START_TIME)).getTime(),
                String.valueOf(getAttribute(objectName, DB_NAME)),
                (long) getAttribute(objectName, STORE_LOG_VERSION),
                ((Date) getAttribute(objectName, STORE_CREATION_DATE)).getTime()
        );
    }

}
