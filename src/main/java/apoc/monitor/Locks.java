package apoc.monitor;

import org.neo4j.procedure.Description;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static org.neo4j.jmx.JmxUtils.*;

/**
 * @author kv
 * @since 6.05.16
 */
public class Locks {
    private static final String JMX_OBJECT_NAME = "Locking";
    private static final String JMX_CONTENDED_LOCKS_KEY = "getContendedLocks";
    private static final String JMX_LOCKS_KEY = "Locks";
    private static final String JMX_NR_ADVERTED_DEADLOCKS_KEY = "NumberOfAvertedDeadlocks";
    private static final String JMX_ITEM_RESOURCE_TYPE = "resourceType";
    private static final String JMX_ITEM_DESCRIPTION = "description";
    private static final String JMX_ITEM_RESOURCE_ID = "resourceId";

    @Context
    public GraphDatabaseService database;


    @Procedure("apoc.monitor.locks")
    @Description("apoc.monitor.locks(minWaitTime) yield advertedDeadLocks, lockCount, contendedLockCount, minimumWaitTimeMs, contendedLocks, info")
    public Stream<LockInfoResult> lockInfo(@Name("minWaitTime") Long minWaitTime) {

        if (minWaitTime == null || minWaitTime < 0) minWaitTime = 0L;

        ObjectName objectName = getObjectName(database, JMX_OBJECT_NAME);
        CompositeData[] locks = getAttribute(objectName, JMX_LOCKS_KEY);
        List<Map<String, Object>> lockInfos = getContentedLocks(objectName, minWaitTime);

        Long avertedDeadLocks = getAttribute(objectName, JMX_NR_ADVERTED_DEADLOCKS_KEY);

        LockInfoResult info = new LockInfoResult(minWaitTime, locks.length, avertedDeadLocks, lockInfos.size(), lockInfos);
        return Stream.of(info);
    }

    public List<Map<String, Object>> getContentedLocks(ObjectName objectName, @Name("minWaitTime") Long minwaittime) {
        CompositeData[] clocks = invoke(objectName, JMX_CONTENDED_LOCKS_KEY, new Long[]{minwaittime}, new String[]{"long"});
        List<Map<String, Object>> lockInfos = new ArrayList<>(clocks.length);
        for (CompositeData cs : clocks) {
            Map<String, Object> m = map(
                    JMX_ITEM_DESCRIPTION, cs.get(JMX_ITEM_DESCRIPTION).toString(),
                    JMX_ITEM_RESOURCE_ID, cs.get(JMX_ITEM_RESOURCE_ID).toString(),
                    JMX_ITEM_RESOURCE_TYPE, cs.get(JMX_ITEM_RESOURCE_TYPE).toString());
            lockInfos.add(m);
        }
        return lockInfos;
    }

    public static class LockInfoResult {

        public final long advertedDeadLocks;

        public final long lockCount;

        public final long contendedLockCount;

        public final long minimumWaitTimeMs;

        public final List<Map<String, Object>> contendedLocks;

        public final String info;

        public LockInfoResult(long minimumWaitTimeMs, long locksCount, long advertedDeadLocks,
                long contendedLockCount, List<Map<String, Object>> lockinfos) {
            this.advertedDeadLocks = advertedDeadLocks;
            this.lockCount = locksCount;
            this.contendedLockCount = contendedLockCount;
            this.contendedLocks = lockinfos;
            this.minimumWaitTimeMs = minimumWaitTimeMs;
            this.info = "Showing contended locks where threads have waited for at least " + minimumWaitTimeMs + " ms.";
        }
    }
}
