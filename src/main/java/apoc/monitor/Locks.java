package apoc.monitor;

import static org.neo4j.jmx.JmxUtils.getAttribute;
import static org.neo4j.jmx.JmxUtils.getObjectName;
import static org.neo4j.jmx.JmxUtils.invoke;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import apoc.Description;
import apoc.result.LockInfoResult;

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
    
    
    @Procedure("apoc.monitor.lock")
    @Description("apoc.monitor.lock(minWaitTime) returns advertedDeadLocks, lockCount, info, contendedLockCount, contendedLocks")
    public Stream<LockInfoResult> lockInfo(@Name("minWaitTime") Long minwaittime) {
    	
    	if (minwaittime == null || minwaittime < 0) minwaittime = 0l;
        ObjectName objectName = getObjectName(database, JMX_OBJECT_NAME);
        CompositeData[] locks = getAttribute(objectName, JMX_LOCKS_KEY);
        CompositeData[] clocks = invoke(objectName,JMX_CONTENDED_LOCKS_KEY,new Long[] {minwaittime},new String[] {"long"});
        List<Map<String,Object>> linfos = new ArrayList<Map<String,Object>>();
        for (CompositeData cs : clocks) {
        	Map<String,Object> m = new HashMap<String,Object>();
        	m.put(JMX_ITEM_DESCRIPTION, cs.get(JMX_ITEM_DESCRIPTION).toString());
        	m.put(JMX_ITEM_RESOURCE_ID, cs.get(JMX_ITEM_RESOURCE_ID).toString());
        	m.put(JMX_ITEM_RESOURCE_TYPE, cs.get(JMX_ITEM_RESOURCE_TYPE).toString());
        	linfos.add(m);
        }
        
        LockInfoResult info = new LockInfoResult(minwaittime
        		                                ,locks.length
        		                                ,getAttribute(objectName, JMX_NR_ADVERTED_DEADLOCKS_KEY)
        										,clocks.length
        		                                , linfos );
        return Stream.of(info);
    }
}
