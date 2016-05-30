package apoc.algo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.codahale.metrics.Timer;

import apoc.Description;
import apoc.result.LongResult;
import apoc.util.PerformanceLoggerSingleton;

public class WeaklyConnectedComponents {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure("apoc.algo.wcc")
    @Description("CALL apoc.algo.wcc() YIELD number of weakly connected components")
    public Stream<LongResult> wcc() {
        try {
        	PerformanceLoggerSingleton metrics=PerformanceLoggerSingleton.getInstance("/Users/tommichiels/Desktop/");
        	log.info("find all nodes iterator");
        	long componentID= 0;
        	ResourceIterator<Node> nodes = db.findNodes(Label.label("Record"));
        	LongHashSet allNodes = new LongHashSet();
            //ResourceIterator<Node> it = db.getIteratorForAllNodes();
            log.info("setup the visited nodes collection");
        	//init visited list
        	while(nodes.hasNext()){
                //Node n = it.next();
                Node node = nodes.next();
                RelationshipType types=RelationshipType.withName("LINK");
                if(node.getDegree(types)==0){
                    metrics.mark("degree 0");
                	componentID++;
                } else{
                    allNodes.add(node.getId());
                }
        	}
            nodes.close();
            log.info("setup the visited nodes collection done");
			// start calculation
            log.info("start calculation");
            Iterator<LongCursor> it = allNodes.iterator();
			while (it.hasNext()) {
				// Every node has to be marked as (part of) a component

				try {
					Timer bfs=metrics.getTimer("BFS");
					Timer.Context context = bfs.time();			
					Long n = it.next().value;
			        LongHashSet reachableIDs = go(db.getNodeById(n), Direction.BOTH);
			        context.stop();
			        Timer remove=metrics.getTimer("remove");
					Timer.Context rmcontext = remove.time();
			        allNodes.removeAll(reachableIDs);
			        rmcontext.stop();
					componentID++;

				} catch (NoSuchElementException e) {
					break;
				}
				it = allNodes.iterator();
			}
			log.info("calculation done");
            //it.close();
        	return Stream.of(new LongResult(componentID));
        } catch (Exception e) {
            String errMsg = "Error encountered while calculating weakly connected components";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }
    
    
	private LongHashSet go(Node node, Direction direction) {

		LongHashSet visitedIDs = new LongHashSet();
		List<Node> frontierList = new LinkedList<>();

		frontierList.add(node);
		visitedIDs.add(node.getId());

		while (!frontierList.isEmpty()) {
			node = frontierList.remove(0);
			Iterator<Node> it = getConnectedNodeIDs(node, direction).iterator();
			while (it.hasNext()) {
				Node child = it.next();
				if (visitedIDs.contains(child.getId())) {
					continue;
				}
				visitedIDs.add(child.getId());
				frontierList.add(child);
			}
		}
		return visitedIDs;
	}
    
	private List<Node> getConnectedNodeIDs(Node node, Direction dir) {
		List<Node> it = new LinkedList<Node>();
		RelationshipType types=RelationshipType.withName("LINK");
		Iterator<Relationship> itR = node.getRelationships(types,dir).iterator();
		while (itR.hasNext()) {
			it.add(itR.next().getOtherNode(node));
		}
		return it;
	}

}
