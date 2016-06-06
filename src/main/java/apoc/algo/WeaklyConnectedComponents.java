package apoc.algo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
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
import apoc.result.CCResult;
import apoc.util.PerformanceLoggerSingleton;

public class WeaklyConnectedComponents {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure("apoc.algo.wcc")
    @Description("CALL apoc.algo.wcc() YIELD number of weakly connected components")
    public Stream<CCResult> wcc() {
        try {
        	PerformanceLoggerSingleton metrics=PerformanceLoggerSingleton.getInstance("/Users/tommichiels/Desktop/");
        	List<List<Vertex>> results = new LinkedList<List<Vertex>>();
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
                    List<Vertex> result = new LinkedList<Vertex>();
                    result.add(new Vertex(node.getId()+"",node.getLabels().iterator().next().name()));
                    results.add(result);
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
					List<Vertex> result = new LinkedList<Vertex>();
					LongHashSet reachableIDs = go(db.getNodeById(n), Direction.BOTH,result);
			        context.stop();
			        Timer remove=metrics.getTimer("remove");
					Timer.Context rmcontext = remove.time();
			        allNodes.removeAll(reachableIDs);
			        rmcontext.stop();
					results.add(result);
			        componentID++;

				} catch (NoSuchElementException e) {
					break;
				}
				it = allNodes.iterator();
			}
			log.info("calculation done");
            //it.close();
        	return results.stream().map((x) ->new CCResult( x.stream().map((z) -> new Long(z.getId())).collect(Collectors.toList()), x.stream().collect(Collectors.groupingBy(Vertex::getType)).entrySet().stream().collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> e.getValue().size()))
                ));
        } catch (Exception e) {
            String errMsg = "Error encountered while calculating weakly connected components";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }
    
    
	private LongHashSet go(Node node, Direction direction, List<Vertex> result) {

		LongHashSet visitedIDs = new LongHashSet();
		List<Node> frontierList = new LinkedList<>();

		frontierList.add(node);
		visitedIDs.add(node.getId());
        result.add(new Vertex(node.getId()+"",node.getLabels().iterator().next().name()));
		
		while (!frontierList.isEmpty()) {
			node = frontierList.remove(0);
			Iterator<Node> it = getConnectedNodeIDs(node, direction).iterator();
			while (it.hasNext()) {
				Node child = it.next();
				if (visitedIDs.contains(child.getId())) {
					continue;
				}
				visitedIDs.add(child.getId());
				if (child.hasLabel(Label.label("Record"))){
					frontierList.add(child);
				}
				result.add(new Vertex(child.getId()+"",child.getLabels().iterator().next().name()));
				
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
		RelationshipType typesLives=RelationshipType.withName("Lives");
		Iterator<Relationship> itLives = node.getRelationships(typesLives,dir).iterator();
		while (itLives.hasNext()) {
			it.add(itLives.next().getOtherNode(node));
		}
		RelationshipType typesUses=RelationshipType.withName("Uses");
		Iterator<Relationship> itUses = node.getRelationships(typesUses,dir).iterator();
		while (itUses.hasNext()) {
			it.add(itUses.next().getOtherNode(node));
		}
		return it;
	}

}
