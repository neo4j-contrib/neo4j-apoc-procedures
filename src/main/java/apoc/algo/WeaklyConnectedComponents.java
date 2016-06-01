package apoc.algo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;

import apoc.Description;
import apoc.result.LongResult;

public class WeaklyConnectedComponents {


	
	@Context
    public GraphDatabaseAPI dbAPI;

	@Context
	public Log log;

	@Procedure("apoc.algo.wcc")
	@Description("CALL apoc.algo.wcc() YIELD number of weakly connected components")
	public Stream<LongResult> wcc() {
		try {
			long componentID = 0;
			ResourceIterator<Node> nodes = dbAPI.getAllNodes().iterator();
			LongHashSet allNodes = new LongHashSet();
			while (nodes.hasNext()) {
				Node node = nodes.next();
				if (node.getDegree() == 0) {
					componentID++;
				} else {
					allNodes.add(node.getId());
				}
			}
			nodes.close();

			Iterator<LongCursor> it = allNodes.iterator();
			while (it.hasNext()) {
				// Every node has to be marked as (part of) a component
				try {
					Long n = it.next().value;
					LongHashSet reachableIDs = go(dbAPI.getNodeById(n), Direction.BOTH);
					allNodes.removeAll(reachableIDs);
					componentID++;

				} catch (NoSuchElementException e) {
					break;
				}
				it = allNodes.iterator();
			}
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
		Iterator<Relationship> itR = node.getRelationships(dir).iterator();
		while (itR.hasNext()) {
			it.add(itR.next().getOtherNode(node));
		}
		return it;
	}

}
