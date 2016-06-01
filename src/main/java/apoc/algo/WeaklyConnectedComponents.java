package apoc.algo;

import static org.neo4j.collection.primitive.Primitive.VALUE_MARKER;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.base.Empty.EmptyPrimitiveLongSet;
import org.neo4j.collection.primitive.hopscotch.LongKeyTable;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongHashSet;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import apoc.Description;
import apoc.result.LongResult;

public class WeaklyConnectedComponents {
	public static final Monitor NO_MONITOR = new Monitor.Adapter() { /*No additional logic*/ };
	
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
			PrimitiveLongSet allNodes = new PrimitiveLongHashSet(new LongKeyTable<>( 12, VALUE_MARKER ), VALUE_MARKER,NO_MONITOR);
			while (nodes.hasNext()) {
				Node node = nodes.next();
				if (node.getDegree() == 0) {
					componentID++;
				} else {
					allNodes.add(node.getId());
				}
			}
			nodes.close();

			PrimitiveLongIterator it = allNodes.iterator();
			while (it.hasNext()) {
				// Every node has to be marked as (part of) a component
				try {
					long n = it.next();
					PrimitiveLongIterator reachableIDs = go(dbAPI.getNodeById(n), Direction.BOTH).iterator();
					while (reachableIDs.hasNext()) {
						long id = (long) reachableIDs.next();
						allNodes.remove(id);
					}
					componentID++;

				} catch (NoSuchElementException e) {
					break;
				}
				it = allNodes.iterator();
			}
			allNodes.close();
			return Stream.of(new LongResult(componentID));
		} catch (Exception e) {
			String errMsg = "Error encountered while calculating weakly connected components";
			log.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	private PrimitiveLongSet go(Node node, Direction direction) {

		PrimitiveLongSet visitedIDs = new PrimitiveLongHashSet(new LongKeyTable<>( 12, VALUE_MARKER ), VALUE_MARKER,NO_MONITOR);
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
