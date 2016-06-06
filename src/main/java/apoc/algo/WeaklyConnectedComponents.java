package apoc.algo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
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

	@Context
	public GraphDatabaseAPI dbAPI;

	@Context
	public Log log;

	@Procedure("apoc.algo.wcc")
	@Description("CALL apoc.algo.wcc() YIELD number of weakly connected components")
	public Stream<LongResult> wcc() {
		long componentID = 0;
		ResourceIterator<Node> nodes = dbAPI.getAllNodes().iterator();
		PrimitiveLongSet allNodes = Primitive.longSet(0);
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
	}

	private PrimitiveLongSet go(Node node, Direction direction) {

		PrimitiveLongSet visitedIDs = Primitive.longSet(0);
		Stack<Node> frontierList = new Stack<Node>();

		frontierList.push(node);
		visitedIDs.add(node.getId());

		while (!frontierList.isEmpty()) {
			node = frontierList.pop();
			Iterator<Relationship> itR = node.getRelationships(direction).iterator();
			while (itR.hasNext()) {
				Node child = itR.next().getOtherNode(node);
				if (visitedIDs.contains(child.getId())) {
					continue;
				}
				visitedIDs.add(child.getId());
				frontierList.push(child);
			}
		}
		return visitedIDs;
	}
}
