package apoc.algo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.stream.Collectors;
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

import org.neo4j.procedure.Description;
import apoc.algo.wcc.CCVar;
import apoc.result.CCResult;

public class WeaklyConnectedComponents {

	@Context
	public GraphDatabaseAPI dbAPI;

	@Context
	public Log log;

	@Procedure("apoc.algo.wcc")
	@Description("CALL apoc.algo.wcc() YIELD number of weakly connected components")
	public Stream<CCResult> wcc() {
		List<List<CCVar>> results = new LinkedList<List<CCVar>>();
		ResourceIterator<Node> nodes = dbAPI.getAllNodes().iterator();
		PrimitiveLongSet allNodes = Primitive.longSet(0);
		while (nodes.hasNext()) {
			Node node = nodes.next();
			if (node.getDegree() == 0) {
				List<CCVar> result = new LinkedList<CCVar>();
                result.add(new CCVar(node.getId()+"",node.getLabels().iterator().next().name()));
                results.add(result);
			} else {
				allNodes.add(node.getId());
			}
		}
		nodes.close();

		PrimitiveLongIterator it = allNodes.iterator();
		while (it.hasNext()) {
			try {
				long n = it.next();
				List<CCVar> result = new LinkedList<CCVar>();
				PrimitiveLongIterator reachableIDs = go(dbAPI.getNodeById(n), Direction.BOTH,result).iterator();
				while (reachableIDs.hasNext()) {
					long id = (long) reachableIDs.next();
					allNodes.remove(id);
				}
				results.add(result);

			} catch (NoSuchElementException e) {
				break;
			}
			it = allNodes.iterator();
		}
		allNodes.close();
		return results.stream().map((x) ->new CCResult( x.stream().map((z) -> new Long(z.getId())).collect(Collectors.toList()), x.stream().collect(Collectors.groupingBy(CCVar::getType)).entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().size()))
            ));
	}

	private PrimitiveLongSet go(Node node, Direction direction, List<CCVar> result) {

		PrimitiveLongSet visitedIDs = Primitive.longSet(0);
		Stack<Node> frontierList = new Stack<Node>();

		frontierList.push(node);
		visitedIDs.add(node.getId());
		result.add(new CCVar(node.getId()+"",node.getLabels().iterator().next().name()));


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
				result.add(new CCVar(child.getId()+"",child.getLabels().iterator().next().name()));
			}
		}
		return visitedIDs;
	}
}
