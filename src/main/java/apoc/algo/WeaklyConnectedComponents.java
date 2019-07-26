package apoc.algo;

import apoc.algo.wcc.CCVar;
import apoc.result.CCResult;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.SynchronizedLongSet;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WeaklyConnectedComponents {

	@Context
	public GraphDatabaseService db;

	@Context
	public Log log;

	@Deprecated
	@Procedure("apoc.algo.wcc")
	@Description("CALL apoc.algo.wcc() YIELD number of weakly connected components")
	public Stream<CCResult> wcc() {
		List<List<CCVar>> results = new LinkedList<List<CCVar>>();
		ResourceIterator<Node> nodes = db.getAllNodes().iterator();
		MutableLongSet allNodes = new SynchronizedLongSet(new LongHashSet()); // TODO: initialze with total number of nodes
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


		allNodes.forEach(id -> {
			try {
				List<CCVar> result = new LinkedList<CCVar>();
				LongSet reachableIDs = go(db.getNodeById(id), Direction.BOTH,result);
				reachableIDs.forEach(localId -> {
					allNodes.remove(localId);
				});
				results.add(result);

			} catch (NoSuchElementException e) {
				// pass
			}

		});

		return results.stream().map((x) ->new CCResult( x.stream().map((z) -> new Long(z.getId())).collect(Collectors.toList()), x.stream().collect(Collectors.groupingBy(CCVar::getType)).entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().size()))
            ));
	}

	private LongSet go(Node node, Direction direction, List<CCVar> result) {

		MutableLongSet visitedIDs = new LongHashSet();
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
