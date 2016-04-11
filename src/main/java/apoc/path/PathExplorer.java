package apoc.path;

import java.util.*;
import java.util.stream.Stream;

import apoc.result.PathResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import apoc.Description;


public class PathExplorer {
	private static final String VERSION = "0.5";
	public static final Uniqueness UNIQUENESS = Uniqueness.RELATIONSHIP_PATH;
	@Context
    public GraphDatabaseService db;

	@Context
    public Log log;
	
	
	public Stream<InfoContainer> info() {
		return getInfo().stream().map(InfoContainer::new);
	}

	@Procedure("apoc.path.expand")
	@Description("apoc.path.expand(startNode <id>|Node|list, 'TYPE|TYPE_OUT>|<TYPE_IN', '+YesLabel|-NoLabel', minLevel, maxLevel ) yield path expand from start node following the given relationships from min to max-level adhering to the label filters")
	public Stream<PathResult> explorePath(@Name("start") Object start
			                   , @Name("relationshipFilter") String pathFilter
			                   , @Name("labelFilter") String labelFilter
			                   , @Name("minLevel") long minLevel
			                   , @Name("maxLevel") long maxLevel ) throws Exception {
		List<Node> nodes = startToNodes(start);
		return explorePathPrivate(nodes, pathFilter, labelFilter, minLevel, maxLevel);
	}

	@SuppressWarnings("unchecked")
	private List<Node> startToNodes(Object start) throws Exception {
		if (start == null) return Collections.emptyList();
		if (start instanceof Node) {
			return Collections.singletonList((Node) start);
		}
		if (start instanceof Number) {
			return Collections.singletonList(db.getNodeById(((Number) start).longValue()));
		}
		if (start instanceof List) {
			List list = (List) start;
			if (list.isEmpty()) return Collections.emptyList();

			Object first = list.get(0);
			if (first instanceof Node) return (List<Node>)list;
			if (first instanceof Number) {
                List<Node> nodes = new ArrayList<>();
                for (Number n : ((List<Number>)list)) nodes.add(db.getNodeById(n.longValue()));
                return nodes;
            }
		}
		throw new Exception("Unsupported data type for start parameter a Node or an Identifier (long) of a Node must be given!");
	}

	private Direction directionFor(String type) {
		if (type.contains("<")) return Direction.INCOMING;
		if (type.contains(">")) return Direction.OUTGOING;
		return Direction.BOTH;
	}

	private Stream<PathResult> explorePathPrivate(Iterable<Node> startNodes
			                   , String pathFilter
			                   , String labelFilter
			                   , long minLevel
			                   , long maxLevel ) {
		// LabelFilter
		// -|Label|:Label|:Label excluded label list
		// +:Label or :Label include labels
		
		int from = new Long(minLevel).intValue();
		int to = new Long(maxLevel).intValue();
		TraversalDescription td = db.traversalDescription().breadthFirst();
		// based on the pathFilter definition now the possible relationships and directions must be shown
		if (pathFilter !=null ) {
			String[] defs = pathFilter.split("\\|");
			if (!defs[0].isEmpty()) {
				for (String def : defs) {
					Direction direction = directionFor(def);
					RelationshipType relType = new DynRelationshipType(def);
					if (relType.name().isEmpty()) {
						td = td.expand(PathExpanderBuilder.allTypes(direction).build());
					} else {
						td = td.relationships(relType, direction);
					}
				}
			} // else td = td.expand(StandardExpander.DEFAULT); }
		} // else { td = td.expand(StandardExpander.DEFAULT); }
		LabelEvaluator labelEvaluator = new LabelEvaluator(labelFilter);
		td = td.evaluator(Evaluators.fromDepth(from))
				.evaluator(Evaluators.toDepth(to))
				.evaluator(labelEvaluator);
		td = td.uniqueness(UNIQUENESS); // this is how Cypher works !!
		// uniqueness should be set as last on the TraversalDescription
		return td.traverse(startNodes).stream().map( PathResult::new );
	}
	
	public class DynRelationshipType implements RelationshipType {
		private String name;
		public DynRelationshipType(String sname) {
			if (sname.startsWith(":")) {
				sname = sname.substring(1);
			}
			if (sname.endsWith(">") || sname.endsWith("<")) {
				sname = sname.substring(0, sname.length() -1);
			}
			this.name = sname;
		}
		public String name() {
			return this.name;
		}
		
	}

	public static class InfoContainer
	{	
		public String info;
		public InfoContainer(String inf) {
			this.info = inf;
		}
	}
	private List<String> getInfo() {
		LinkedList<String> infolist = new LinkedList<String>();
		infolist.add("explorePath version " + VERSION);
		
		infolist.add("usage call explorePath(startNode <id>|Node, relationshipFilter, labelFilter, minLevel, maxLevel )");
		
		infolist.add("- startnode <id> (long, int) or Node");
		infolist.add("> > relationshipFilter RELATIONSHIP_TYPE1{<,>,}|RELATIONSHIP_TYPE2{<,>,}|... ");
		infolist.add("> > RELATIONSHIP_TYPE> = only direction Outgoing");
		infolist.add("> > RELATIONSHIP_TYPE< = only direction Incoming");
		infolist.add("> > RELATIONSHIP_TYPE = both directions");
		infolist.add("- labelFilter {+,-} LABEL1|LABEL2|...");
		infolist.add("> > '+' include label list (white list");
		infolist.add("> > '-' exclude label list (black list");
		infolist.add("- minLevel minimum path level");
		infolist.add("- maxLevel maximum path level");
		infolist.add(": RETURNS a variable with then name 'exploredPath' ");
		
		return infolist;
	}
	public static class LabelEvaluator implements Evaluator {
		private boolean included = true;
		private Set<String> labels = new HashSet<String>();
		public LabelEvaluator(String labelFilter) {
			// parse the filter
			if (labelFilter ==  null || labelFilter.equalsIgnoreCase("")) labelFilter = "-"; // exclude nothing
			included = labelFilter.startsWith("+");
			String work = labelFilter.substring(1); // remove the + or -
			// split on |
			String[] defs = work.split("\\|") ;
			for (String def : defs) {
				if (def.startsWith(":")) def = def.substring(1);
				labels.add(def);
			}
		}
		
		
		@Override
		public Evaluation evaluate(Path path) {
			Node check = path.endNode();
			if (included) {
				if (labelExists(check)) {
					return Evaluation.INCLUDE_AND_CONTINUE;
				} else {
					return Evaluation.EXCLUDE_AND_PRUNE;
				}
			} else {
				if (labelExists(check)) {
					return Evaluation.EXCLUDE_AND_PRUNE;
				} else {
					return Evaluation.INCLUDE_AND_CONTINUE;
				}
			}
		}
		private boolean labelExists(Node node) {
			for ( Label lab : node.getLabels() ) {
				if (labels.contains(lab.name())) {
					return true;
				}
			}
			return false;
		}
	}

}
