package apoc.path;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

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
	@Context
    public GraphDatabaseService db;

	@Context
    public Log log;
	
	
	public Stream<InfoContainer> info() {
		return getInfo().stream().map(InfoContainer::new);
	}
	
	
	
	@Procedure("apoc.path.expand")
	@Description("usage - CALL apoc.path.expand(startNode <id>|Node, relationshipFilter, labelFilter, minLevel, maxLevel ) yield expandedPath as <identifier>")
	public Stream<PathContainer> explorePath(@Name("start") Object start
			                   ,@Name("relationshipFilter") String pathFilter
			                   ,@Name("labelFilter") String labelFilter
			                   ,@Name("minLevel") long minLevel
			                   ,@Name("maxLevel") long maxLevel ) throws Exception {
		List<Node> nodes = new LinkedList<Node>();
		if (start instanceof Node) {
			nodes.add((Node) start);
		} else if (start instanceof Long) {
			nodes.add(db.getNodeById(((Long) start).longValue())) ;
		} else if (start instanceof Integer) {
			nodes.add(db.getNodeById(((Integer) start).longValue())) ;
		} else {
			throw new Exception("Unsupported data type for start parameter a Node or an Identifier (long) of a Node must be given!");
		}
		return explorePathPrivate(nodes, pathFilter, labelFilter, minLevel, maxLevel);
	}

	private Stream<PathContainer> explorePathPrivate(Iterable<Node> startNodes
			                   , String pathFilter
			                   ,String labelFilter
			                   ,long minLevel
			                   ,long maxLevel ) {
		// LabelFilter
		// -|Label|:Label|:Label excluded label list
		// +:Label or :Label include labels
		
		int from = new Long(minLevel).intValue();
		int to = new Long(maxLevel).intValue();
		TraversalDescription td = db.traversalDescription().breadthFirst();
		// based on the pathFilter definition now the possible relationships and directions must be shown
		String[] defs = pathFilter.split("\\|");
		if (!defs[0].isEmpty()) {
			for (String def : defs) {
				RelationshipType relType = new DynRelationshipType(def);
				if (relType.name().length() > 0) {
					if (def.indexOf("<") > -1) {
						td = td.relationships(relType, Direction.INCOMING);
					} else if (def.indexOf(">") > -1) {
						td = td.relationships(relType, Direction.OUTGOING);
					} else {
						td = td.relationships(relType, Direction.BOTH);
					}
				} else {
					//
					// if no relation is given then all relationships apply
					// 
					PathExpanderBuilder peb = null;
					if (def.indexOf("<") > -1) {
						peb = PathExpanderBuilder.allTypes(Direction.INCOMING);
					} else if (def.indexOf(">") > -1) {
						peb = PathExpanderBuilder.allTypes(Direction.OUTGOING);
					} else {
						peb = PathExpanderBuilder.allTypesAndDirections();
					}
					td = td.expand(peb.build());
				}
			}
		}
		LabelEvaluator labeval = new LabelEvaluator(labelFilter);
		td = td.evaluator(Evaluators.fromDepth(from))
		     .evaluator(Evaluators.toDepth(to)).evaluator(labeval);
		td = td.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL); // this is how Cypher works !!
		// uniqueness should be set as last on the TraversalDescription
		return td.traverse(startNodes).stream().map( PathContainer::new );
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
	public static class PathContainer 
	{	
		public Path expandedPath;

		public PathContainer(Path p) {
			this.expandedPath = p;
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
		infolist.add("- labelFilter {+.-} LABEL1|LABEL2|...");
		infolist.add("> > '+' include label list (white list");
		infolist.add("> > '-' exclude label list (black list");
		infolist.add("- minLevel minimum path level");
		infolist.add("- maxLevel maximum path level");
		infolist.add(": RETURNS a variable with then name 'exploredPath' ");
		
		return infolist;
	}
	public static class LabelEvaluator implements Evaluator {
		private boolean included = true;
		private List<String> labels = new ArrayList<String>();
		public LabelEvaluator(String labelFilter) {
			// parse the filter
			if (labelFilter.equalsIgnoreCase("")) labelFilter = "-"; // exclude nothing
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
			boolean ex = false;
			for ( Label lab : node.getLabels() ) {
				if (labels.contains(lab.name())) {
					ex = true;
					break;
				}
			}
			
			return ex;
		}
	}

}
