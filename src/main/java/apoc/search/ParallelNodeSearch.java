package apoc.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import apoc.Description;
import apoc.result.NodeResult;

public class ParallelNodeSearch {
	
	private static final String SEARCH_TYPE_EXACT = "EXACT";
	private static final String SEARCH_TYPE_STARTS_WITH = "STARTS WITH";
	private static final String SEARCH_TYPE_ENDS_WITH = "ENDS WITH";
	private static final String SEARCH_TYPE_CONTAINS = "CONTAINS";
	
    @Context 
    public GraphDatabaseAPI api;
    
    @Context
    public Log log;
    
    
    @Procedure("apoc.search.nodeAllReduced")
    @Description("Do a parallel search over multiple indexes returning a reduced representation of the nodes found: node id, label and the searched property. apoc.search.nodeShortAll( map of label and properties which will be searched upon, searchType: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ). All 'hits' are returned.")
	public Stream<NodeReducedResult> multiSearchAll( @Name("LabelPropertyMap") final String labelProperties, @Name("searchType") final String searchType, @Name("searchvalue") final String search) throws Exception {

		List<NodeReducedResult> res = new ArrayList<NodeReducedResult>(); 

		List<QueryWorker> defList = validateInput(labelProperties, searchType, search);
		defList.parallelStream().forEach(new Consumer<QueryWorker>() {
			@Override
			public void accept(QueryWorker t) {
				res.addAll(t.doQuery());
			}
		} );
		return res.stream();
	}

    
	@Procedure("apoc.search.nodeReduced")
    @Description("Do a parallel search over multiple indexes returning a reduced representation of the nodes found: node id, labels and the searched properties. apoc.search.nodeShort( map of label and properties which will be searched upon, searchType: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ). Multiple search results for the same node are merged into one record.")
	public Stream<NodeReducedResult> multiSearch( @Name("LabelPropertyMap") final String labelProperties, @Name("searchType") final String searchType, @Name("searchvalue") final String search) throws Exception {
		Map<Long,NodeReducedResult> mres = new HashMap<Long,NodeReducedResult>(); 
		try {
			List<QueryWorker> defList = validateInput(labelProperties, searchType, search);
			
			List<NodeReducedResult> res = new ArrayList<NodeReducedResult>(); 
			defList.parallelStream().forEach(new Consumer<QueryWorker>() {
				@Override
				public void accept(QueryWorker t) {
					res.addAll(t.doQuery());
				}
			} );
			// merge duplicate nodes
			for (NodeReducedResult msr : res) {
				NodeReducedResult mn = mres.get(msr.id);
				if (mn == null) {
					mres.put(msr.id,msr);
				} else {
					// check label
					for (String sl : msr.label) {
						if (!mn.label.contains(sl)) {
							mn.label.add(sl);
						}
					}
					// check properties
					for (String key : msr.values.keySet()) {
						if (!mn.values.containsKey(key)) {
							mn.values.put(key, msr.values.get(key));
						}
					}
					
				}
			}
		} catch (Exception ee) {
			ee.printStackTrace();
			throw(ee);
		}
		return mres.values().stream();
	}

	@Procedure("apoc.search.nodeAll")
    @Description("Do a parallel search over multiple indexes returning nodes. usage apoc.search.nodeAll( map of label and properties which will be searched upon, searchType: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ) returns all the Nodes found in the different searches.")
	public Stream<NodeResult> multiSearchNodeAll( @Name("LabelPropertyMap") final String labelProperties, @Name("searchType") final String searchType, @Name("searchvalue") final String search) throws Exception {

		List<NodeResult> res = new ArrayList<NodeResult>(); 

		List<QueryWorker> defList = validateInput(labelProperties, searchType, search);
		defList.parallelStream().forEach(new Consumer<QueryWorker>() {
			@Override
			public void accept(QueryWorker t) {
				res.addAll(t.doQueryNode());
				
			}
		} );
		return res.stream();
	}

	
	@Procedure("apoc.search.node")
    @Description("Do a parallel search over multiple indexes returning nodes. usage apoc.search.node( map of label and properties which will be searched upon, searchType: EXACT | CONTAINS | STARTS WITH | ENDS WITH, searchValue ) returns all the DISTINCT Nodes found in the different searches.")
	public Stream<NodeResult> multiSearchNode( @Name("LabelPropertyMap") final String labelProperties, @Name("searchType") final String searchType, @Name("searchvalue") final String search) throws Exception {
		List<Long> mids = new ArrayList<Long>();
		List<NodeResult> res = new ArrayList<NodeResult>();
		try {
			List<QueryWorker> defList = validateInput(labelProperties, searchType, search);
			
			defList.parallelStream().forEach(new Consumer<QueryWorker>() {
				@Override
				public void accept(QueryWorker t) {
					res.addAll(t.doQueryNode().stream().filter(p -> { 
						if (!mids.contains(p.node.getId())) {
							mids.add(p.node.getId());
							return true;
						} else {
							return false;
						}
						
					}).collect(Collectors.toList()));
				}
			} );
		} catch (Exception ee) {
			ee.printStackTrace();
			throw(ee);
		}
		return res.stream();
	}

	
	private List<QueryWorker> validateInput(final String labelProperties, final String searchType, final String search) throws Exception {
		// validate input
		if (search == null || search.trim().isEmpty()) {
			throw new Exception("searchValue is mandatory and cannot be empty");
		}
		if (searchType == null) {
			throw new Exception("searchType is mandatory and must have one of the following values :" + SEARCH_TYPE_EXACT + ", " + SEARCH_TYPE_CONTAINS + ", " + SEARCH_TYPE_STARTS_WITH + ", " + SEARCH_TYPE_ENDS_WITH + ".");
		}
		if (!searchType.equals(SEARCH_TYPE_EXACT) 
			&& !searchType.equals(SEARCH_TYPE_CONTAINS)
			&& !searchType.equals(SEARCH_TYPE_STARTS_WITH)
			&& !searchType.equals(SEARCH_TYPE_ENDS_WITH)) {
			throw new Exception("searchType invalid, it must have one of the following values :" + SEARCH_TYPE_EXACT + ", " + SEARCH_TYPE_CONTAINS + ", " + SEARCH_TYPE_STARTS_WITH + ", " + SEARCH_TYPE_ENDS_WITH + ".");
		}
		if (labelProperties == null || labelProperties.trim().isEmpty()) {
			throw new Exception("LabelProperties cannot be empty. example { Person: [\"fullName\",\"lastName\"],Company:\"name\", Event : \"Description\"}");
		}
		//
		// parse the labelProperties
		//
		ObjectMapper om = new ObjectMapper();
		om.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		HashMap<String,Object> result = om.readValue(labelProperties, HashMap.class);
		List<QueryWorker> list = new ArrayList<QueryWorker>();
		for (String lbl : result.keySet()) {
			Object b = result.get(lbl);
			if (b instanceof String) {
				list.add( new QueryWorker(api, lbl, (String) b , searchType, search,log));
			} else if (b instanceof List<?>) {
				List<String> bl = (List<String>) b;
				for (String prop: bl) {
					list.add( new QueryWorker(api, lbl, prop ,searchType,search,log));
				}
			}
		}
		return list;
	}
	public class QueryWorker  {
		private GraphDatabaseAPI api;
		private String label,prop,search,searchType;
		private Log nlog;
		public QueryWorker(GraphDatabaseAPI dbapi, String label , String prop, String searchType, String search, Log lg) {
			this.api = dbapi;
			this.label = label;
			this.prop = prop;
			this.search = search;
			this.searchType = searchType;
			this.nlog = lg;
		}
	    public List<NodeReducedResult> doQuery()  	{
			long tstart = System.currentTimeMillis();
			
	    	List<NodeReducedResult> m = new ArrayList<NodeReducedResult>();
	    	try (Transaction tx = api.beginTx()) {
				ResourceIterator<Node>  niter = null;
				if (searchType.equals(SEARCH_TYPE_EXACT)) {
					niter = api.findNodes(new DynamicLabel(label), prop, search);
				} else if (searchType.equals(SEARCH_TYPE_CONTAINS)) {
					niter = api.execute("match (n:" + label +") where n." + prop + " contains \"" + search + "\" return n").columnAs("n");
				} else if (searchType.equals(SEARCH_TYPE_STARTS_WITH)) {
					niter = api.execute("match (n:" + label +") where n." + prop + " starts with \"" + search + "\" return n").columnAs("n");
				} else if (searchType.equals(SEARCH_TYPE_ENDS_WITH)) {
					niter = api.execute("match (n:" + label +") where n." + prop + " ends with \"" + search + "\" return n").columnAs("n");
				}
				while (niter.hasNext()) {
					Node n = niter.next();
					Map<String,Object> props = new HashMap<String,Object>();
					List<String> labels = new ArrayList<String>();
					labels.add(label);
					props.put(prop, n.getProperty(prop));
					NodeReducedResult msr = new NodeReducedResult(labels
								, n.getId()
								, props);
					m.add(msr);
				}
				tx.success();
	    	}
	    	nlog.debug("(" + Thread.currentThread().getId() + ") search on label:" + label + " and prop:" + prop + " takes " + (System.currentTimeMillis() - tstart));
			return m;
	    }
	    public List<NodeResult> doQueryNode()  	{
			// log.info("apoc.search.multi QueryWorker.doQuery: Thread id " + Thread.currentThread().getId() + " label:" + label + " prop:" + prop);
			long tstart = System.currentTimeMillis();
			ResourceIterator<Node>  niter = null;
	    	List<NodeResult> m = new ArrayList<NodeResult>();
	    	try (Transaction tx = api.beginTx()) {
				
				if (searchType.equals(SEARCH_TYPE_EXACT)) {
					niter = api.findNodes(new DynamicLabel(label), prop, search);
				} else if (searchType.equals(SEARCH_TYPE_CONTAINS)) {
					niter = api.execute("match (n:" + label +") where n." + prop + " contains \"" + search + "\" return n").columnAs("n");
				} else if (searchType.equals(SEARCH_TYPE_STARTS_WITH)) {
					niter = api.execute("match (n:" + label +") where n." + prop + " starts with \"" + search + "\" return n").columnAs("n");
				} else if (searchType.equals(SEARCH_TYPE_ENDS_WITH)) {
					niter = api.execute("match (n:" + label +") where n." + prop + " ends with \"" + search + "\" return n").columnAs("n");
				}
				m = niter.stream().map(n -> { return new NodeResult(n); }).collect(Collectors.toList());
				tx.success();
	    	}
	    	nlog.debug("(" + Thread.currentThread().getId() + ") search on label:" + label + " and prop:" + prop + " takes " + (System.currentTimeMillis() - tstart));
			return m;
	    }

	}
	
	private class DynamicLabel implements Label {
		private String name;
		public DynamicLabel(String sname) {
			this.name = sname;
		}
		public String name() {
			return this.name;
		}
		
	}
	public class NodeReducedResult{
		public List<String> label;
		public Map<String,Object> values;
		public long   id;
	
		public NodeReducedResult( List<String> labels,
				long id,
				Map<String,Object> val
				) {
			this.label = labels;
			this.id = id;
			this.values = val;
		}
	
	}
}

