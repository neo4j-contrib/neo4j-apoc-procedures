package apoc.export.cypher.formatter;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class UpdateAllCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {

	@Override
	public String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
	    return super.mergeStatementForNode(CypherFormat.UPDATE_ALL, node, uniqueConstraints, indexedProperties, indexNames);
	}

	@Override
	public String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties) {
        return super.mergeStatementForRelationship(CypherFormat.UPDATE_ALL, relationship, uniqueConstraints, indexedProperties);
	}

	@Override
	public Map<String,Object> statementForSameNodes(Iterable<Node> node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
		return null;
	}

	@Override
	public Map<String,Object> statementForSameRelationship(Iterable<Relationship> relationship, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
		return null;
	}
}
