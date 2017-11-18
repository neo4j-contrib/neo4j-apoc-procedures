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
public class AddStructureCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {

	@Override
	public String statementForNode(Node node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
		return super.mergeStatementForNode(CypherFormat.ADD_STRUCTURE, node, uniqueConstraints, indexedProperties, indexNames);
	}

	@Override
	public String statementForRelationship(Relationship relationship,  Map<String, String> uniqueConstraints, Set<String> indexedProperties) {
		return new CreateCypherFormatter().statementForRelationship(relationship, uniqueConstraints, indexedProperties);
	}

	@Override
	public String statementForCleanUp(int batchSize) {
		return "";
	}

	@Override
	public String statementForIndex(String label, Iterable<String> key) {
		return "";
	}

	@Override
	public String statementForConstraint(String label, String key) {
		return "";
	}
}
