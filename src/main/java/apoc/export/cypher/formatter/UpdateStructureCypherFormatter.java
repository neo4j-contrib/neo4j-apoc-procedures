package apoc.export.cypher.formatter;

import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class UpdateStructureCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {

	@Override
	public String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
		return "";
	}

	@Override
	public String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties) {
        return super.mergeStatementForRelationship(CypherFormat.UPDATE_STRUCTURE, relationship, uniqueConstraints, indexedProperties);
	}

	@Override
	public String statementForCleanUp(int batchSize) {
		return "";
	}

	@Override
	public void statementForSameNodes(Iterable<Node> node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter) {
	}

	@Override
	public void statementForSameRelationship(Iterable<Relationship> relationship, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter) {
	}

	@Override
	public String statementForIndex(String label, Iterable<String> key) {
		return "";
	}

	@Override
	public String statementForConstraint(String label, Iterable<String> key) {
		return "";
	}
}
