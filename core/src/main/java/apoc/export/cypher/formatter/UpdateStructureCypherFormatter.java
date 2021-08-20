package apoc.export.cypher.formatter;

import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import org.neo4j.graphdb.GraphDatabaseService;
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
	public String statementForNodeIndex(String label, Iterable<String> key) {
		return "";
	}

	@Override
	public String statementForIndexRelationship(String type, Iterable<String> key) {
		return "";
	}

	@Override
	public String statementForConstraint(String label, Iterable<String> key) {
		return "";
	}

	@Override
	public void statementForNodes(Iterable<Node> node, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db) {
	}

	@Override
	public void statementForRelationships(Iterable<Relationship> relationship, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db) {
		buildStatementForRelationships("MERGE ", "SET ", relationship, uniqueConstraints, exportConfig, out, reporter, db);
	}
}
