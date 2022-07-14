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
public class AddStructureCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {

	@Override
	public String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
		return super.mergeStatementForNode(CypherFormat.ADD_STRUCTURE, node, uniqueConstraints, indexedProperties, indexNames);
	}

	@Override
	public String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, ExportConfig exportConfig) {
		return new CreateCypherFormatter().statementForRelationship(relationship, uniqueConstraints, indexedProperties, exportConfig);
	}

	@Override
	public String statementForCleanUp(int batchSize) {
		return "";
	}

	@Override
	public String statementForNodeIndex(String indexType, String label, Iterable<String> key, boolean ifNotExist, String idxName) {
		return "";
	}
	
	@Override
	public String statementForIndexRelationship(String indexType, String type, Iterable<String> key, boolean ifNotExists, String idxName) {
		return "";
	}

	@Override
	public String statementForCreateConstraint(String name, String label, Iterable<String> keys, boolean ifNotExists) {
		return "";
	}

	@Override
	public String statementForDropConstraint(String name) {
		return "";
	}

	@Override
	public void statementForNodes(Iterable<Node> node, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db) {
		buildStatementForNodes("MERGE ", "ON CREATE SET ", node, uniqueConstraints, exportConfig, out, reporter, db);
	}

	@Override
	public void statementForRelationships(Iterable<Relationship> relationship, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db) {
		buildStatementForRelationships("CREATE ", " SET ", relationship, uniqueConstraints, exportConfig, out, reporter, db);
	}
}
