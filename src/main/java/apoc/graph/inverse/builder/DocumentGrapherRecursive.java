package apoc.graph.inverse.builder;

import apoc.result.VirtualNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.util.*;

public class DocumentGrapherRecursive {

    private GraphDatabaseService db;
    private IdBuilder documentIdBuilder;
    private RelationshipBuilder documentRelationBuilder;
    private LabelBuilder documentLabelBuilder;
    private boolean writeToGraph;
    private List<Node> nodeList = new ArrayList<>();

    public DocumentGrapherRecursive(GraphDatabaseService db, boolean writeToGraph) {
        this.db = db;
        this.documentIdBuilder = new IdBuilder();
        this.documentRelationBuilder = new RelationshipBuilder();
        this.documentLabelBuilder = new LabelBuilder();
        this.writeToGraph = writeToGraph;
    }

    class RecursiveReturn {
        Map<String, Object> document;
        List<Node> childrenNodes;

        public RecursiveReturn() {
            document = new HashMap<>();
            childrenNodes = new ArrayList<>();
        }
    }

    public List<Node> upsertDocument(Map inDocument) {
        upsertDocument(inDocument, 0);
        return nodeList;
    }

    private Node upsertDocument(Map inDocument, int level) {

        RecursiveReturn recursiveResult = recursiveNavigation(inDocument, level);
        List<Node> childrenNodes = recursiveResult.childrenNodes;
        Map<String, Object> document = recursiveResult.document;
        IdBuilder documentId = this.documentIdBuilder.buildId(document);
        Label label = this.documentLabelBuilder.buildLabel(document);

        Node node = findNodeIntoGraphDb(label, documentId);

        if (node == null) {
            if (this.writeToGraph) {
                node = db.createNode(label);
            } else {
                node = new VirtualNode(new Label[]{label}, Collections.emptyMap(), db);
            }
        }

        //set properties
        final Node n = node;
        document.forEach((k, v) -> {
            if (v != null) {
                n.setProperty(k, v);
            }
        });

        //build relationship
        childrenNodes.forEach(child -> documentRelationBuilder.buildRelation(n, child));

        nodeList.add(node);
        return node;
    }

    private Node findNodeIntoGraphDb(Label label, IdBuilder documentId) {
        Node node = null;
        //check if node already exists
        String query = "MATCH (n:" + label.name() + " {" + documentId.toCypherFilter() + "}) RETURN n";

        Result result = db.execute(query);
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            node = (Node) row.get("n");
        }
        return node;
    }

    private RecursiveReturn recursiveNavigation(Map inDocument, int level) {
        RecursiveReturn result = new RecursiveReturn();

        //extract child, recursive
        inDocument.forEach((k, v) -> {
            String fieldKey = (String) k;

            //if value is a complex object (map)
            if (v instanceof Map) {
                Map inner = (Map) v;
                manageComplexType(result, inner, level);
            } else if (v instanceof List) {
                //if value is and array
                List list = (List) v;
                manageArrayType(result, fieldKey, list, level);
            } else {
                //if value is a primitive type
                result.document.put(fieldKey, v);
            }
        });

        return result;
    }

    private void manageArrayType(RecursiveReturn result, String fieldKey, List list, int level) {
        if (!list.isEmpty()) {
            Object object = list.get(0);
            //assumption: homogeneous array

            //if is an array of complex type
            if (object instanceof Map) {
                list.forEach(m -> {
                    Map inner = (Map) m;
                    manageComplexType(result, inner, level);
                });
            } else {
                //if is an array of primitive type
                result.document.put(fieldKey, list.stream()
                        .map(o -> String.valueOf(o))
                        .toArray(size -> new String[size]));
            }
        }
    }

    private void manageComplexType(RecursiveReturn result, Map inner, int level) {
        Node child = upsertDocument(inner, ++level);
        result.childrenNodes.add(child);
    }

}
