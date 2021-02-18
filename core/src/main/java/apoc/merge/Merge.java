package apoc.merge;

import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.Util.labelString;
import static java.util.Collections.emptyMap;

public class Merge {

    @Context
    public Transaction tx;

    @Procedure(value="apoc.merge.node.eager", mode = Mode.WRITE, eager = true)
    @Description("apoc.merge.node.eager(['Label'], identProps:{key:value, ...}, onCreateProps:{key:value,...}, onMatchProps:{key:value,...}}) - merge nodes eagerly, with dynamic labels, with support for setting properties ON CREATE or ON MATCH")
    public Stream<NodeResult> nodesEager(@Name("label") List<String> labelNames,
                                        @Name("identProps") Map<String, Object> identProps,
                                        @Name(value = "props",defaultValue = "{}") Map<String, Object> props,
                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        return nodes(labelNames, identProps,props,onMatchProps);
    }

    @Procedure(value="apoc.merge.node", mode = Mode.WRITE)
    @Description("\"apoc.merge.node.eager(['Label'], identProps:{key:value, ...}, onCreateProps:{key:value,...}, onMatchProps:{key:value,...}}) - merge nodes with dynamic labels, with support for setting properties ON CREATE or ON MATCH")
    public Stream<NodeResult> nodes(@Name("label") List<String> labelNames,
                                        @Name("identProps") Map<String, Object> identProps,
                                        @Name(value = "props",defaultValue = "{}") Map<String, Object> props,
                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        if (identProps==null || identProps.isEmpty()) {
            throw new IllegalArgumentException("you need to supply at least one identifying property for a merge");
        }

        String labels = labelString(labelNames);

        Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", props, "onMatchProps", onMatchProps);
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = "MERGE (n:" + labels + "{" + identPropsString + "}) ON CREATE SET n += $onCreateProps ON MATCH SET n += $onMatchProps RETURN n";
        return tx.execute(cypher, params ).columnAs("n").stream().map(node -> new NodeResult((Node) node));
    }

    @Procedure(value = "apoc.merge.relationship", mode = Mode.WRITE)
    @Description("apoc.merge.relationship(startNode, relType,  identProps:{key:value, ...}, onCreateProps:{key:value, ...}, endNode, onMatchProps:{key:value, ...}) - merge relationship with dynamic type, with support for setting properties ON CREATE or ON MATCH")
    public Stream<RelationshipResult> relationship(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                        @Name("identProps") Map<String, Object> identProps,
                                                        @Name("props") Map<String, Object> onCreateProps,
                                                        @Name("endNode") Node endNode,
                                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        String identPropsString = buildIdentPropsString(identProps);

        Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", onCreateProps==null ? emptyMap() : onCreateProps,
                "onMatchProps", onMatchProps == null ? emptyMap() : onMatchProps, "startNode", startNode, "endNode", endNode);

        final String cypher =
                "WITH $startNode as startNode, $endNode as endNode " +
                "MERGE (startNode)-[r:"+ Util.quote(relType) +"{"+identPropsString+"}]->(endNode) " +
                "ON CREATE SET r+= $onCreateProps " +
                "ON MATCH SET r+= $onMatchProps " +
                "RETURN r";
        return tx.execute(cypher, params ).columnAs("r").stream().map(rel -> new RelationshipResult((Relationship) rel));
    }
    @Procedure(value = "apoc.merge.relationship.eager", mode = Mode.WRITE, eager = true)
    @Description("apoc.merge.relationship(startNode, relType,  identProps:{key:value, ...}, onCreateProps:{key:value, ...}, endNode, onMatchProps:{key:value, ...}) - merge relationship with dynamic type, with support for setting properties ON CREATE or ON MATCH")
    public Stream<RelationshipResult> relationshipEager(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                        @Name("identProps") Map<String, Object> identProps,
                                                        @Name("props") Map<String, Object> onCreateProps,
                                                        @Name("endNode") Node endNode,
                                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        return relationship(startNode, relType, identProps, onCreateProps, endNode, onMatchProps );
    }


    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps == null) return "";
        return identProps.keySet().stream().map(Util::quote)
                .map(s -> s + ":$identProps." + s)
                .collect(Collectors.joining(","));
    }
}
