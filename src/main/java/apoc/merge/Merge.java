package apoc.merge;

import apoc.result.*;
import apoc.util.Util;
import com.google.common.collect.Lists;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Merge {

    @Context
    public GraphDatabaseService db;

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.merge.node(['Label'], {key:value, ...}, {key:value,...}) - merge node with dynamic labels")
    public Stream<NodeResult> node(@Name("label") List<String> labelNames, @Name("identProps") Map<String, Object> identProps, @Name("props") Map<String, Object> props) {
        if ((identProps==null) || (identProps.isEmpty())) {
            throw new IllegalArgumentException("you need to supply at least one identifying property for a merge");
        }

        String labels = labelNames.stream().map(s -> Util.quote(s)).collect(Collectors.joining(":"));

        Map<String, Object> params = new HashMap<>();
        params.put("identProps", identProps);
        params.put("props", props);
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = "MERGE (n:" + labels + "{" + identPropsString + "}) ON CREATE SET n += $props RETURN n";
        Node node = Iterators.single(db.execute(cypher, params ).columnAs("n"));
        return Stream.of(new NodeResult(node));
    }

    @Procedure(value="apoc.merge.node.eager", mode = Mode.WRITE, eager = true)
    @Description("apoc.merge.node.eager(['Label'], identProps:{key:value, ...}, config:{onCreateProps:{key:value,...}, onMatchProps:{key:value,...}}) - merge node with dynamic labels, with support for setting properties ON CREATE or ON MATCH")
    public Stream<NodeResult> nodeEager(@Name("label") List<String> labelNames, @Name("identProps") Map<String, Object> identProps, @Name(value="config", defaultValue = "{}") Map<String, Object> config) {
        if ((identProps==null) || (identProps.isEmpty())) {
            throw new IllegalArgumentException("you need to supply at least one identifying property for a merge");
        }

        validateKeys(config.keySet(), Lists.newArrayList("onCreateProps", "onMatchProps"));

        String labels = labelNames.stream().map(s -> Util.quote(s)).collect(Collectors.joining(":"));

        Map<String, Object> onCreateProps = (Map<String, Object>) config.getOrDefault("onCreateProps", Collections.emptyMap());
        Map<String, Object> onMatchProps = (Map<String, Object>) config.getOrDefault("onMatchProps", Collections.emptyMap());

        Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", onCreateProps, "onMatchProps", onMatchProps);
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = "MERGE (n:" + labels + "{" + identPropsString + "}) ON CREATE SET n += $onCreateProps ON MATCH SET n += $onMatchProps RETURN n";
        return db.execute(cypher, params ).columnAs("n").stream().map(node -> new NodeResult((Node) node));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.merge.relationship(startNode, relType,  identProps:{key:value, ...}, {key:value, ...}, endNode) - merge relationship with dynamic type")
    public Stream<RelationshipResult> relationship(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                   @Name("identProps") Map<String, Object> identProps, @Name("props") Map<String, Object> props, @Name("endNode") Node endNode) {
        String identPropsString = buildIdentPropsString(identProps);

        Map<String, Object> params = new HashMap<>();
        params.put("identProps", identProps);
        params.put("props", props != null ? props : Collections.emptyMap());
        params.put("startNode", startNode);
        params.put("endNode", endNode);

        final String cypher = "WITH $startNode as startNode, $endNode as endNode MERGE (startNode)-[r:"+ Util.quote(relType) +"{"+identPropsString+"}]->(endNode) ON CREATE SET r+= $props RETURN r";
        Relationship rel = Iterators.single(db.execute(cypher, params ).columnAs("r"));
        return Stream.of(new RelationshipResult(rel));
    }

    @Procedure(value = "apoc.merge.relationship.eager", mode = Mode.WRITE, eager = true)
    @Description("apoc.merge.relationship.eager(startNode, relType,  identProps:{key:value, ...}, config:{onCreateProps:{key:value, ...}, onMatchProps:{key:value, ...}}, endNode) - merge relationship with dynamic type, with support for setting properties ON CREATE or ON MATCH")
    public Stream<RelationshipResult> relationshipEager(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                   @Name("identProps") Map<String, Object> identProps, @Name("config") Map<String, Object> config, @Name("endNode") Node endNode) {
        validateKeys(config.keySet(), Lists.newArrayList("onCreateProps", "onMatchProps"));

        String identPropsString = buildIdentPropsString(identProps);

        Map<String, Object> onCreateProps = (Map<String, Object>) config.getOrDefault("onCreateProps", Collections.emptyMap());
        Map<String, Object> onMatchProps = (Map<String, Object>) config.getOrDefault("onMatchProps", Collections.emptyMap());

        Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", onCreateProps, "onMatchProps", onMatchProps,
                                                "startNode", startNode, "endNode", endNode);

        final String cypher = "WITH $startNode as startNode, $endNode as endNode MERGE (startNode)-[r:"+ Util.quote(relType) +"{"+identPropsString+"}]->(endNode) ON CREATE SET r+= $onCreateProps ON MATCH SET r+= $onMatchProps RETURN r";
        return db.execute(cypher, params ).columnAs("r").stream().map(rel -> new RelationshipResult((Relationship) rel));
    }

    private void validateKeys(Set<String> mapKeys, List<String> allowedKeys) {
        Set<String> copy = new HashSet<>(mapKeys);
        copy.removeAll(allowedKeys);

        if (!copy.isEmpty()) {
            throw new IllegalArgumentException("Config map may only take the following: " + allowedKeys.toString() + ". Unknown keys found: " + copy.toString());
        }
    }

    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps==null) {
            return "";
        } else {
            return identProps.keySet().stream().map(s -> "`"+s+"`:$identProps.`" + s+"`").collect(Collectors.joining(","));
        }
    }
}
