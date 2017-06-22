package apoc.merge;

import apoc.result.*;
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
        String labels = String.join(":", labelNames);

        Map<String, Object> params = buildParams(identProps, props);
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = "MERGE (n:" + labels + "{" + identPropsString + "}) ON CREATE SET n += $props RETURN n";
        Node node = Iterators.single(db.execute(cypher, params ).columnAs("n"));
        return Stream.of(new NodeResult(node));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.merge.relationship(startNode, relType,  {key:value, ...}, {key:value, ...}, endNode) - merge relationship with dynamic type")
    public Stream<RelationshipResult> relationship(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                   @Name("identProps") Map<String, Object> identProps, @Name("props") Map<String, Object> props, @Name("endNode") Node endNode) {
        Map<String, Object> params = buildParams(identProps, props);
        String identPropsString = buildIdentPropsString(identProps);

        params.put("startNode", startNode);
        params.put("endNode", endNode);

        final String cypher = "WITH $startNode as startNode, $endNode as endNode MERGE (startNode)-[r:"+ relType +"{"+identPropsString+"}]->(endNode) ON CREATE SET r+= $props RETURN r";
        Relationship rel = Iterators.single(db.execute(cypher, params ).columnAs("r"));
        return Stream.of(new RelationshipResult(rel));
    }

    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps==null) {
            return "";
        } else {
            return identProps.keySet().stream().map(s -> "`"+s+"`:$`" + s+"`").collect(Collectors.joining(","));
        }
    }

    private Map<String, Object> buildParams(Map<String, Object> identProps, Map<String, Object> props) {
        Map<String, Object> map = identProps == null ? new HashMap<>() : new HashMap<>(identProps);
        map.put("props", props == null ? Collections.EMPTY_MAP : props);
        return map;
    }

}
