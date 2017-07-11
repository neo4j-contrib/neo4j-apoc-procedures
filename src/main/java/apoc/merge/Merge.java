package apoc.merge;

import apoc.result.*;
import apoc.util.Util;
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

        String labels = labelNames.stream().map(s -> wrapInBacktics(s)).collect(Collectors.joining(":"));

        Map<String, Object> params = new HashMap<>();
        params.put("props", Util.merge(identProps, props));
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = "MERGE (n:" + labels + "{" + identPropsString + "}) ON CREATE SET n += $props RETURN n";
        Node node = Iterators.single(db.execute(cypher, params ).columnAs("n"));
        return Stream.of(new NodeResult(node));
    }

    private String wrapInBacktics(String s) {
        return "`" + s + "`";
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.merge.relationship(startNode, relType,  {key:value, ...}, {key:value, ...}, endNode) - merge relationship with dynamic type")
    public Stream<RelationshipResult> relationship(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                   @Name("identProps") Map<String, Object> identProps, @Name("props") Map<String, Object> props, @Name("endNode") Node endNode) {
        String identPropsString = buildIdentPropsString(identProps);

        Map<String, Object> params = new HashMap<>();
        params.put("props", Util.merge(identProps, props));
        params.put("startNode", startNode);
        params.put("endNode", endNode);

        final String cypher = "WITH $startNode as startNode, $endNode as endNode MERGE (startNode)-[r:"+ wrapInBacktics(relType) +"{"+identPropsString+"}]->(endNode) ON CREATE SET r+= $props RETURN r";
        Relationship rel = Iterators.single(db.execute(cypher, params ).columnAs("r"));
        return Stream.of(new RelationshipResult(rel));
    }

    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps==null) {
            return "";
        } else {
            return identProps.keySet().stream().map(s -> "`"+s+"`:$props.`" + s+"`").collect(Collectors.joining(","));
        }
    }

    private Map<String, Object> buildParams(Map<String, Object> identProps, Map<String, Object> props) {
        Map<String, Object> map = identProps == null ? new HashMap<>() : new HashMap<>(identProps);
        map.put("props", props == null ? Collections.EMPTY_MAP : props);
        return map;
    }

}
