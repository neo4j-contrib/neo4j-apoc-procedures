package apoc.entities;

import apoc.Extended;
import apoc.result.UpdatedNodeResult;
import apoc.result.UpdatedRelationshipResult;
import apoc.util.EntityUtilExtended;
import apoc.util.ExtendedMapUtils;
import apoc.util.ExtendedUtil;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Extended
public class EntitiesExtended {

    public static final String INVALID_LABELS_MESSAGE = "The list of label names may not contain any `NULL` or empty `STRING` values. If you wish to match a `NODE` without a label, pass an empty list instead.";
    public static final String INVALID_IDENTIFY_PROPERTY_MESSAGE = "you need to supply at least one identifying property for a match";
    public static final String INVALID_REL_TYPE_MESSAGE = "It is not possible to match a `RELATIONSHIP` without a `RELATIONSHIP` type.";

    @Context
    public Transaction tx;
    
    @UserFunction("apoc.node.rebind")
    @Description("apoc.node.rebind(node - to rebind a node (i.e. executing a Transaction.getNodeById(node.getId())  ")
    public Node nodeRebind(@Name("node") Node node) {
        return Util.rebind(tx, node);
    }

    @UserFunction("apoc.rel.rebind")
    @Description("apoc.rel.rebind(rel) - to rebind a rel (i.e. executing a Transaction.getRelationshipById(rel.getId())  ")
    public Relationship relationshipRebind(@Name("rel") Relationship rel) {
        return Util.rebind(tx, rel);
    }

    @UserFunction("apoc.any.rebind")
    @Description("apoc.any.rebind(Object) - to rebind any rel, node, path, map, list or combination of them (i.e. executing a Transaction.getNodeById(node.getId()) / Transaction.getRelationshipById(rel.getId()))")
    public Object anyRebind(@Name("any") Object any) {
        return EntityUtilExtended.anyRebind(tx, any);
    }

    @Procedure(value = "apoc.node.match", mode = Mode.WRITE)
    @Description("Matches the given `NODE` values with the given dynamic labels.")
    public Stream<UpdatedNodeResult> nodes(
            @Name(value = "labels", description = "The list of labels used for the generated MATCH statement.")
            List<String> labelNames,
            @Name(value = "identProps", description = "Properties on the node that are always matched.")
            Map<String, Object> identProps,
            @Name(
                    value = "onMatchProps",
                    defaultValue = "{}",
                    description = "Properties that are set when a node is matched.")
            Map<String, Object> onMatchProps) {
        /*
         * Partially taken from apoc.merge.nodes, modified to perform a match instead of a merge.
         */
        final Result nodeResult = getNodeResult(labelNames, identProps, onMatchProps);
        return nodeResult.columnAs("n").stream().map(node -> new UpdatedNodeResult((Node) node));
    }

    @Procedure(value = "apoc.rel.match", mode = Mode.WRITE)
    @Description("Matches the given `RELATIONSHIP` values with the given dynamic types/properties.")
    public Stream<UpdatedRelationshipResult> relationship(
            @Name(value = "startNode", description = "The start node of the relationship.") Node startNode,
            @Name(value = "relType", description = "The type of the relationship.") String relType,
            @Name(value = "identProps", description = "Properties on the relationship that are always matched.")
            Map<String, Object> identProps,
            @Name(value = "endNode", description = "The end node of the relationship.") Node endNode,
            @Name(
                    value = "onMatchProps",
                    defaultValue = "{}",
                    description = "Properties that are set when a relationship is matched.")
            Map<String, Object> onMatchProps) {
        /*
         * Partially taken from apoc.merge.relationship, modified to perform a match instead of a merge.
         */
        final Result execute = getRelResult(startNode, relType, identProps, endNode, onMatchProps);
        return execute.columnAs("r").stream().map(rel -> new UpdatedRelationshipResult((Relationship) rel));
    }

    private Result getRelResult(
            Node startNode,
            String relType,
            Map<String, Object> identProps,
            Node endNode,
            Map<String, Object> onMatchProps) {
        String identPropsString = buildIdentPropsString(identProps);
        onMatchProps = Objects.requireNonNullElse(onMatchProps, Util.map());

        if (StringUtils.isBlank(relType)) {
            throw new IllegalArgumentException(INVALID_REL_TYPE_MESSAGE);
        }

        Map<String, Object> params = Util.map(
                "identProps",
                identProps,
                "onMatchProps",
                onMatchProps,
                "startNode",
                startNode,
                "endNode",
                endNode);

        final String cypher = "WITH $startNode as startNode, $endNode as endNode "
                + "MATCH (startNode)-[r:"+ Util.quote(relType) + "{" + identPropsString + "}]->(endNode) "
                + "SET r+= $onMatchProps "
                + "RETURN r";
        return tx.execute(cypher, params);
    }

    private Result getNodeResult(
            List<String> labelNames,
            Map<String, Object> identProps,
            Map<String, Object> onMatchProps) {
        onMatchProps = Objects.requireNonNullElse(onMatchProps, Util.map());
        labelNames = Objects.requireNonNullElse(labelNames, Collections.EMPTY_LIST);

        if (ExtendedMapUtils.isEmpty(identProps)) {
            throw new IllegalArgumentException(INVALID_IDENTIFY_PROPERTY_MESSAGE);
        }

        boolean containsInvalidLabels =  labelNames.stream().anyMatch(label -> StringUtils.isBlank(label));
        if (containsInvalidLabels) {
            throw new IllegalArgumentException(INVALID_LABELS_MESSAGE);
        }

        Map<String, Object> params =
                Util.map("identProps", identProps, "onMatchProps", onMatchProps);
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = "MATCH (n" + ExtendedUtil.joinStringLabels(labelNames) + " {" + identPropsString + "}) "
                + "SET n += $onMatchProps "
                + "RETURN n";
        return tx.execute(cypher, params);
    }

    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps == null) return "";
        return identProps.keySet().stream()
                .map(Util::quote)
                .map(s -> s + ":$identProps." + s)
                .collect(Collectors.joining(","));
    }
}
