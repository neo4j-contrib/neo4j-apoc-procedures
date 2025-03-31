package apoc.create;

import apoc.Extended;
import apoc.result.VirtualNode;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;
import java.util.Map;

@Extended
public class CreateExtended {

    @UserFunction("apoc.create.virtual.fromNodeExtended")
    @Description(
            "Returns a virtual `NODE` from the given existing `NODE`. The virtual `NODE` only contains the requested properties.")
    public Node virtualFromNodeFunction(
            @Name(value = "node", description = "The node to generate a virtual node from.") Node node,
            @Name(value = "propertyNames", description = "The properties to copy to the virtual node.") List<String> propertyNames,
            @Name(value = "additionalProperties", defaultValue = "{}", description = "Additional properties to add to the virtual node") Map<String, Object> additionalProperties,
            @Name(value = "config", defaultValue = "{}", description = "{ wrapNodeIds = false :: BOOLEAN }") Map<String, Object> config) {
        VirtualNode virtualNode = new VirtualNode(node, propertyNames);
        additionalProperties.forEach(virtualNode::setProperty);
        return virtualNode;
    }
}
