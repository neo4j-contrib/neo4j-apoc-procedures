package apoc.convert;

import apoc.Extended;
import apoc.meta.Types;
import apoc.util.MissingDependencyException;
import apoc.util.collection.Iterables;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.convert.ConvertExtendedUtil.MAPPING_KEY;
import static apoc.convert.ConvertExtendedUtil.getYamlFactory;
import static apoc.convert.ConvertExtendedUtil.parse;
import static apoc.util.ExtendedUtil.toValidYamlValue;
import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

@Extended
public class ConvertExtended {
    private static final String YAML_MISSING_DEPS_ERROR = """
            Cannot find the Yaml client jar.
            Please put the apoc-yaml-dependencies-5.x.x-all.jar into plugin folder.
            See the documentation: https://neo4j.com/labs/apoc/5/overview/apoc.convert/apoc.convert.toYaml/#yaml-dependencies""";

    
    @UserFunction("apoc.convert.toYaml")
    @Description("apoc.convert.toYaml(value, $config) - Serializes the given value to a YAML string")
    public String toYaml(@Name("value") Object value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        Object result = writeYamlResult(value);
        try {
            return getYamlFactory(result, config);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(YAML_MISSING_DEPS_ERROR);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + "value" + " to yaml", e);
        }
    }

    @UserFunction("apoc.convert.fromYaml")
    @Description("apoc.convert.fromYaml(value, $config) - Deserializes the YAML string to Neo4j value")
    public Object fromYaml(@Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        try {
            Object parse = parse(value, config);
            var mappingConf = (Map<String, Object>) config.getOrDefault(MAPPING_KEY, Map.of());
            return toValidYamlValue(parse, null, mappingConf, true);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(YAML_MISSING_DEPS_ERROR);
        }
    }

    /**
     * convert result recursively, 
     * which handle complex types, like list/map of nodes/rels/paths
     */
    private Object writeYamlResult(Object value) {
        Types type = Types.of(value);
        return switch (type) {
            case NODE -> nodeToMap((Node) value);
            case RELATIONSHIP -> relToMap((Relationship) value);
            
            case PATH -> writeYamlResult(Iterables.stream((Path) value)
                    .map(i -> i instanceof Node ? nodeToMap((Node) i) : relToMap((Relationship) i))
                    .collect(Collectors.toList()));
            
            case LIST -> ConvertUtils.convertToList(value).stream()
                    .map(this::writeYamlResult)
                    .collect(Collectors.toList());
            
            case MAP -> ((Map<String, Object>) value)
                    .entrySet()
                    .stream()
                    .collect(
                            HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                            (mapAccumulator, entry) ->
                                    mapAccumulator.put(entry.getKey(), writeYamlResult(entry.getValue())),
                            HashMap::putAll);
            
            default -> value;
        };
    }

    private Map<String, Object> relToMap(Relationship rel) {
        Map<String, Object> mapRel = map(
                "id", rel.getElementId(),
                "type", "relationship",
                "label", rel.getType().toString(),
                "start", nodeToMap(rel.getStartNode()),
                "end", nodeToMap(rel.getEndNode()));

        return mapWithOptionalProps(mapRel, rel.getAllProperties());
    }

    private Map<String, Object> nodeToMap(Node node) {
        Map<String, Object> mapNode = map("id", node.getElementId());

        mapNode.put("type", "node");

        if (node.getLabels().iterator().hasNext()) {
            mapNode.put("labels", labelStrings(node));
        }
        return mapWithOptionalProps(mapNode, node.getAllProperties());
    }

    private Map<String, Object> mapWithOptionalProps(Map<String, Object> mapEntity, Map<String, Object> props) {
        if (!props.isEmpty()) {
            mapEntity.put("properties", props);
        }
        return mapEntity;
    }
}
