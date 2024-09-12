package apoc.convert;

import apoc.Extended;
import apoc.util.JsonUtil;
import apoc.util.MissingDependencyException;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import static apoc.convert.ConvertExtendedUtil.toValidYamlValue;

@Extended
public class ConvertExtended {
    public static final String MAPPING_KEY = "mapping";

    private static final String YAML_MISSING_DEPS_ERROR = "Cannot find the Yaml client jar.\n" +
            "Please put the apoc-yaml-dependencies-5.x.x-all.jar into plugin folder.\n" +
            "See the documentation: https://neo4j.com/labs/apoc/5/overview/apoc.convert/apoc.convert.toYaml/#yaml-dependencies";
    
    @UserFunction("apoc.convert.fromYaml")
    @Description("apoc.convert.fromYaml(value, $config) - Deserializes the YAML string to Neo4j value")
    public Object fromYaml(@Name("value") String value, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        try {
            Object parse = parse(value, config);
            Map mappingConf = (Map<String, Object>) config.getOrDefault(MAPPING_KEY, Map.of());
            return toValidYamlValue(parse, null, mappingConf, true);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(YAML_MISSING_DEPS_ERROR);
        }
    }





}
