package apoc.convert;

import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

import java.time.temporal.Temporal;
import java.util.List;
import java.util.Map;

public class ConvertExtendedUtil {

    private static final SimpleModule YAML_MODULE = new SimpleModule("Neo4jApocYamlSerializer");
    public static final String MAPPING_KEY = "mapping";

    static {
        YAML_MODULE.addSerializer(Point.class, new PointSerializer());
        YAML_MODULE.addSerializer(Temporal.class, new TemporalSerializer());
        YAML_MODULE.addSerializer(DurationValue.class, new DurationValueSerializer());
    }

    /**
     * get YAMLFactory with configured enable and disable values
     */
    public static String getYamlFactory(Object result, Map<String, Object> config) throws JsonProcessingException {
        YAMLFactory factory = new YAMLFactory();

        List<String> enable = (List<String>) config.getOrDefault("enable", List.of());
        List<String> disable = (List<String>) config.getOrDefault("disable", List.of());
        enable.forEach(name -> factory.enable(YAMLGenerator.Feature.valueOf(name)));
        disable.forEach(name -> factory.disable(YAMLGenerator.Feature.valueOf(name)));

        ObjectMapper objectMapper = new ObjectMapper(factory);
        objectMapper.registerModule(YAML_MODULE);

        return objectMapper.writeValueAsString(result);
    }

    public static Object parse(String value, Map<String, Object> config) throws JsonProcessingException {
        YAMLFactory factory = new YAMLFactory();

        List<String> enable = (List<String>) config.getOrDefault("enable", List.of());
        List<String> disable = (List<String>) config.getOrDefault("disable", List.of());
        enable.forEach(name -> factory.enable(YAMLGenerator.Feature.valueOf(name)));
        disable.forEach(name -> factory.disable(YAMLGenerator.Feature.valueOf(name)));

        ObjectMapper objectMapper = new ObjectMapper(factory);
        objectMapper.registerModule(YAML_MODULE);

        return objectMapper.readValue(value, Object.class);
    }

}
