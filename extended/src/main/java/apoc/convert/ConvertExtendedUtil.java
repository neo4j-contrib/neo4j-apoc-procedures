package apoc.convert;

import apoc.export.util.DurationValueSerializerExtended;
import apoc.export.util.PointSerializerExtended;
import apoc.export.util.TemporalSerializerExtended;
import apoc.util.collection.IterablesExtended;
import apoc.util.collection.IteratorsExtended;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

import java.lang.reflect.Array;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConvertExtendedUtil {

    private static final SimpleModule YAML_MODULE = new SimpleModule("Neo4jApocYamlSerializer");
    public static final String MAPPING_KEY = "mapping";

    static {
        YAML_MODULE.addSerializer(Point.class, new PointSerializerExtended());
        YAML_MODULE.addSerializer(Temporal.class, new TemporalSerializerExtended());
        YAML_MODULE.addSerializer(DurationValue.class, new DurationValueSerializerExtended());
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

    public static List convertToListExtended(Object list) {
        if (list == null) return null;
        else if (list instanceof List) return (List) list;
        else if (list instanceof Collection) return new ArrayList((Collection) list);
        else if (list instanceof Iterable) return IterablesExtended.asList((Iterable) list);
        else if (list instanceof Iterator) return IteratorsExtended.asList((Iterator) list);
        else if (list.getClass().isArray()) {
            return convertArrayToListExtended(list);
        }
        return Collections.singletonList(list);
    }

    public static List convertArrayToListExtended(Object list) {
        final Object[] objectArray;
        if (list.getClass().getComponentType().isPrimitive()) {
            int length = Array.getLength(list);
            objectArray = new Object[length];
            for (int i = 0; i < length; i++) {
                objectArray[i] = Array.get(list, i);
            }
        } else {
            objectArray = (Object[]) list;
        }
        List result = new ArrayList<>(objectArray.length);
        Collections.addAll(result, objectArray);
        return result;
    }
}
