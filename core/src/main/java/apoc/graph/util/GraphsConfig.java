package apoc.graph.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.Util.toBoolean;

public class GraphsConfig {

    private static final Pattern MAPPING_PATTERN = Pattern.compile("^(\\w+\\s*(?::\\s*(?:\\w+)\\s*)*)\\s*(?:\\{\\s*(-?[\\*\\w!@\\.]+\\s*(?:,\\s*-?[!@\\w\\*\\.]+\\s*)*)\\})?$");

    public static class GraphMapping {

        private static final String IDS = "ids";
        private static final String VALUE_OBJECTS = "valueObjects";
        private static final String PROPERTIES = "properties";
        private static final String WILDCARD = "*";

        private List<String> valueObjects = new ArrayList<>();
        private List<String> ids = new ArrayList<>();
        private List<String> properties = new ArrayList<>();
        private List<String> labels = new ArrayList<>();

        private boolean allProps = true;

        static final GraphMapping EMPTY = new GraphMapping();

        GraphMapping(List<String> valueObjects, List<String> ids, List<String> properties, List<String> labels, boolean allProps) {
            this.allProps = allProps;
            if (valueObjects != null) this.valueObjects.addAll(valueObjects);
            if (ids != null) this.ids.addAll(ids);
            if (labels != null) this.labels.addAll(labels);
            if (!this.allProps) {
                if (properties != null) this.properties.addAll(properties);
                this.properties.addAll(this.ids);
                this.properties.addAll(this.valueObjects);
            }
        }

        GraphMapping() {}

        public List<String> getValueObjects() {
            return valueObjects;
        }

        public List<String> getIds() {
            return ids;
        }

        public List<String> getProperties() {
            return properties;
        }

        public List<String> getLabels() {
            return labels;
        }

        public boolean isAllProps() {
            return allProps;
        }

        public static GraphMapping from(String pattern) {
            Matcher matcher = MAPPING_PATTERN.matcher(pattern);
            if (!matcher.matches()) {
                throw new RuntimeException("The provided pattern " + pattern + " does not match the requirements");
            }
            List<String> labels = Arrays.asList(matcher.group(1).split(":"));
            AtomicBoolean allProps = new AtomicBoolean(false);
            Map<String, List<String>> map = Stream.of(matcher.group(2).split(","))
                    .map(s -> {
                        String value = s.trim();
                        String key;
                        if (value.startsWith("@")) {
                            key = VALUE_OBJECTS;
                            value = value.substring(1);
                        } else if (value.startsWith("!")) {
                            key = IDS;
                            value = value.substring(1);
                        } else {
                            key = PROPERTIES;
                        }
                        if (WILDCARD.equals(value) && key.equals(PROPERTIES)) {
                            allProps.set(true);
                            return null;
                        } else {
                            return new AbstractMap.SimpleEntry<>(key, value);
                        }
                    })
                    .filter(e -> e != null)
                    .collect(Collectors.groupingBy(Map.Entry::getKey,
                            Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
            return new GraphMapping(map.get(VALUE_OBJECTS), map.get(IDS), map.get(PROPERTIES), labels, allProps.get());
        }
    }

    private boolean write;
    private String labelField;
    private String idField;
    private boolean generateId;
    private String defaultLabel;
    private Map<String, GraphMapping> mappings;

    private boolean skipValidation;

    public GraphsConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        write = toBoolean(config.getOrDefault("write", false));
        generateId = toBoolean(config.getOrDefault("generateId", true));
        idField = config.getOrDefault("idField", "id").toString();
        labelField = config.getOrDefault("labelField", "type").toString();
        defaultLabel = config.getOrDefault("defaultLabel", "DocNode").toString();
        mappings = toMappings((Map<String, String>) config.getOrDefault("mappings", Collections.emptyMap()));
        skipValidation = toBoolean(config.getOrDefault("skipValidation", false));
    }

    private Map<String, GraphMapping> toMappings(Map<String, String> mappings) {
        return mappings.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), GraphMapping.from(e.getValue())))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    public boolean isWrite() {
        return write;
    }

    public String getLabelField() {
        return labelField;
    }

    public String getIdField() {
        return idField;
    }

    public boolean isGenerateId() {
        return generateId;
    }

    public String getDefaultLabel() {
        return defaultLabel;
    }

    public boolean isSkipValidation() {
        return skipValidation;
    }

    public List<String> valueObjectForPath(String path) {
        return mappings.getOrDefault(path, GraphMapping.EMPTY).getValueObjects();
    }

    public List<String> idsForPath(String path) {
        return mappings.getOrDefault(path, GraphMapping.EMPTY).getIds();
    }

    public List<String> labelsForPath(String path) {
        return mappings.getOrDefault(path, GraphMapping.EMPTY).getLabels();
    }

    public List<String> propertiesForPath(String path) {
        if (allPropertiesForPath(path)) {
            return Collections.emptyList();
        }
        // We also need to consider the properties defined in the mapping fields
        final List<String> pathProperties = mappings.keySet()
                .stream()
                .filter(key -> key.startsWith(path))
                .map(key -> path.length() >= key.length() ? "" : key.substring(path.length() + 1))
                .map(key -> key.split("\\.")[0])
                .filter(key -> !key.isEmpty())
                .collect(Collectors.toList());
        List<String> properties = mappings.getOrDefault(path, GraphMapping.EMPTY).getProperties();
        properties.addAll(pathProperties);
        return properties;
    }

    public boolean allPropertiesForPath(String path) {
        return mappings.getOrDefault(path, GraphMapping.EMPTY).isAllProps();
    }
}
