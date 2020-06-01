package apoc.util;

import org.assertj.core.api.AbstractAssert;

import java.util.*;

public class MapAssertions extends AbstractAssert<MapAssertions, Map<String,Object>> {
    private final Collection<String> blackListedKeys;
    private final List<String> propertyPath;

    private MapAssertions(Map<String, Object> stringObjectMap, Collection<String> blackListedKeys, List<String> propertyPath) {
        super(stringObjectMap, MapAssertions.class);
        this.blackListedKeys = blackListedKeys;
        this.propertyPath = propertyPath;
    }

    public MapAssertions(Map<String, Object> stringObjectMap, Collection<String> blackListedKeys) {
        this(stringObjectMap, blackListedKeys, new ArrayList<>());
    }

    public static MapAssertions assertThat(Map<String, Object> actual, List<String> blackListedKeys) {
            return new MapAssertions(actual, blackListedKeys);
    }

    public MapAssertions isEqualsTo(Map<String, Object> expected) {
        isNotNull();

        if (actual.size() != expected.size() ) {
            failWithMessage("not same number of elements: %s actual has %d elements, expected %s has %d elements", actual, actual.size(), expected, expected.size());
        } else {

            actual.keySet().stream().filter(k -> !blackListedKeys.contains(k)).forEach(k -> {

                Object actualValue = actual.get(k);
                Object expectedValue = expected.get(k);

                if (actualValue instanceof Collection) {
                    System.out.println("coll");
                } else if (actualValue instanceof Map) {
                    System.out.println("map");
                } else {

                    if (!Objects.equals(actualValue, expectedValue)) {
                        failWithMessage("property %s differs: actual %s, expected %s", String.join(".", propertyPath) + "." + k, actualValue, expectedValue);
                    }

                }
            });
        }
        return this;
    }
}
