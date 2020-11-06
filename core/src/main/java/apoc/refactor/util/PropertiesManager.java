package apoc.refactor.util;

import apoc.util.ArrayBackedList;
import org.neo4j.graphdb.Entity;

import java.lang.reflect.Array;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 * @since 3.0.0
 */
public class PropertiesManager {

    private PropertiesManager() {
    }

    public static void mergeProperties(Map<String, Object> properties, Entity target, RefactorConfig refactorConfig) {
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            String key = prop.getKey();
            String mergeMode = refactorConfig.getMergeMode(key);
            mergeProperty(target, refactorConfig, prop, key, mergeMode);
        }
    }

    private static void mergeProperty(Entity target, RefactorConfig refactorConfig, Map.Entry<String, Object> prop, String key, String mergeMode) {
        switch (mergeMode) {
            case RefactorConfig.OVERWRITE:
            case RefactorConfig.OVERRIDE:
                target.setProperty(key, prop.getValue());
                break;
            case RefactorConfig.DISCARD:
                if (!target.hasProperty(key)) {
                    target.setProperty(key, prop.getValue());
                }
                break;
            case RefactorConfig.COMBINE:
                combineProperties(prop, target, refactorConfig);
                break;
        }
    }

    public static void combineProperties(Map.Entry<String, Object> prop, Entity target, RefactorConfig refactorConfig) {
        if (!target.hasProperty(prop.getKey()))
            target.setProperty(prop.getKey(), prop.getValue());
        else {
            Set<Object> values = new LinkedHashSet<>();
            if (target.getProperty(prop.getKey()).getClass().isArray())
                values.addAll(new ArrayBackedList(target.getProperty(prop.getKey())));
            else
                values.add(target.getProperty(prop.getKey()));
            if (prop.getValue().getClass().isArray())
                values.addAll(new ArrayBackedList(prop.getValue()));
            else
                values.add(prop.getValue());
            Object array = createPropertyValueFromSet(values, refactorConfig);
            target.setProperty(prop.getKey(), array);
        }
    }

    private static Object createPropertyValueFromSet(Set<Object> input, RefactorConfig refactorConfig) {
        Object array = null;
        try {
            if (input.size() == 1 && !refactorConfig.isSingleElementAsArray()) {
                return input.toArray()[0];
            } else {
                if (sameTypeForAllElements(input)) {
                    Class clazz = Class.forName(input.toArray()[0].getClass().getName());
                    array = Array.newInstance(clazz, input.size());
                    System.arraycopy(input.toArray(), 0, array, 0, input.size());
                } else {
                    array = new String[input.size()];
                    Object[] elements = input.toArray();
                    for (int i = 0; i < elements.length; i++) {
                        ((String[]) array)[i] = String.valueOf(elements[i]);
                    }
                }
            }
        } catch (Exception e) {
        }
        return array;
    }

    private static boolean sameTypeForAllElements(Set<Object> input) {
        Object[] elements = input.toArray();
        Class first = elements[0].getClass();
        for (int i = 1; i < elements.length; i++) {
            if (!first.equals(elements[i].getClass()))
                return false;
        }
        return true;
    }
}
