package apoc.refactor.util;

import apoc.util.ArrayBackedList;
import org.neo4j.graphdb.PropertyContainer;

import java.lang.reflect.Array;
import java.util.*;

/**
 * @author AgileLARUS
 *
 * @since 3.0.0
 */
public class PropertiesManager {

	public static final String OVERWRITE = "overwrite";
	public static final String COMBINE   = "combine";
    public static final String DISCARD = "discard";

    private PropertiesManager() {
    }

    public static void mergeProperties(Map<String, Object> properties, PropertyContainer target, String propertyManagementMode) {
        switch (propertyManagementMode) {
            case OVERWRITE:
                overwriteProperties(properties, target);
                break;
            case DISCARD:
                discardProperties(properties, target);
                break;
			case COMBINE:
				combineProperties(properties, target);
                break;
		}
	}

	public static void overwriteProperties(Map<String, Object> properties, PropertyContainer target){
		for (Map.Entry<String, Object> prop : properties.entrySet()) {
			target.setProperty(prop.getKey(), prop.getValue());
		}
	}

    public static void discardProperties(Map<String, Object> properties, PropertyContainer target){
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            if(!target.hasProperty(prop.getKey()))
                target.setProperty(prop.getKey(), prop.getValue());
        }
    }

    public static void combineProperties(Map<String, Object> properties, PropertyContainer target) {
        for (Map.Entry<String, Object> prop : properties.entrySet())
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
                Object array = createPropertyValueFromSet(values);
                target.setProperty(prop.getKey(), array);
            }
    }

    private static Object createPropertyValueFromSet(Set<Object> input) {
        Object array = null;
        try {
            if (input.size() == 1)
                return input.toArray()[0];
            else {
                if (sameTypeForAllElements(input)) {
                    Class clazz = Class.forName(input.toArray()[0].getClass().getName());
                    array = Array.newInstance(clazz, input.size());
                    System.arraycopy(input.toArray(), 0, array, 0, input.size());
                }  else {
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
