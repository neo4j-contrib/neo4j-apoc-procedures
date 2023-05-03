/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.refactor.util;

import apoc.util.ArrayBackedList;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Relationship;

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
            String mergeMode = refactorConfig.getMergeMode(key, target instanceof Relationship);
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
            if (array != null) {
                target.setProperty(prop.getKey(), array);
            }
        }
    }

    private static Object createPropertyValueFromSet(Set<Object> input, RefactorConfig refactorConfig) {
        Object array;
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
        } catch (NegativeArraySizeException | ClassNotFoundException e) {
            return null;
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
