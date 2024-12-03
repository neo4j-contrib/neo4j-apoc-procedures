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
package apoc.convert;

import apoc.util.collection.IterablesExtended;
import apoc.util.collection.IteratorsExtended;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ConvertUtilsExtended {
    @SuppressWarnings("unchecked")
    public static List convertToList(Object list) {
        if (list == null) return null;
        else if (list instanceof List) return (List) list;
        else if (list instanceof Collection) return new ArrayList((Collection) list);
        else if (list instanceof Iterable) return IterablesExtended.asList((Iterable) list);
        else if (list instanceof Iterator) return IteratorsExtended.asList((Iterator) list);
        else if (list.getClass().isArray()) {
            return convertArrayToList(list);
        }
        return Collections.singletonList(list);
    }

    public static List convertArrayToList(Object list) {
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
