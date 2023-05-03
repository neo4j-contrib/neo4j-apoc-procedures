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
package apoc.util;

import java.lang.reflect.Array;
import java.util.AbstractList;

/**
 * @author mh
 * @since 11.06.16
 */
public class ArrayBackedList extends AbstractList {

    private final Object array;
    private int length;

    public ArrayBackedList(Object array) {
        if (!array.getClass().isArray()) throw new IllegalArgumentException("Not an array " + array);
        this.array = array;
        this.length = Array.getLength(array);
    }

    @Override
    public Object get(int index) {
        return Array.get(array, index);
    }

    @Override
    public int size() {
        return length;
    }
}
