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
package apoc.coll;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

import java.util.*;
import org.junit.Test;

/**
 * @author mh
 * @since 10.04.16
 */
public class SetBackedListExtendedTest {

    @Test
    public void testEmptyList() {
        SetBackedListExtended list = new SetBackedListExtended(EMPTY_SET);
        assertEquals(0, list.size());
        assertEquals(true, list.isEmpty());
        assertEquals(false, list.contains(1));
        assertEquals(false, list.iterator().hasNext());
        ListIterator it = list.listIterator();
        assertEquals(false, it.hasNext());
        assertEquals(-1, it.previousIndex());
        assertEquals(0, it.nextIndex());
    }

    @Test
    public void testSingleList() {
        SetBackedListExtended list = new SetBackedListExtended(singleton(1));
        assertEquals(1, list.size());
        assertEquals(false, list.isEmpty());
        assertEquals(true, list.contains(1));
        assertEquals(false, list.contains(0));
        assertEquals(true, list.iterator().hasNext());
        assertEquals(1, list.iterator().next());
        ListIterator it = list.listIterator();
        assertEquals(true, it.hasNext());
        assertEquals(-1, it.previousIndex());
        assertEquals(0, it.nextIndex());
        assertEquals(1, it.next());
        assertEquals(0, it.previousIndex());
        assertEquals(1, it.nextIndex());
        assertEquals(true, it.hasPrevious());
        assertEquals(1, it.previous());
        assertEquals(1, it.next());
        assertEquals(false, it.hasNext());
    }

    @Test
    public void testDoubleList() {
        SetBackedListExtended list = new SetBackedListExtended(new LinkedHashSet<>(asList(1, 2)));
        assertEquals(2, list.size());
        assertEquals(false, list.isEmpty());
        assertEquals(true, list.contains(1));
        assertEquals(true, list.contains(2));
        assertEquals(false, list.contains(0));
        Iterator it = list.iterator();
        assertEquals(true, it.hasNext());
        assertEquals(1, it.next());
        assertEquals(2, it.next());
        assertEquals(false, it.hasNext());
        ListIterator li = list.listIterator();
        assertEquals(true, li.hasNext());
        assertEquals(-1, li.previousIndex());
        assertEquals(0, li.nextIndex());
        assertEquals(1, li.next());
        assertEquals(0, li.previousIndex());
        assertEquals(1, li.nextIndex());
        assertEquals(true, li.hasPrevious());
        assertEquals(1, li.previous());
        assertEquals(1, li.next());
        assertEquals(true, li.hasNext());
        assertEquals(0, li.previousIndex());
        assertEquals(1, li.nextIndex());
        assertEquals(2, li.next());
        assertEquals(1, li.previousIndex());
        assertEquals(2, li.nextIndex());
        assertEquals(true, li.hasPrevious());
        assertEquals(false, li.hasNext());
        assertEquals(2, li.previous());
    }

    @Test
    public void testReverse() {
        LinkedHashSet set = new LinkedHashSet(asList(1, 2, 3, 4, 5));
        SetBackedListExtended list = new SetBackedListExtended(set);
        assertEquals(asList(1, 2, 3, 4, 5), list);

        ListIterator it = list.listIterator();
        while (it.hasNext()) it.next();
        List result = new ArrayList(set.size());
        while (it.hasPrevious()) {
            result.add(it.previous());
        }

        assertEquals(asList(5, 4, 3, 2, 1), result);
    }

    @Test
    public void testContains() {
        LinkedHashSet set = new LinkedHashSet(asList(1, 2, 3, 4, 5));
        SetBackedListExtended list = new SetBackedListExtended(set);
        assertEquals(true, list.contains(1));
        assertEquals(true, list.contains(3));
        assertEquals(false, list.contains(7));
        assertEquals(false, list.containsAll(asList(1, 2, 8)));
        assertEquals(true, list.containsAll(asList(1, 2, 5)));
    }
}