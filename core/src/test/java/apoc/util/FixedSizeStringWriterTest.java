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

import static org.junit.Assert.*;

import org.junit.Test;

public class FixedSizeStringWriterTest {

    @Test
    public void shouldStoreASubstring() {
        FixedSizeStringWriter writer = new FixedSizeStringWriter(10);
        writer.write("This is");
        writer.write(" a string");
        assertEquals("This is a ", writer.toString());
        assertTrue("Should be exceeded", writer.isExceeded());
    }

    @Test
    public void shouldStoreASubstring1() {
        FixedSizeStringWriter writer = new FixedSizeStringWriter(10);
        char[] chars = "This is".toCharArray();
        writer.write(chars, 0, chars.length);
        writer.write(" a string");
        assertEquals("This is a ", writer.toString());
        assertTrue("Should be exceeded", writer.isExceeded());
    }

    @Test
    public void shouldStoreASubstring2() {
        FixedSizeStringWriter writer = new FixedSizeStringWriter(10);
        char[] thisIs = "This is".toCharArray();
        writer.write(thisIs, 5, thisIs.length);
        char[] aString = " a string".toCharArray();
        writer.write(aString, 0, 6);
        assertEquals("is a str", writer.toString());
        assertFalse("Should not be exceeded", writer.isExceeded());
    }
}
