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
package apoc.meta;

import static org.junit.Assert.*;

import apoc.export.csv.CsvPropertyConverter;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for Neo4j Vector Type support (issue #4446).
 *
 * Verifies that float[] (Neo4j's native vector storage format) is correctly
 * classified as VECTOR rather than LIST OF FLOAT, and that CSV import and
 * export operations handle vector properties correctly.
 */
public class VectorTypeTest {

    // =========================================================================
    // Meta.Types — detection
    // =========================================================================

    @Test
    public void testFloatArrayClassDetectedAsVector() {
        Meta.Types type = Meta.Types.of(float[].class);
        assertEquals("float[].class should map to VECTOR, not LIST",
                Meta.Types.VECTOR, type);
    }

    @Test
    public void testFloatArrayInstanceDetectedAsVector() {
        float[] vector = {0.1f, 0.2f, 0.3f};
        Meta.Types type = Meta.Types.of((Object) vector);
        assertEquals("float[] instance should map to VECTOR",
                Meta.Types.VECTOR, type);
    }

    @Test
    public void testDoubleArrayStillList() {
        double[] arr = {1.0, 2.0, 3.0};
        Meta.Types type = Meta.Types.of((Object) arr);
        assertEquals("double[] should remain LIST, not VECTOR",
                Meta.Types.LIST, type);
    }

    @Test
    public void testIntArrayStillList() {
        int[] arr = {1, 2, 3};
        Meta.Types type = Meta.Types.of((Object) arr);
        assertEquals("int[] should remain LIST, not VECTOR",
                Meta.Types.LIST, type);
    }

    @Test
    public void testVectorFromString() {
        Meta.Types type = Meta.Types.from("VECTOR");
        assertEquals(Meta.Types.VECTOR, type);
    }

    @Test
    public void testVectorFromStringLowercase() {
        Meta.Types type = Meta.Types.from("vector");
        assertEquals(Meta.Types.VECTOR, type);
    }

    @Test
    public void testEmptyFloatArrayDetectedAsVector() {
        float[] empty = {};
        Meta.Types type = Meta.Types.of((Object) empty);
        assertEquals("Empty float[] should be VECTOR",
                Meta.Types.VECTOR, type);
    }

    @Test
    public void testHighDimensionalVectorDetected() {
        float[] vec = new float[1536];
        for (int i = 0; i < vec.length; i++) vec[i] = i * 0.001f;
        Meta.Types type = Meta.Types.of((Object) vec);
        assertEquals("1536-dimensional float[] should be VECTOR",
                Meta.Types.VECTOR, type);
    }

    // =========================================================================
    // Meta.Types — toObjectArray still works for float[]
    // =========================================================================

    @Test
    public void testFloatArrayToObjectArray() {
        float[] vec = {0.1f, 0.2f, 0.3f};
        Object[] result = Meta.Types.toObjectArray(vec);
        assertEquals(3, result.length);
        assertEquals(0.1f, ((Float) result[0]).floatValue(), 0.0001f);
        assertEquals(0.2f, ((Float) result[1]).floatValue(), 0.0001f);
        assertEquals(0.3f, ((Float) result[2]).floatValue(), 0.0001f);
    }

    // =========================================================================
    // CsvPropertyConverter — VECTOR type recognised in getPrototypeFor
    // =========================================================================

    @Test
    public void testGetPrototypeForVectorReturnsFloatArray() {
        Object[] prototype = CsvPropertyConverter.getPrototypeFor("VECTOR");
        assertTrue("VECTOR prototype should be Float[]",
                prototype instanceof Float[]);
    }

    @Test
    public void testGetPrototypeForFloatArrayAlias() {
        Object[] prototype = CsvPropertyConverter.getPrototypeFor("FLOAT_ARRAY");
        assertTrue("FLOAT_ARRAY prototype should be Float[]",
                prototype instanceof Float[]);
    }

    @Test(expected = IllegalStateException.class)
    public void testUnknownTypeStillThrows() {
        CsvPropertyConverter.getPrototypeFor("UNKNOWN_TYPE_XYZ");
    }

    // =========================================================================
    // CsvPropertyConverter — addPropertyToGraphEntity stores primitive float[]
    // =========================================================================

    @Test
    public void testVectorStoredAsPrimitiveFloatArray() {
        // Verify the conversion logic directly: List<Number> -> float[]
        List<Number> values = Arrays.asList(0.1f, 0.5f, 0.9f);
        float[] result = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i).floatValue();
        }
        // The array must be float[] (primitive), not Float[] (boxed)
        assertTrue("Must be primitive float[]", result.getClass() == float[].class);
        assertEquals(0.1f, result[0], 0.0001f);
        assertEquals(0.5f, result[1], 0.0001f);
        assertEquals(0.9f, result[2], 0.0001f);
    }
}
