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
package apoc.uuid;

import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

public class UuidUtilTest {

    @Test
    public void fromHexToBase64() {
        var input = "290d6cba-ce94-455e-b59f-029cf1e395c5";
        var output = UuidUtil.fromHexToBase64(input);
        assertThat(output).isEqualTo("KQ1sus6URV61nwKc8eOVxQ");
    }

    @Test
    public void fromBase64ToHex() {
        var input = "KQ1sus6URV61nwKc8eOVxQ";
        var output = UuidUtil.fromBase64ToHex(input);
        assertThat(output).isEqualTo("290d6cba-ce94-455e-b59f-029cf1e395c5");
    }

    @Test
    public void fromBase64WithAlignmentToHex() {
        var input = "KQ1sus6URV61nwKc8eOVxQ==";
        var output = UuidUtil.fromBase64ToHex(input);
        assertThat(output).isEqualTo("290d6cba-ce94-455e-b59f-029cf1e395c5");
    }

    @Test
    public void shouldFailIfHexFormatIsWrong() {
        var input = "290d6cba-455e-b59f-029cf1e395c5";
        assertThatCode(() -> UuidUtil.fromHexToBase64(input)).hasMessageStartingWith("Invalid UUID string");
    }

    @Test
    public void shouldFailIfHexFormatIsEmpty() {
        var input = "";
        assertThatCode(() -> UuidUtil.fromHexToBase64(input)).hasMessageStartingWith("Invalid UUID string");
    }

    @Test
    public void shouldFailIfHexFormatIsNull() {
        assertThatCode(() -> UuidUtil.fromHexToBase64(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void shouldFailIfBase64LengthIsWrong() {
        var input1 = "KQ1sus6URV61nwKc8eO=="; // wrong length
        assertThatCode(() -> UuidUtil.fromBase64ToHex(input1)).hasMessageStartingWith("Invalid UUID length. Expected 24 characters");
        var input2 = "Q1sus6URV61nwKc8eOVxQ"; // wrong length
        assertThatCode(() -> UuidUtil.fromBase64ToHex(input2)).hasMessageStartingWith("Invalid UUID length. Expected 22 characters");
    }

    @Test
    public void shouldFailIfBase64IsEmpty() {
        assertThatCode(() -> UuidUtil.fromBase64ToHex("")).hasMessageStartingWith("Expected not empty UUID value");
    }

    @Test
    public void shouldFailIfBase64IsNull() {
        assertThatCode(() -> UuidUtil.fromBase64ToHex(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void generateBase64ForSpecificUUIDs() {
        var uuid = UUID.fromString("00000000-0000-0000-0000-000000000000");
        var uuidBase64 = UuidUtil.generateBase64Uuid(uuid);
        assertThat(uuidBase64).isEqualTo("AAAAAAAAAAAAAAAAAAAAAA");
    }
}