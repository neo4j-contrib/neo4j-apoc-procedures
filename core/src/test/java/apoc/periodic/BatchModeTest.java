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
package apoc.periodic;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BatchModeTest {
    @Test
    public void useBatchModeIfDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("batchMode", BatchMode.BATCH_SINGLE.name());

        BatchMode batchMode = BatchMode.fromConfig(config);
        assertEquals(BatchMode.BATCH_SINGLE, batchMode);
    }

    @Test
    public void useIterateListIfBatchModeNotDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("iterateList", false);

        BatchMode batchMode = BatchMode.fromConfig(config);
        assertEquals(BatchMode.SINGLE, batchMode);
    }

    @Test
    public void batchModeByDefault() {
        BatchMode batchMode = BatchMode.fromConfig(new HashMap<>());
        assertEquals(BatchMode.BATCH, batchMode);
    }

    @Test
    public void ignoreCaseOfBatchMode() {
        Map<String, Object> config = new HashMap<>();
        config.put("batchMode", BatchMode.BATCH_SINGLE.name().toLowerCase());

        BatchMode batchMode = BatchMode.fromConfig(config);
        assertEquals(BatchMode.BATCH_SINGLE, batchMode);
    }

    @Test
    public void throwExceptionForInvalidBatchMode() {
        Map<String, Object> config = new HashMap<>();
        config.put("batchMode", "random");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            BatchMode.fromConfig(config);
        });

        assertEquals("Invalid batch mode: `random`. Valid values are: [BATCH, BATCH_SINGLE, SINGLE]", exception.getMessage());
    }
}