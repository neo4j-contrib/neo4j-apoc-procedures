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

import apoc.util.Util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

enum BatchMode {
    BATCH,
    BATCH_SINGLE,
    SINGLE;


    private static final Map<String, BatchMode> nameIndex =  new HashMap<>();
    static {
        for (BatchMode batchMode : BatchMode.values()) {
            nameIndex.put(batchMode.name(), batchMode);
        }
    }

   static BatchMode fromIterateList(boolean iterateList) {
        return iterateList ? BATCH : SINGLE;
    }

    static BatchMode fromConfig(Map<String,Object> config) {
        Object batchMode = config.get("batchMode");

        if(batchMode != null) {
            BatchMode lookedUpBatchMode = nameIndex.get(batchMode.toString().toUpperCase());
            if (lookedUpBatchMode == null) {
                throw new IllegalArgumentException("Invalid batch mode: `" + batchMode + "`. Valid values are: " + Arrays.toString(BatchMode.values()));
            }
            return lookedUpBatchMode;
        }

        Object iterateList = config.get("iterateList");
        if(iterateList != null) {
            return BatchMode.fromIterateList(Util.toBoolean(iterateList));
        }

        return BATCH;
    }
}
