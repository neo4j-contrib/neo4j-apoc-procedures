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
package apoc.export.cypher.formatter;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public enum CypherFormatExtended {
    CREATE("create") {
        @Override
        public CypherFormatterExtended getFormatter() {
            return new CreateCypherFormatterExtended();
        }
    },
    ADD_STRUCTURE("addStructure") {
        @Override
        public CypherFormatterExtended getFormatter() {
            return new AddStructureCypherFormatterExtended();
        }
    },
    UPDATE_STRUCTURE("updateStructure") {
        @Override
        public CypherFormatterExtended getFormatter() {
            return new UpdateStructureCypherFormatterExtended();
        }
    },
    UPDATE_ALL("updateAll") {
        @Override
        public CypherFormatterExtended getFormatter() {
            return new UpdateAllCypherFormatterExtended();
        }
    };

    private String value;

    CypherFormatExtended(String value) {
        this.value = value;
    }

    public static CypherFormatExtended fromString(String value) {
        if (value != null && !"".equals(value)) {
            for (CypherFormatExtended formatType : CypherFormatExtended.values()) {
                if (formatType.value.equalsIgnoreCase(value)) {
                    return formatType;
                }
            }
        }
        return CREATE;
    }

    public abstract CypherFormatterExtended getFormatter();

    public String toString() {
        return this.value;
    }
}
