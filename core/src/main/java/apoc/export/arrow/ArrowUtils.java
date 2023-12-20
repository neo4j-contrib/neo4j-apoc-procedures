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
package apoc.export.arrow;

import java.util.List;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ArrowUtils {

    private ArrowUtils() {}

    public static Field FIELD_ID = new Field("<id>", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
    public static Field FIELD_LABELS = new Field(
            "labels",
            FieldType.nullable(Types.MinorType.LIST.getType()),
            List.of(new Field("$data$", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)));
    public static Field FIELD_SOURCE_ID =
            new Field("<source.id>", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
    public static Field FIELD_TARGET_ID =
            new Field("<target.id>", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
    public static Field FIELD_TYPE = new Field("<type>", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
}
