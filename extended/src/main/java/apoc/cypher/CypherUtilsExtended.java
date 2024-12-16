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
package apoc.cypher;

import apoc.result.CypherStatementMapResult;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Name;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;

public class CypherUtilsExtended {
    public static Stream<CypherStatementMapResult> runCypherQuery(
            Transaction tx, @Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return tx.execute(withParamMapping(statement, params.keySet()), params).stream()
                .map(CypherStatementMapResult::new);
    }

    public static String withParamMapping(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH "
                + join(
                        ", ",
                        keys.stream().map(s -> format(" $`%s` as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
    }
}
