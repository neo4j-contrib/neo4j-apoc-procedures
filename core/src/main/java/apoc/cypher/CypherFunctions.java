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

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.cypher.Cypher.withParamMapping;

/**
 * Created by lyonwj on 9/29/17.
 */
public class CypherFunctions {
    @Context
    public Transaction tx;

    @UserFunction
    @Deprecated
    @Description("use either apoc.cypher.runFirstColumnMany for a list return or apoc.cypher.runFirstColumnSingle for returning the first row of the first column")
    public Object runFirstColumn(@Name("cypher") String statement, @Name("params") Map<String, Object> params, @Name(value = "expectMultipleValues",defaultValue = "true") boolean expectMultipleValues) {
        if (params == null) params = Collections.emptyMap();
        String resolvedStatement = withParamMapping(statement, params.keySet());
        if (!resolvedStatement.contains(" runtime")) resolvedStatement = "cypher runtime=slotted " + resolvedStatement;
        try (Result result = tx.execute(resolvedStatement, params)) {

        String firstColumn = result.columns().get(0);
        try (ResourceIterator<Object> iter = result.columnAs(firstColumn)) {
            if (expectMultipleValues) return iter.stream().collect(Collectors.toList());
            return iter.hasNext() ? iter.next() : null;
        }
      }
    }

    @UserFunction
    @Description("apoc.cypher.runFirstColumnMany(statement, params) - executes statement with given parameters, returns first column only collected into a list, params are available as identifiers")
    public List<Object> runFirstColumnMany(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return (List)runFirstColumn(statement, params, true);
    }
    @UserFunction
    @Description("apoc.cypher.runFirstColumnSingle(statement, params) - executes statement with given parameters, returns first element of the first column only, params are available as identifiers")
    public Object runFirstColumnSingle(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return runFirstColumn(statement, params, false);
    }
}
