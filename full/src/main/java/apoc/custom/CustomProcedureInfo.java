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
package apoc.custom;

import static apoc.SystemLabels.Function;
import static apoc.SystemLabels.Procedure;
import static apoc.custom.CypherProceduresHandler.FUNCTION;
import static apoc.custom.CypherProceduresHandler.PREFIX;
import static apoc.custom.CypherProceduresHandler.PROCEDURE;
import static apoc.custom.CypherProceduresUtil.getFunctionInfo;
import static apoc.custom.CypherProceduresUtil.getProcedureInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.procs.*;

public class CustomProcedureInfo {
    public String type;
    public String name;
    public String description;
    public String mode;
    public String statement;
    public List<List<String>> inputs;
    public Object outputs;
    public Boolean forceSingle;

    public CustomProcedureInfo(
            String type,
            String name,
            String description,
            String mode,
            String statement,
            List<List<String>> inputs,
            Object outputs,
            Boolean forceSingle) {
        this.type = type;
        this.name = name;
        this.description = description;
        this.statement = statement;
        this.outputs = outputs;
        this.inputs = inputs;
        this.forceSingle = forceSingle;
        this.mode = mode;
    }

    public static CustomProcedureInfo fromNode(Node node) {
        if (node.hasLabel(Procedure)) {
            return getProcedureInfo(node);
        } else if (node.hasLabel(Function)) {
            return getFunctionInfo(node);
        } else {
            throw new IllegalStateException("don't know what to do with systemdb node " + node);
        }
    }

    public static CustomProcedureInfo getCustomProcedureInfo(ProcedureSignature signature, String statement) {
        return new CustomProcedureInfo(
                PROCEDURE,
                signature.name().toString().substring(PREFIX.length() + 1),
                signature.description().orElse(null),
                signature.mode().toString().toLowerCase(),
                statement,
                convertInputSignature(signature.inputSignature()),
                Iterables.asList(Iterables.map(
                        f -> Arrays.asList(f.name(), prettyPrintType(f.neo4jType())), signature.outputSignature())),
                null);
    }

    public static CustomProcedureInfo getCustomFunctionInfo(
            UserFunctionSignature signature, boolean forceSingle, String statement) {
        return new CustomProcedureInfo(
                FUNCTION,
                signature.name().toString().substring(PREFIX.length() + 1),
                signature.description().orElse(null),
                null,
                statement,
                convertInputSignature(signature.inputSignature()),
                prettyPrintType(signature.outputType()),
                forceSingle);
    }

    public static List<List<String>> convertInputSignature(List<FieldSignature> signatures) {
        return Iterables.asList(Iterables.map(
                f -> {
                    List<String> list = new ArrayList<>(3);
                    list.add(f.name());
                    list.add(prettyPrintType(f.neo4jType()));
                    final Optional<DefaultParameterValue> defaultParameterValue = f.defaultValue();
                    defaultParameterValue.map(DefaultParameterValue::value).ifPresent(v -> list.add(v.toString()));
                    return list;
                },
                signatures));
    }

    public static String prettyPrintType(Neo4jTypes.AnyType type) {
        String s = type.toString().toLowerCase();
        if (s.endsWith("?")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }
}
