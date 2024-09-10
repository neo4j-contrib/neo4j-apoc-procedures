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

import static apoc.custom.CypherProceduresHandler.PREFIX;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDuration;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

import apoc.SystemPropertyKeys;
import apoc.util.Util;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

public class CypherProceduresUtil {
    public static final String MAP_RESULT_TYPE = "MAPRESULT";

    public static QualifiedName qualifiedName(@Name("name") String name) {
        String[] names = name.split("\\.");
        List<String> namespace = new ArrayList<>(names.length);
        namespace.add(PREFIX);
        namespace.addAll(Arrays.asList(names));
        return new QualifiedName(namespace.subList(0, namespace.size() - 1), names[names.length - 1]);
    }

    public static Mode mode(String s) {
        return s == null ? Mode.READ : Mode.valueOf(s.toUpperCase());
    }

    public static CustomProcedureInfo getFunctionInfo(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());
        boolean forceSingle = (boolean) node.getProperty(SystemPropertyKeys.forceSingle.name(), false);
        UserFunctionSignature signature = getUserFunctionSignature(node);

        return CustomProcedureInfo.getCustomFunctionInfo(signature, forceSingle, statement);
    }

    public static CustomProcedureInfo getProcedureInfo(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());
        ProcedureSignature signature = getProcedureSignature(node);

        return CustomProcedureInfo.getCustomProcedureInfo(signature, statement);
    }

    public static UserFunctionSignature getUserFunctionSignature(Node node) {
        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty(SystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(SystemPropertyKeys.prefix.name(), new String[] {PREFIX});

        String property = (String) node.getProperty(SystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        return new UserFunctionSignature(
                new QualifiedName(prefix, name),
                inputs,
                typeof((String) node.getProperty(SystemPropertyKeys.output.name())),
                null,
                new String[0],
                description,
                "apoc.custom",
                false,
                false,
                false);
    }

    public static ProcedureSignature getProcedureSignature(Node node) {
        String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        String description = (String) node.getProperty(SystemPropertyKeys.description.name(), null);
        String[] prefix = (String[]) node.getProperty(SystemPropertyKeys.prefix.name(), new String[] {PREFIX});

        String property = (String) node.getProperty(SystemPropertyKeys.inputs.name());
        List<FieldSignature> inputs = deserializeSignatures(property);

        List<FieldSignature> outputSignature =
                deserializeSignatures((String) node.getProperty(SystemPropertyKeys.outputs.name()));
        return Signatures.createProcedureSignature(
                new QualifiedName(prefix, name),
                inputs,
                outputSignature,
                Mode.valueOf((String) node.getProperty(SystemPropertyKeys.mode.name())),
                false,
                null,
                new String[0],
                description,
                null,
                false,
                false,
                false,
                false,
                false);
    }

    public static List<FieldSignature> deserializeSignatures(String s) {
        List<Map<String, Object>> mapped = Util.fromJson(s, List.class);
        if (mapped.isEmpty()) return ProcedureSignature.VOID;
        return mapped.stream()
                .map(map -> {
                    String typeString = (String) map.get("type");
                    if (typeString.endsWith("?")) {
                        typeString = typeString.substring(0, typeString.length() - 1);
                    }
                    Neo4jTypes.AnyType type = typeof(typeString);
                    // we insert the default value only if is present
                    if (map.containsKey("default")) {
                        return FieldSignature.inputField(
                                (String) map.get("name"), type, new DefaultParameterValue(map.get("default"), type));
                    } else {
                        return FieldSignature.inputField((String) map.get("name"), type);
                    }
                })
                .collect(Collectors.toList());
    }

    public static Neo4jTypes.AnyType typeof(String typeName) {
        typeName = typeName.replaceAll("\\?", "");
        typeName = typeName.toUpperCase();
        if (typeName.startsWith("LIST OF ")) return NTList(typeof(typeName.substring(8)));
        if (typeName.startsWith("LIST ")) return NTList(typeof(typeName.substring(5)));
        if (typeName.startsWith("LIST<") && typeName.endsWith(">")) {
            AnyType typeof = typeof(typeName.substring(5, typeName.length() - 1));
            return NTList(typeof);
        }
        return getBaseType(typeName);
    }

    public static AnyType getBaseType(String typeName) {
        switch (typeName) {
            case "ANY":
                return NTAny;
            case "MAP":
            case MAP_RESULT_TYPE:
                return NTMap;
            case "NODE":
                return NTNode;
            case "REL":
            case "RELATIONSHIP":
            case "EDGE":
                return NTRelationship;
            case "PATH":
                return NTPath;
            case "NUMBER":
                return NTNumber;
            case "LONG":
            case "INT":
            case "INTEGER":
                return NTInteger;
            case "FLOAT":
            case "DOUBLE":
                return NTFloat;
            case "BOOL":
            case "BOOLEAN":
                return NTBoolean;
            case "DATE":
                return NTDate;
            case "TIME":
            case "ZONED TIME":
                return NTTime;
            case "LOCALTIME":
            case "LOCAL TIME":
                return NTLocalTime;
            case "DATETIME":
            case "ZONED DATETIME":
                return NTDateTime;
            case "LOCALDATETIME":
            case "LOCAL DATETIME":
                return NTLocalDateTime;
            case "DURATION":
                return NTDuration;
            case "POINT":
                return NTPoint;
            case "GEO":
            case "GEOMETRY":
                return NTGeometry;
            default:
                return NTString;
        }
    }
}
