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
package apoc.diff;

import apoc.Description;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Benjamin Clauss
 * @since 15.06.2018
 */
public class Diff {

    @Context
    public Transaction tx;

    @UserFunction()
    @Description("apoc.diff.nodes([leftNode],[rightNode]) returns a detailed diff of both nodes")
    public Map<String, Object> nodes(@Name("leftNode") Node leftNode, @Name("rightNode") Node rightNode) {
        leftNode = Util.rebind(tx, leftNode);
        rightNode = Util.rebind(tx, rightNode);
        Map<String, Object> allLeftProperties = leftNode.getAllProperties();
        Map<String, Object> allRightProperties = rightNode.getAllProperties();

        Map<String, Object> result = new HashMap<>();
        result.put("leftOnly", getPropertiesOnlyLeft(allLeftProperties, allRightProperties));
        result.put("rightOnly", getPropertiesOnlyLeft(allRightProperties, allLeftProperties));
        result.put("inCommon", getPropertiesInCommon(allLeftProperties, allRightProperties));
        result.put("different", getPropertiesDiffering(allLeftProperties, allRightProperties));

        return result;
    }

    private Map<String, Object> getPropertiesOnlyLeft(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> leftOnly = new HashMap<>();
        leftOnly.putAll(left);
        leftOnly.keySet().removeAll(right.keySet());
        return leftOnly;
    }

    private Map<String, Object> getPropertiesInCommon(Map<String, Object> left, Map<String, Object> right) {
        // we use filter(entry -> Objects.deepEquals...) instead of retainAll because retainAll use `.equals()`, so it doesn't compare arrays 
        return left.entrySet().stream()
                .filter(entry -> Objects.deepEquals(entry.getValue(), right.get(entry.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, Map<String, Object>> getPropertiesDiffering(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Map<String, Object>> different = new HashMap<>();
        Map<String, Object> keyPairs = new HashMap<>();
        keyPairs.putAll(left);
        keyPairs.keySet().retainAll(right.keySet());

        for (Map.Entry<String, Object> entry : keyPairs.entrySet()) {
            if (!Objects.deepEquals(left.get(entry.getKey()), right.get(entry.getKey()))) {
                Map<String, Object> pairs = new HashMap<>();
                pairs.put("left", left.get(entry.getKey()));
                pairs.put("right", right.get(entry.getKey()));
                different.put(entry.getKey(), pairs);
            }
        }
        return different;
    }
}
