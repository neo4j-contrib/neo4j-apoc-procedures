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
package apoc.label;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class Label {

    @Context public GraphDatabaseService db;

    @UserFunction("apoc.label.exists")
    @Description("apoc.label.exists(element, label) - returns true or false related to label existance")
    public boolean exists(@Name("node") Object element, @Name("label") String label) {

        return element instanceof Node ? ((Node) element).hasLabel(org.neo4j.graphdb.Label.label(label)) :
                element instanceof Relationship ? ((Relationship) element).isType(RelationshipType.withName(label)) : false;

    }
}