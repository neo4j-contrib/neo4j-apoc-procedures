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
package apoc.graph.document.builder;

import static org.junit.Assert.assertEquals;

import apoc.graph.util.GraphsConfig;
import apoc.result.VirtualGraph;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import java.util.*;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.MapUtil;

public class DocumentToGraphTest {

    @Test
    public void testWithNoRoot() {
        Map<String, Object> map = new HashMap<>();

        Map<String, String> mappings = new HashMap<>();
        mappings.put("$.entities", "Entity{!name,type,@metadata}");

        map.put("mappings", mappings);
        map.put("skipValidation", true);

        GraphsConfig config = new GraphsConfig(map);

        DocumentToGraph documentToGraph = new DocumentToGraph(null, config);

        Map<String, Object> document = new HashMap<>();
        List<Map<String, Object>> entities = new ArrayList<>();
        entities.add(MapUtil.map("name", 1, "type", "Artist"));
        entities.add(MapUtil.map("name", 2, "type", "Engineer"));

        document.put("entities", entities);

        VirtualGraph s = documentToGraph.create(document);
        Set<VirtualNode> nodes = (Set<VirtualNode>) s.graph.get("nodes");
        Iterator<VirtualNode> nodesIterator = nodes.iterator();

        nodesIterator.next(); // skip root node

        VirtualNode entityNode1 = nodesIterator.next();
        assertEquals(entityNode1.getProperty("name"), 1);
        assertEquals(entityNode1.getProperty("type"), "Artist");
        assertEquals(entityNode1.getLabels(), Arrays.asList(Label.label("Artist"), Label.label("Entity")));

        VirtualNode entityNode2 = nodesIterator.next();
        assertEquals(entityNode2.getProperty("name"), 2);
        assertEquals(entityNode2.getProperty("type"), "Engineer");
        assertEquals(entityNode2.getLabels(), Arrays.asList(Label.label("Engineer"), Label.label("Entity")));

        Set<VirtualRelationship> relationships = (Set<VirtualRelationship>) s.graph.get("relationships");
        Iterator<VirtualRelationship> relationshipIterator = relationships.iterator();

        VirtualRelationship rel1 = relationshipIterator.next();
        assertEquals(rel1.getType(), RelationshipType.withName("ENTITIES"));
        assertEquals(rel1.getEndNode().getProperty("name"), 1);

        VirtualRelationship rel2 = relationshipIterator.next();
        assertEquals(rel2.getType(), RelationshipType.withName("ENTITIES"));
        assertEquals(rel2.getEndNode().getProperty("name"), 2);
    }

    @Test
    public void testWithRoot() {
        Map<String, Object> map = new HashMap<>();
        map.put("idField", "uri");

        Map<String, String> mappings = new HashMap<>();
        mappings.put("$.entities", "Entity{!name,type,@metadata}");

        map.put("mappings", mappings);
        map.put("skipValidation", true);

        GraphsConfig config = new GraphsConfig(map);

        DocumentToGraph documentToGraph = new DocumentToGraph(null, config);

        Map<String, Object> document = new HashMap<>();
        List<Map<String, Object>> entities = new ArrayList<>();
        entities.add(MapUtil.map("name", 1, "type", "Artist"));

        document.put("uri", "1234");
        document.put("type", "Article");
        document.put("entities", entities);

        VirtualGraph s = documentToGraph.create(document);
        Set<VirtualNode> nodes = (Set<VirtualNode>) s.graph.get("nodes");
        Iterator<VirtualNode> nodesIterator = nodes.iterator();

        VirtualNode rootNode = nodesIterator.next();
        assertEquals(rootNode.getProperty("type"), "Article");
        assertEquals(rootNode.getProperty("uri"), "1234");
        assertEquals(rootNode.getLabels(), Arrays.asList(Label.label("Article")));

        VirtualNode entityNode = nodesIterator.next();
        assertEquals(entityNode.getProperty("name"), 1);
        assertEquals(entityNode.getProperty("type"), "Artist");
        assertEquals(entityNode.getLabels(), Arrays.asList(Label.label("Artist"), Label.label("Entity")));

        Set<VirtualRelationship> relationships = (Set<VirtualRelationship>) s.graph.get("relationships");
        Iterator<VirtualRelationship> relationshipIterator = relationships.iterator();

        VirtualRelationship relationship = relationshipIterator.next();
        assertEquals(relationship.getType(), RelationshipType.withName("ENTITIES"));
        assertEquals(relationship.getStartNode().getProperty("uri"), "1234");
        assertEquals(relationship.getEndNode().getProperty("name"), 1);
    }
}
