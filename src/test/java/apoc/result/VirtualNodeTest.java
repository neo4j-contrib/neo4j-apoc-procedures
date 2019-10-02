package apoc.result;

import apoc.util.Util;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public class VirtualNodeTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @Test
    public void shouldCreateVirtualNode() {
        Map<String, Object> props = Util.map("key", "value");
        Label[] labels = {Label.label("Test")};
        VirtualNode vn = new VirtualNode(labels, props);
        assertTrue("the id should be < 0", vn.getId() < 0);
        assertEquals(props, vn.getAllProperties());
        Iterator<Label> it = vn.getLabels().iterator();
        assertEquals(labels[0], it.next());
        assertFalse("should not have next label", it.hasNext());
    }

    @Test
    public void shouldCreateVirtualNodeWithRelationshipsTo() {
        Map<String, Object> startProps = Util.map("key", "value");
        Label[] startLabels = {Label.label("Test")};
        VirtualNode start = new VirtualNode(startLabels, startProps);
        assertTrue("the node id should be < 0", start.getId() < 0);
        assertEquals(startProps, start.getAllProperties());
        Iterator<Label> startLabelIt = start.getLabels().iterator();
        assertEquals(startLabels[0], startLabelIt.next());
        assertFalse("start should not have next label", startLabelIt.hasNext());

        Map<String, Object> endProps = Util.map("key", "value");
        Label[] endLabels = {Label.label("Test")};
        VirtualNode end = new VirtualNode(endLabels, endProps);
        assertTrue("the node id should be < 0", end.getId() < 0);
        assertEquals(endProps, end.getAllProperties());
        Iterator<Label> endLabelIt = end.getLabels().iterator();
        assertEquals(endLabels[0], endLabelIt.next());
        assertFalse("end should not have next label", endLabelIt.hasNext());

        RelationshipType relationshipType = RelationshipType.withName("TYPE");
        Relationship rel = start.createRelationshipTo(end, relationshipType);
        assertTrue("the rel id should be < 0", rel.getId() < 0);

        assertEquals(1, Iterables.count(start.getRelationships()));
        assertEquals(0, Iterables.count(start.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING, relationshipType)));
        assertEquals(end, start.getRelationships().iterator().next().getOtherNode(start));

        assertEquals(1, Iterables.count(end.getRelationships()));
        assertEquals(0, Iterables.count(end.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING, relationshipType)));
        assertEquals(start, end.getRelationships().iterator().next().getOtherNode(end));
    }

    @Test
    public void shouldCreateVirtualNodeWithRelationshipsFrom() {
        Map<String, Object> startProps = Util.map("key", "value");
        Label[] startLabels = {Label.label("Test")};
        VirtualNode start = new VirtualNode(startLabels, startProps);
        assertTrue("the node id should be < 0", start.getId() < 0);
        assertEquals(startProps, start.getAllProperties());
        Iterator<Label> startLabelIt = start.getLabels().iterator();
        assertEquals(startLabels[0], startLabelIt.next());
        assertFalse("start should not have next label", startLabelIt.hasNext());

        Map<String, Object> endProps = Util.map("key", "value");
        Label[] endLabels = {Label.label("Test")};
        VirtualNode end = new VirtualNode(endLabels, endProps);
        assertTrue("the node id should be < 0", end.getId() < 0);
        assertEquals(endProps, end.getAllProperties());
        Iterator<Label> endLabelIt = end.getLabels().iterator();
        assertEquals(endLabels[0], endLabelIt.next());
        assertFalse("end should not have next label", endLabelIt.hasNext());

        RelationshipType relationshipType = RelationshipType.withName("TYPE");
        Relationship rel = end.createRelationshipFrom(start, relationshipType);
        assertTrue("the rel id should be < 0", rel.getId() < 0);

        assertEquals(1, Iterables.count(start.getRelationships()));
        assertEquals(0, Iterables.count(start.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING, relationshipType)));
        assertEquals(end, start.getRelationships().iterator().next().getOtherNode(start));

        assertEquals(1, Iterables.count(end.getRelationships()));
        assertEquals(0, Iterables.count(end.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING, relationshipType)));
        assertEquals(start, end.getRelationships().iterator().next().getOtherNode(end));
    }

}
