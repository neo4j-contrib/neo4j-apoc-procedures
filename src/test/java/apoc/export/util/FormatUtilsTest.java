package apoc.export.util;

import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 19.12.16
 */
public class FormatUtilsTest {
    @Test
    public void formatString() throws Exception {
        assertEquals("\"\\n\"",FormatUtils.formatString("\n"));
        assertEquals("\"\\t\"",FormatUtils.formatString("\t"));
        assertEquals("\"\\\"\"",FormatUtils.formatString("\""));
        assertEquals("\"\\\\\"",FormatUtils.formatString("\\"));
        assertEquals("\"\\n\"",FormatUtils.formatString('\n'));
        assertEquals("\"\\t\"",FormatUtils.formatString('\t'));
        assertEquals("\"\\\"\"",FormatUtils.formatString('"'));
        assertEquals("\"\\\\\"",FormatUtils.formatString('\\'));
    }

    @Test
    public void joinLabels() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        final String delimiter = ":";
        try (Transaction t = db.beginTx()) {
            Node node = db.createNode();
            assertEquals("", FormatUtils.joinLabels(node, delimiter));

            node.addLabel(Label.label("label_a"));
            node.addLabel(Label.label("label_c"));
            node.addLabel(Label.label("label_b"));
            assertEquals("label_a:label_b:label_c", FormatUtils.joinLabels(node, delimiter));
        }
    }

}
