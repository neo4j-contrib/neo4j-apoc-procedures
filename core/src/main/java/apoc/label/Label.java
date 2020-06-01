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