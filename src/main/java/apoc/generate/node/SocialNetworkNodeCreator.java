package apoc.generate.node;

import com.github.javafaker.Faker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * A {@link NodeCreator} that assigns every {@link Node} a "Person" {@link Label}, and a randomly generated English
 * name under the property key "name".
 */
public class SocialNetworkNodeCreator implements NodeCreator {

    private static final Label PERSON_LABEL = Label.label("Person");
    private static final String NAME = "name";

    private final Faker faker = new Faker();

    /**
     * {@inheritDoc}
     */
    @Override
    public Node createNode(GraphDatabaseService database) {
        Node node = database.createNode(PERSON_LABEL);

        node.setProperty(NAME, faker.name().fullName());

        return node;
    }
}
