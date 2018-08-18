package apoc.sequence;

import apoc.result.NodeResult;
import apoc.result.ObjectResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Created by Valentino Maiorca on 6/29/17.
 */
public class Sequence {

    /**
     * Label for all sequence-node.
     */
    public static final Label SEQUENCE_LABEL = Label.label("Apoc_Sequence");

    public static final String PROVIDER_PROPERTY = "provider";

    public static final String NAME_PROPERTY = "name";

    // Simple provider. Increments by 1 the value.
    public static final SequenceProvider<Long> LONG_SP = new SequenceProvider<Long>() {

        @Override
        public Long start() {
            return 0L;
        }

        @Override
        public Long next(Long current) {
            return current + 1;
        }

        @Override
        public LinkedList<Long> next(Long current, long length) {
            return LongStream.range(current + 1, current + length + 1).boxed()
                    .collect(Collectors.toCollection(LinkedList::new));
        }

        @Override
        public String getName() {
            return "LongCounter";
        }

    };

    // Map <providerName, sequenceProvider> holding instances of provider for fast access.

    private static final Map<String, SequenceProvider> SEQUENCE_PROVIDER_MAP = new ConcurrentHashMap<>();

    static {
        SEQUENCE_PROVIDER_MAP.put(LONG_SP.getName(), LONG_SP);
    }

    /**
     * Registers a new provider. It will be available for sequence creation.
     *
     * @param provider
     * @return True if the provider wasn't already registered, false otherwise.
     */
    public static boolean registerProvider(SequenceProvider provider) {
        return SEQUENCE_PROVIDER_MAP.put(provider.getName(), provider) == null;
    }

    @Context
    public GraphDatabaseService db;

    @UserFunction(value = "apoc.sequence.get")
    @Description("apoc.sequence.get('sequenceName') - returns the node representing that sequence, null if not existing.")
    public Node get(@Name("sequenceName") String sequenceName) {
        return db.findNode(SEQUENCE_LABEL, NAME_PROPERTY, sequenceName);
    }


    // TODO: 29/06/17 Can't be completely thread safe. Can't acquire a lock on a node if the node doesn't exist.
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.sequence.create('sequenceName', 'providerName') - create sequence with given provider")
    public Stream<NodeResult> create(@Name("sequenceName") String sequenceName,
                                     @Name(value = "providerName", defaultValue = "LongCounter") String providerName) {
        // MultipleFoundException if more than one.
        Node sequenceNode = db.findNode(SEQUENCE_LABEL, NAME_PROPERTY, sequenceName);
        if (sequenceNode != null)
            throw new RuntimeException(String.format("There's already a sequence with name <%s> !", sequenceName));

        SequenceProvider provider = SEQUENCE_PROVIDER_MAP.get(providerName);
        if (provider == null)
            throw new RuntimeException("No provider registered with name: " + providerName);

        Node node = db.createNode(SEQUENCE_LABEL);
        node.setProperty(NAME_PROPERTY, sequenceName);
        node.setProperty(PROVIDER_PROPERTY, providerName);
        node.setProperty(provider.propertyName(), provider.start());

        return Stream.of(new NodeResult(node));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.sequence.next('sequenceName') - get&increment sequence value using related provider and length as increments number")
    @SuppressWarnings("unchecked")
    public Stream<ObjectResult> next(@Name("sequenceName") String sequenceName, @Name(value = "length", defaultValue = "1") long length) {
        LinkedList<Object> result = new LinkedList<>();

        try (Transaction tx = db.beginTx()) {
            Node sequenceNode = db.findNode(SEQUENCE_LABEL, NAME_PROPERTY, sequenceName);

            if (sequenceNode == null)
                throw new RuntimeException(String.format("You must create %s (with apoc.sequence.create) before using it!",
                        sequenceName));

            String providerName = (String) sequenceNode.getProperty(PROVIDER_PROPERTY);
            SequenceProvider provider = SEQUENCE_PROVIDER_MAP.get(providerName);

            if (provider == null)
                throw new RuntimeException(String.format("No provider associated with <%s>", sequenceName));

            tx.acquireWriteLock(sequenceNode);

            Object current = sequenceNode.getProperty(provider.propertyName());

            if (length < 1)
                throw new RuntimeException("Length can't be lower than 1!");
            if (length > 1) {
                result = provider.next(current, length - 1);
                // next method doesn't include the left bound, so we add it later.
                result.addFirst(current);

                current = result.getLast();
            } else
                result.add(current);


            sequenceNode.setProperty(provider.propertyName(), provider.next(current));

            tx.success();
        }

        return result.stream().map(ObjectResult::new);
    }

}
