package apoc.algo.algorithms;

public interface AlgorithmInterface
{
    double getResult( long node );

    long numberOfNodes();

    String getPropertyName();

    long getMappedNode(int algoId);

    class Statistics {
        public long nodes, relationships, readNodeMillis, readRelationshipMillis,computeMillis,writeMillis;
        public boolean write;
        public String property;

        public Statistics(long nodes, long relationships, long iterations, long readNodeMillis, long readRelationshipMillis, long computeMillis, long writeMillis, boolean write, String property) {
            this.nodes = nodes;
            this.relationships = relationships;
            this.readNodeMillis = readNodeMillis;
            this.readRelationshipMillis = readRelationshipMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.write = write;
            this.property = property;
        }

        public Statistics() {
        }
    }
}



