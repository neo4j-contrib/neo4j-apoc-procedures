package apoc.export.util;

/**
 * @author mh
 * @since 17.01.14
 */
public class ElementCounter {
    int nodes, relationships, properties;

    @Override
    public String toString() {
        return String.format("nodes = %d rels = %d properties = %d",nodes,relationships,properties);
    }
    public ElementCounter update(long nodes, long relationships, long properties) {
        this.nodes += nodes;
        this.relationships += relationships;
        this.properties += properties;
        return this;
    }

    public int getNodes() {
        return nodes;
    }

    public int getRelationships() {
        return relationships;
    }

    public int getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElementCounter that = (ElementCounter) o;

        return nodes == that.nodes && properties == that.properties && relationships == that.relationships;

    }

    @Override
    public int hashCode() {
        int result = nodes;
        result = 31 * result + relationships;
        result = 31 * result + properties;
        return result;
    }
}
