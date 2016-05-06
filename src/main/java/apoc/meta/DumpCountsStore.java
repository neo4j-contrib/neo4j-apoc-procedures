package apoc.meta;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.kvstore.*;
import org.neo4j.storageengine.api.Token;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

/**
 * Tool that will dump content of count store content into a simple string representation for further analysis.
 */
public class DumpCountsStore implements CountsVisitor, MetadataVisitor, UnknownKey.Visitor {

    DumpCountsStore(PrintStream out, NeoStores neoStores) {
        this(out,
                allTokensFrom(neoStores.getLabelTokenStore()),
                allTokensFrom(neoStores.getRelationshipTypeTokenStore()),
                allTokensFrom(neoStores.getPropertyKeyTokenStore()));
    }

    private final PrintStream out;
    private final List<Token> labels;
    private final List<RelationshipTypeToken> relationshipTypes;
    private final List<Token> propertyKeys;

    private DumpCountsStore(PrintStream out, List<Token> labels, List<RelationshipTypeToken> relationshipTypes,
                            List<Token> propertyKeys) {
        this.out = out;
        this.labels = labels;
        this.relationshipTypes = relationshipTypes;
        this.propertyKeys = propertyKeys;
    }

    @Override
    public void visitMetadata(File file, Headers headers, int entryCount) {
        out.printf("Counts Store:\t%s%n", file);
        for (HeaderField<?> headerField : headers.fields()) {
            out.printf("%s:\t%s%n", headerField.toString(), headers.get(headerField));
        }
        out.printf("\tentries:\t%d%n", entryCount);
        out.println("Entries:");
    }

    @Override
    public void visitNodeCount(int labelId, long count) {
        out.printf("\tNode[(%s)]:\t%d%n", label(labelId), count);
    }

    @Override
    public void visitRelationshipCount(int startLabelId, int typeId, int endLabelId, long count) {
        out.printf("\tRelationship[(%s)-%s->(%s)]:\t%d%n",
                label(startLabelId), relationshipType(typeId), label(endLabelId),
                count);
    }

    @Override
    public void visitIndexStatistics(int labelId, int propertyKeyId, long updates, long size) {
        out.printf("\tIndexStatistics[(%s {%s})]:\tupdates=%d, size=%d%n",
                label(labelId), propertyKey(propertyKeyId),
                updates, size);
    }

    @Override
    public void visitIndexSample(int labelId, int propertyKeyId, long unique, long size) {
        out.printf("\tIndexSample[(%s {%s})]:\tunique=%d, size=%d%n",
                label(labelId), propertyKey(propertyKeyId),
                unique, size);
    }

    @Override
    public boolean visitUnknownKey(ReadableBuffer key, ReadableBuffer value) {
        out.printf("\t%s:\t%s%n", key, value);
        return true;
    }

    private String label(int id) {
        if (id == ReadOperations.ANY_LABEL) {
            return "";
        }
        return token(new StringBuilder(), labels, ":", "label", id).toString();
    }

    private String propertyKey(int id) {
        return token(new StringBuilder(), propertyKeys, "", "key", id).toString();
    }

    private String relationshipType(int id) {
        if (id == ReadOperations.ANY_RELATIONSHIP_TYPE) {
            return "";
        }
        return token(new StringBuilder().append('['), relationshipTypes, ":", "type", id).append(']').toString();
    }

    private static StringBuilder token(StringBuilder result, List<? extends Token> tokens, String pre, String handle, int id) {
        Token token = null;
        // search backwards for the token
        for (int i = (id < tokens.size()) ? id : tokens.size() - 1; i >= 0; i--) {
            token = tokens.get(i);
            if (token.id() == id) {
                break; // found
            }
            if (token.id() < id) {
                token = null; // not found
                break;
            }
        }
        if (token != null) {
            String name = token.name();
            result.append(pre).append(name)
                    .append(" [").append(handle).append("Id=").append(token.id()).append(']');
        } else {
            result.append(handle).append("Id=").append(id);
        }
        return result;
    }

    private static <TOKEN extends Token> List<TOKEN> allTokensFrom(TokenStore<?, TOKEN> store) {
        try (TokenStore<?, TOKEN> tokens = store) {
            return tokens.getTokens(Integer.MAX_VALUE);
        }
    }
}
