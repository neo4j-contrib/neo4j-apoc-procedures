package apoc.index.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

public class DynamicChainAnalyzer extends Analyzer {

    public DynamicChainAnalyzer() {
        // default,
        // TODO: need to obtain information about the GraphDB and IndexManager
    }

    @Override
    protected Analyzer.TokenStreamComponents createComponents(final String fieldName) {
        Tokenizer source = new WhitespaceTokenizer();
        TokenStream filter = new LowerCaseFilter( source );
        return new TokenStreamComponents( source, filter );
    }

}
