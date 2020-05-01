package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import apoc.util.MapUtil;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

public class LabelBuilderTest {

    @Test
    public void firstLetterOfWordCapitalised() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mappings", MapUtil.map("$", "keyPhrase{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("Keyphrase")));
    }

    @Test
    public void firstLetterOfAllCapsWordCapitalised() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mappings", MapUtil.map("$", "COMMERCIAL_ITEM{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("CommercialItem")));
    }

    @Test
    public void removeUnderscores() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mappings", MapUtil.map("$", "Key_Phrase{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("KeyPhrase")));
    }

    @Test
    public void firstLetterOfWordsCapitalised() {
        Map<String, Object> conf = new HashMap< >();
        conf.put("mappings", MapUtil.map("$", "KeyPhrase{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("Keyphrase")));
    }
}