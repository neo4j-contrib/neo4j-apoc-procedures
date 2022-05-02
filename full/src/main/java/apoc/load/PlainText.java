package apoc.load;

import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;

import static apoc.load.LoadHtml.withError;

public class PlainText implements HtmlResultInterface {

    @Override
    public String getResult(Document document, String selector, Map<String, Object> config, List<String> errorList, Log log) {
        LoadHtml.FailSilently failConfig = LoadHtml.FailSilently.valueOf((String) config.getOrDefault("failSilently", "FALSE"));
        StringBuilder plainText = new StringBuilder();
        Elements elements = document.select(selector);
        for (Element element : elements) {
            final String result = getResult(config, errorList, log, failConfig, element);
            plainText.append(result);
        }
        return plainText.toString();
    }

    private String getResult(Map<String, Object> config, List<String> errorList, Log log, LoadHtml.FailSilently failConfig, Element element) {
        return withError(element, errorList, failConfig, log, () -> getPlainText(element, config));
    }

    public String getPlainText(Element element, Map<String, Object> config) {
        FormattingVisitor formatter = new FormattingVisitor(config);
        NodeTraversor.traverse(formatter, element);
        return formatter.toString();
    }

    private static class FormattingVisitor implements NodeVisitor {
        private int width;
        private final int textSize;
        private final StringBuilder builder;

        private FormattingVisitor(Map<String, Object> config) {
            this.width = 0;
            this.builder = new StringBuilder();
            this.textSize = Util.toInteger(config.getOrDefault("textSize", 80));
        }

        public void head(Node node, int depth) {
            String name = node.nodeName();
            if (node instanceof TextNode) {
                this.append(((TextNode)node).text());
            } else if (name.equals("li")) {
                this.append("\n - ");
            } else if (name.equals("dt")) {
                builder.append("  ");
            } else {
                final CharSequence[] charSequences = {"p", "h1", "h2", "h3", "h4", "h5", "tr"};
                if (StringUtils.containsAny(name, charSequences)) {
                    this.append("\n");
                }
            }
        }

        public void tail(Node node, int depth) {
            final CharSequence[] charSequences = {"br", "dd", "dt", "p", "h1", "h2", "h3", "h4", "h5"};
            if (StringUtils.containsAny(node.nodeName(), charSequences)) {
                this.append("\n");
            }
        }

        private void append(String text) {
            if (text.startsWith("\n")) {
                this.builder.append(text);
                this.width = 0;
                return;
            }

            if (!StringUtils.isBlank(text)) {
                for (String word: text.split("\\s+")) {
                    if (word.length() + this.width > textSize) {
                        this.builder.append("\n");//.append(word);
                        this.width = word.length();
                    } else {
                        this.width += word.length();
                    }
                    this.builder.append(word).append(" ");
                }
            }
        }

        public String toString() {
            return this.builder.toString();
        }
    }
}
