package apoc.load;

import org.jsoup.nodes.Document;
import org.neo4j.logging.Log;

import java.util.List;

public interface HtmlResultInterface {

    enum Type {
        DEFAULT(new SelectElement()),
        PLAIN_TEXT(new PlainText());

        private final HtmlResultInterface resultInterface;
        Type(HtmlResultInterface resultInterface) {
            this.resultInterface = resultInterface;
        }

        public HtmlResultInterface get() {
            return resultInterface;
        }
    }

    Object getResult(Document document, String selector, LoadHtmlConfig config, List<String> errorList, Log log);
}