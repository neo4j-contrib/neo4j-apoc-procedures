package apoc.load;


import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static apoc.load.LoadHtml.getElements;

public class SelectElement implements HtmlResultInterface {

    @Override
    public List<Map<String, Object>> getResult(Document document, String selector, LoadHtmlConfig config, List<String> errorList, Log log, AtomicInteger rows, Transaction tx) {
        final Elements select = document.select(selector);
        return getElements(select, config, errorList, log, rows, tx);
    }
}