package apoc.dv;

import apoc.load.CSVResult;
import apoc.load.LoadCsv;
import apoc.result.NodeResult;
import apoc.result.VirtualNode;
import apoc.util.Util;
import org.neo4j.procedure.Name;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class CSVResource extends VirtualizedResource implements Serializable {

    boolean headers;
    Map<String, Object> loadCSVparams;

    public CSVResource(Map<String, Object> vrconfig) {
        super(vrconfig);

        if(vrconfig.containsKey("header")&&vrconfig.get("header").equals(true)){
            headers = true;
        } else{
            headers = false;
        }

        loadCSVparams = vrconfig;

    }

//    @Override
//    public String getQuery() {
//        return "CALL apoc.load.csv(\"" + url + "\", $extraVRQueryParams) yield " + (headers?"map":"list") +
//                " as row where " + dbquery + " " +
//                " return apoc.create.vNode([" + labelsAsCommaSeparatedString() + "], row) as node , null as rel ";
//    }

    @Override
    public Map<String, Object> getExtraQueryParams() {
        return loadCSVparams;
    }

    @Override
    public String getType() {
        return "CSV";
    }

//    @Override
//    public String getQueryLinkedTo(String relName) {
//        return "CALL apoc.load.csv(\"" + url + "\", $extraVRQueryParams) yield " + (headers ? "map" : "list") +
//                " as row where " + dbquery + " " +
//                " with apoc.create.vNode([" + labelsAsCommaSeparatedString() + "], row) as node " +
//                " return node, apoc.create.vRelationship(node,\"" + relName + "\", {}, $the_node) as rel ";
//    }

    @Override
    public Stream<NodeResult> doQuery(Map<String, Object> params) {
        LoadCsv lcsv = new LoadCsv();
        return lcsv.csv(url, params).filter(x -> applyFilter(x, params))
                .map(x -> new NodeResult(new VirtualNode(Util.labels(labels), x.map)));
    }

    private boolean applyFilter(CSVResult csvr, Map<String, Object> params) {

        boolean result = true;
        Iterator<String> iterator = filterParams.iterator();
        while (result && iterator.hasNext()) {
            String x = iterator.next();
            if (params.containsKey(x)) {
                result &= csvr.map.containsKey(x) && csvr.map.get(x).equals(params.get(x));

            } else {
                throw new MissingParameter("Virtualized Resource requires values for the following params: " + filterParams);
            }
        }
        return result;
    }

    @Override
    protected String getVarReplacement(String paramName) {
        return "$" + paramName ;
    }
}
