package apoc.dv;

import apoc.dv.result.NodeWithRelResult;
import apoc.load.CSVResult;
import apoc.load.LoadCsv;
import apoc.result.NodeResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Name;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CSVResource extends VirtualizedResource implements Serializable {

    Map<String, Object> loadCSVparams;
    Map<String, String> headersAndParamsMap;

    public CSVResource(Map<String, Object> vrconfig) {
        super(vrconfig);
        loadCSVparams = vrconfig;
        headersAndParamsMap = parseFilterParams(vrconfig);
    }

    private Map<String, String> parseFilterParams(Map<String, Object> vrconfig) {
        String patternString = "^(.+)\\s*=\\s*\\{vrp:([a-zA-Z_]\\w*)\\}$";
        Pattern pattern = Pattern.compile(patternString);

        Map<String, String> map = new HashMap<>();
        String[] ands = rawDbQuery.split("and");
        for (String filter:ands) {
            Matcher matcher = pattern.matcher(filter.trim());
            if(matcher.matches()){
                String columnName = matcher.group(1);
                String paramName = matcher.group(2);
                map.put(columnName.trim(),paramName.trim());
            }
            else{
                throw new BadVRConfig(filter.trim() + " could not be parsed as a valid filter");
            }
        }
        return map;
    }

    @Override
    public Map<String, Object> getExtraQueryParams() {
        return loadCSVparams;
    }

    @Override
    public String getType() {
        return "CSV";
    }

    @Override
    protected Stream<Node> streamVirtualNodes(Map<String, Object> params) {
        LoadCsv lcsv = new LoadCsv();
        return lcsv.csv(url, params).filter(x -> applyFilter(x, params))
                .map(x -> new VirtualNode(Util.labels(labels), x.map));
    }

    private boolean applyFilter(CSVResult csvr, Map<String, Object> params) {

        boolean result = true;
        Iterator<Map.Entry<String, String>> iterator = headersAndParamsMap.entrySet().iterator();

        while (result && iterator.hasNext()) {
            Map.Entry<String, String> filter = iterator.next();
            if (params.containsKey(filter.getValue())) {
                result &= csvr.map.containsKey(filter.getKey()) && csvr.map.get(filter.getKey()).equals(params.get(filter.getValue()));

            } else {
                throw new MissingParameter("Virtual results must contain values for the following headers: " + headersAndParamsMap.entrySet());
            }
        }
        return result;
    }

    @Override
    protected String getVarReplacement(String paramName) {
        return "$" + paramName ;
    }
}
