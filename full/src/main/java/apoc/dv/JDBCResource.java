package apoc.dv;

import apoc.load.Jdbc;
import apoc.result.NodeResult;
import apoc.result.VirtualNode;
import apoc.util.Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class JDBCResource extends VirtualizedResource implements Serializable {
    public JDBCResource(Map<String, Object> vrconfig) {
        super(vrconfig);
    }

    @Override
    protected String getVarReplacement(String paramName) {
        return "?";
    }


//    @Override
//    public String getQueryLinkedTo(String relName) {
//        return "CALL apoc.load.jdbc(\"" + url + "\",\"" + dbquery + "\") yield row " +
//                " with apoc.create.vNode([" + labelsAsCommaSeparatedString() + "], row) as node " +
//                " return node, apoc.create.vRelationship(node,\"" + relName + "\", {}, $the_node) as rel ";
//    }

    @Override
    public Stream<NodeResult> doQuery(Map<String, Object> params) {
        Jdbc jdbc = new Jdbc();
        //not using the config (last param in hte jdbc procedure)
        return jdbc.jdbc(url, dbquery,getParamsAsList(params),new HashMap<String, Object>())
                .map(x -> new NodeResult(new VirtualNode(Util.labels(labels), x.row)));
    }

    private List<Object> getParamsAsList(Map<String, Object> params) {
        ArrayList<Object> paramList = new ArrayList<>();
        for (String x:filterParams) {
            if (params.containsKey(x)) {
                paramList.add(params.get(x));
            } else {
                throw new MissingParameter("Virtualized Resource requires the following params: " + filterParams);
            }
        }
        return paramList;
    }

    @Override
    public Map<String, Object> getExtraQueryParams() {
        return new HashMap<>();
    }

    @Override
    public String getType() {
        return "JDBC";
    }

}
