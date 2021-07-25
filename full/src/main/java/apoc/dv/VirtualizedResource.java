package apoc.dv;

import apoc.dv.result.NodeWithRelResult;
import apoc.result.NodeResult;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public abstract class VirtualizedResource implements Serializable {
    protected String url;
    protected String desc;
    protected List<String> filterParams = new ArrayList<>();
    protected List<String> labels;
    protected String dbQuery;
    protected String rawDbQuery;

    public VirtualizedResource(Map<String, Object> vrconfig) throws BadVRConfig {
        url = getRequiredConfigParam("url", vrconfig);
        dbQuery = rawDbQuery = getRequiredConfigParam("query", vrconfig);

        String patternString = "\\{vrp:([a-zA-Z]\\w*)\\}";
        Pattern pattern = Pattern.compile(patternString);

        Matcher matcher = pattern.matcher(dbQuery);
        while(matcher.find()){
            String paramName = matcher.group(1);
            dbQuery = dbQuery.replace("{vrp:" + paramName + "}", getVarReplacement(paramName));
            filterParams.add(paramName);
        }

        if (filterParams.size()==0){
            throw new BadVRConfig("A virtualized resource must have at least one filter parameter.");
        }

        desc = getOptionalConfigParam("desc", vrconfig);
        labels = getRequiredListConfigParam("labels",vrconfig);

    }

    protected abstract String getVarReplacement(String paramName);

    protected String getRequiredConfigParam(String paramName, Map<String, Object> vrconfig) {
        if(vrconfig.containsKey(paramName)){
            return (String)vrconfig.get(paramName);
        } else{
            throw new RuntimeException("VR config missing " + paramName + " param");
        }
    }

    protected List<String> getRequiredListConfigParam(String paramName, Map<String, Object> vrconfig) {
        if(vrconfig.containsKey(paramName)){
            if (vrconfig.get(paramName) instanceof List) {
                return (List<String>) vrconfig.get(paramName);
            }
            else{
                throw new RuntimeException("Param " + paramName + " in VR config should be a list");
            }
        } else{
            throw new RuntimeException("VR config missing " + paramName + " param");
        }
    }

    protected String getOptionalConfigParam(String paramName, Map<String, Object> vrconfig) {
        if(vrconfig.containsKey(paramName)){
            return (String)vrconfig.get(paramName);
        } else {
            return null;
        }
    }

    public abstract Map<String, Object> getExtraQueryParams();

    public abstract String getType();

    public Stream<NodeResult> doQuery(Map<String, Object> params){
        return streamVirtualNodes(params).map(NodeResult::new);
    };

    public Stream<NodeWithRelResult> doQueryAndLink(Node node, String relName, Map<String, Object> params){
        return streamVirtualNodes(params).map(x -> {
            VirtualRelationship virtualizedRel = new VirtualRelationship(node, x, RelationshipType.withName(relName));
            return new NodeWithRelResult(x, virtualizedRel);
        });
    };

    protected abstract Stream<Node> streamVirtualNodes(Map<String, Object> params);

    protected class BadVRConfig extends RuntimeException {
        public BadVRConfig(String s) {
            super(s);
        }
    }
    protected class MissingParameter extends RuntimeException {
        public MissingParameter(String s) {
        }
    }
}
