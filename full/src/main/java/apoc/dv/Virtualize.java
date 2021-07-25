package apoc.dv;

import apoc.dv.result.NodeWithRelResult;
import apoc.result.NodeResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

public class Virtualize {

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Procedure(name = "apoc.dv.catalog.add", mode = Mode.WRITE)
    @Description("Add a virtualized resource configuration")
    public Stream<VRResult> addVR(
            @Name("vrName") String vrName,
            @Name(value = "config", defaultValue = "{}") Map<String,Object> vrconfig) throws IOException, ClassNotFoundException {

        VRCatalog vrc = new VRCatalog(tx);
        if(vrconfig.containsKey("vrType")){
            String vrType = (String) vrconfig.get("vrType");
            if(vrType.equals("JDBC")){
                vrc.addVR(vrName,new JDBCResource(vrconfig));
            } else if(vrType.equals("CSV")){
                vrc.addVR(vrName, new CSVResource(vrconfig));
            }
            vrc.writeToDB(tx);
        }else{
            throw new RuntimeException("VR config missing vrType parameter");
        }

        return vrc.getVRList("");
    }

    @Procedure(name = "apoc.dv.catalog.drop", mode = Mode.WRITE)
    @Description("Remove a virtualized resource config by name")
    public Stream<VRResult> dropVR(
            @Name("vrName") String vrName) throws IOException, ClassNotFoundException {

        VRCatalog vrc = new VRCatalog(tx);
        vrc.dropVR(vrName);
        vrc.writeToDB(tx);
        return vrc.getVRList("");
    }

    @Procedure(name = "apoc.dv.catalog.list", mode = Mode.READ)
    @Description("List all virtualized resource configs")
    public Stream<VRResult> listVR(
            @Name(value = "vrName", defaultValue = "") String vrNameFilter) throws IOException, ClassNotFoundException {

        VRCatalog vrc = new VRCatalog(tx);
        return vrc.getVRList(vrNameFilter);
    }

    @Procedure(name = "apoc.dv.query", mode = Mode.READ)
    @Description("Query a virtualized resource by name and return virtual nodes")
    public Stream<NodeResult> query(
            @Name("vrName") String vrName,
            @Name(value = "params", defaultValue = "{}") Map<String,Object> params) throws IOException, ClassNotFoundException {

        VRCatalog vrc = new VRCatalog(tx);
        VirtualizedResource vr = vrc.getVR(vrName);
        if(vr!=null) {
            params.put("extraVRQueryParams", vr.getExtraQueryParams());
            return vr.doQuery(params);
        }else {
            throw new RuntimeException("Virtualized resource " + vrName + " is not defined.");
        }
    }

    @Procedure(name = "apoc.dv.queryAndLink", mode = Mode.READ)
    @Description("Query a virtualized resource by name and return virtual nodes linked using virtual rels to the node passed as first param")
    public Stream<NodeWithRelResult> queryAndLink(
            @Name("node") Node node,
            @Name("relName") String relName,
            @Name("vrName") String vrName,
            @Name(value = "params", defaultValue = "{}") Map<String,Object> params
            ) throws IOException, ClassNotFoundException {

        VRCatalog vrc = new VRCatalog(tx);
        VirtualizedResource vr = vrc.getVR(vrName);
        if(vr!=null) {
            params.put("extraVRQueryParams", vr.getExtraQueryParams());
            return vr.doQueryAndLink(node, relName, params);
        }else {
            throw new RuntimeException("Virtualized resource " + vrName + " is not defined.");
        }
    }

}
