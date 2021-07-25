package apoc.dv;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class VRCatalog {

    private final Map<String, VirtualizedResource> vrs;

    public VRCatalog(Transaction tx) throws IOException, ClassNotFoundException {
        Result vrcats = tx.execute("MERGE (vrcat:_VRCat { _id: 1}) RETURN vrcat");
        if (vrcats.hasNext()) {
            Node vrcat = (Node)vrcats.next().get("vrcat");
            this.vrs = (vrcat.hasProperty("_vrs")?
                    (Map<String, VirtualizedResource>) deserialiseObject(
                            (byte[]) vrcat.getProperty("_vrs")): new HashMap<>());

        } else {
            this.vrs = new HashMap<>();
        }

    }

    private byte[] serialiseObject(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream
                = new ObjectOutputStream(baos);
        objectOutputStream.writeObject(o);
        objectOutputStream.flush();
        objectOutputStream.close();
        return baos.toByteArray();
    }

    private Object deserialiseObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream
                = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return objectInputStream.readObject();
    }

    public VirtualizedResource getVR(String vrName) {
        return vrs.get(vrName);
    }

    public void addVR(String vrName, VirtualizedResource vr) {
        this.vrs.put(vrName, vr);
    }

    public void dropVR(String vrName) {
        this.vrs.remove(vrName);
    }

    public void writeToDB(Transaction tx) throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("vrs", serialiseObject(this.vrs));

        tx.execute("MERGE (vrcat:_VRCat { _id: 1}) "
                + "SET vrcat._vrs = $vrs ", params);

    }

    public Stream<VRResult> getVRList(String vrNameFilter) {
        return vrs.entrySet().stream().filter(e -> (vrNameFilter.equals("")||e.getKey().equals(vrNameFilter)))
                .map( e -> new VRResult(e.getKey(), e.getValue().getType(), e.getValue().url,
                        e.getValue().dbquery,e.getValue().desc,
                        e.getValue().filterParams, e.getValue().labels));
    }
}
