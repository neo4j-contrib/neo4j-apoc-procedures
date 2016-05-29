package apoc.create;

import apoc.Description;
import apoc.get.Get;
import apoc.result.*;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class Create {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @PerformsWrites
    @Description("apoc.create.node(['Label'], {key:value,...}) - create node with dynamic labels")
    public Stream<NodeResult> node(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        return Stream.of(new NodeResult(setProperties(db.createNode(Util.labels(labelNames)),props)));
    }


    @Procedure
    @PerformsWrites
    @Description("apoc.create.addLabels( [node,id,ids,nodes], ['Label',...]) - adds the given labels to the node or nodes")
    public Stream<NodeResult> addLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get((GraphDatabaseAPI) db).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.addLabel(label);
            }
            return r;
        });
    }
    @Procedure
    @PerformsWrites
    @Description("apoc.create.setLabels( [node,id,ids,nodes], ['Label',...]) - sets the given labels, non matching labels are removed on the node or nodes")
    public Stream<NodeResult> setLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get((GraphDatabaseAPI) db).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : node.getLabels()) {
                if (labelNames.contains(label.name())) continue;
                node.removeLabel(label);
            }
            for (Label label : labels) {
                if (node.hasLabel(label)) continue;
                node.addLabel(label);
            }
            return r;
        });
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.create.removeLabels( [node,id,ids,nodes], ['Label',...]) - removes the given labels from the node or nodes")
    public Stream<NodeResult> removeLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get((GraphDatabaseAPI) db).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.removeLabel(label);
            }
            return r;
        });
    }
    @Procedure
    @PerformsWrites
    @Description("apoc.create.nodes(['Label'], [{key:value,...}]) create multiple nodes with dynamic labels")
    public Stream<NodeResult> nodes(@Name("label") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = Util.labels(labelNames);
        return props.stream().map(p -> new NodeResult(setProperties(db.createNode(labels), p)));
    }

    @Procedure
    @PerformsWrites
    @Description("apoc.create.relationship(person1,'KNOWS',{key:value,...}, person2) create relationship with dynamic rel-type")
    public Stream<RelationshipResult> relationship(@Name("from") Node from,
                                                   @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                   @Name("to") Node to) {
        return Stream.of(new RelationshipResult(setProperties(from.createRelationshipTo(to,RelationshipType.withName(relType)),props)));
    }

    @Procedure
    @Description("apoc.create.vNode(['Label'], {key:value,...}) returns a virtual node")
    public Stream<NodeResult> vNode(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        Label[] labels = Util.labels(labelNames);
        return Stream.of(new NodeResult(new VirtualNode(labels, props, db)));
    }

    @Procedure
    @Description("apoc.create.vNodes(['Label'], [{key:value,...}]) returns virtual nodes")
    public Stream<NodeResult> vNodes(@Name("label") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = Util.labels(labelNames);
        return props.stream().map(p -> new NodeResult(new VirtualNode(labels, p, db)));
    }

    @Procedure
    @Description("apoc.create.vRelationship(nodeFrom,'KNOWS',{key:value,...}, nodeTo) returns a virtual relationship")
    public Stream<RelationshipResult> vRelationship(@Name("from") Node from, @Name("relType") String relType, @Name("props") Map<String, Object> props, @Name("to") Node to) {
        RelationshipType type = RelationshipType.withName(relType);
        return Stream.of(new RelationshipResult(new VirtualRelationship(from,to,type).withProperties(props)));
    }

    @Procedure
    @Description("apoc.create.vPattern({_labels:['LabelA'],key:value},'KNOWS',{key:value,...}, {_labels:['LabelB'],key:value}) returns a virtual pattern")
    public Stream<VirtualPathResult> vPattern(@Name("from") Map<String,Object> n,
                                              @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                              @Name("to") Map<String,Object> m) {
        n = new LinkedHashMap<>(n); m=new LinkedHashMap<>(m);
        RelationshipType type = RelationshipType.withName(relType);
        VirtualNode from = new VirtualNode(Util.labels(n.remove("_labels")), n, db);
        VirtualNode to = new VirtualNode(Util.labels(m.remove("_labels")), m, db);
        Relationship rel = new VirtualRelationship(from, to, RelationshipType.withName(relType)).withProperties(props);
        return Stream.of(new VirtualPathResult(from, rel, to));
    }

    @Procedure
    @Description("apoc.create.vPatternFull(['LabelA'],{key:value},'KNOWS',{key:value,...},['LabelB'],{key:value}) returns a virtual pattern")
    public Stream<VirtualPathResult> vPatternFull(@Name("labelsN") List<String> labelsN, @Name("n") Map<String,Object> n,
                                                  @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                  @Name("labelsM") List<String> labelsM, @Name("m") Map<String,Object> m) {
        RelationshipType type = RelationshipType.withName(relType);
        VirtualNode from = new VirtualNode(Util.labels(labelsN), n, db);
        VirtualNode to = new VirtualNode(Util.labels(labelsM), m, db);
        Relationship rel = new VirtualRelationship(from, to, type).withProperties(props);
        return Stream.of(new VirtualPathResult(from,rel,to));
    }

    private <T extends PropertyContainer> T setProperties(T pc, Map<String, Object> p) {
        if (p == null) return pc;
        for (Map.Entry<String, Object> entry : p.entrySet()) pc.setProperty(entry.getKey(), entry.getValue());
        return pc;
    }

    @Procedure
    @Description("apoc.create.uuid yield uuid - creates an UUID")
    public Stream<UUIDResult> uuid() {
        return Stream.of(new UUIDResult(0));
    }

    @Procedure
    @Description("apoc.create.uuids(count) yield uuid - creates 'count' UUIDs ")
    public Stream<UUIDResult> uuids(@Name("count") long count) {
        return LongStream.range(0,count).mapToObj(UUIDResult::new);
    }

    public static class UUIDResult {
        public final long row;
        public final String uuid;

        public UUIDResult(long row) {
            this.row = row;
            this.uuid = UUID.randomUUID().toString();
                    // TODO Long.toHexString(uuid.getMostSignificantBits())+Long.toHexString(uuid.getLeastSignificantBits());
        }
    }

}
