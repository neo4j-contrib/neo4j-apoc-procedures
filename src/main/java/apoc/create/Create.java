package apoc.create;

import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class Create {

    private static final Label[] NO_LABELS = new Label[0];

    @Context
    public GraphDatabaseService db;

    @Procedure
    @PerformsWrites
    public Stream<NodeResult> node(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        return Stream.of(new NodeResult(setProperties(db.createNode(labels(labelNames)),props)));
    }

    @Procedure
    @PerformsWrites
    public Stream<NodeResult> nodes(@Name("label") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = labels(labelNames);
        return props.stream().map(p -> new NodeResult(setProperties(db.createNode(labels), p)));
    }

    @Procedure
    @PerformsWrites
    public Stream<RelationshipResult> relationship(@Name("from") Node from,
                                                   @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                   @Name("to") Node to) {
        return Stream.of(new RelationshipResult(setProperties(from.createRelationshipTo(to,RelationshipType.withName(relType)),props)));
    }

    @Procedure
    public Stream<NodeResult> vNode(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        Label[] labels = labels(labelNames);
        return Stream.of(new NodeResult(new VirtualNode(labels, props, db)));
    }

    @Procedure
    public Stream<NodeResult> vNodes(@Name("label") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = labels(labelNames);
        return props.stream().map(p -> new NodeResult(new VirtualNode(labels, p, db)));
    }

    @Procedure
    public Stream<RelationshipResult> vRelationship(@Name("from") Node from, @Name("relType") String relType, @Name("props") Map<String, Object> props, @Name("to") Node to) {
        RelationshipType type = RelationshipType.withName(relType);
        return Stream.of(new RelationshipResult(new VirtualRelationship(from,to,type).withProperties(props)));
    }

    @Procedure
    public Stream<PathResult> vPattern(@Name("from") Map<String,Object> n,
                                       @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                       @Name("to") Map<String,Object> m) {
        n = new LinkedHashMap<>(n); m=new LinkedHashMap<>(m);
        RelationshipType type = RelationshipType.withName(relType);
        VirtualNode from = new VirtualNode(labels(n.remove("_labels")), n, db);
        VirtualNode to = new VirtualNode(labels(m.remove("_labels")), m, db);
        Relationship rel = new VirtualRelationship(from, to, RelationshipType.withName(relType)).withProperties(props);
        return Stream.of(new PathResult(from, rel, to));
    }

    @Procedure
    public Stream<PathResult> vPatternFull(@Name("labelsN") List<String> labelsN, @Name("n") Map<String,Object> n,
                                                   @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                   @Name("labelsM") List<String> labelsM, @Name("m") Map<String,Object> m) {
        RelationshipType type = RelationshipType.withName(relType);
        VirtualNode from = new VirtualNode(labels(labelsN), n, db);
        VirtualNode to = new VirtualNode(labels(labelsM), m, db);
        Relationship rel = new VirtualRelationship(from, to, type).withProperties(props);
        return Stream.of(new PathResult(from,rel,to));
    }

    private <T extends PropertyContainer> T setProperties(T pc, Map<String, Object> p) {
        if (p == null) return pc;
        for (Map.Entry<String, Object> entry : p.entrySet()) pc.setProperty(entry.getKey(), entry.getValue());
        return pc;
    }

    private Label[] labels(Object labelNames) {
        if (labelNames==null) return NO_LABELS;
        if (labelNames instanceof List) {
            List names = (List) labelNames;
            Label[] labels = new Label[names.size()];
            int i = 0;
            for (Object l : names) {
                if (l==null) continue;
                labels[i++] = Label.label(l.toString());
            }
            if (i <= labels.length) return Arrays.copyOf(labels,i);
            return labels;
        }
        return new Label[]{Label.label(labelNames.toString())};
    }
    private RelationshipType type(Object type) {
        if (type == null) throw new RuntimeException("No relationship-type provided");
        return RelationshipType.withName(type.toString());
    }

}
