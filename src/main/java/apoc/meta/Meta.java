package apoc.meta;

import apoc.Description;
import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;

public class Meta {

    @Context
    public GraphDatabaseService db;
    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction kernelTx;

    public enum Types {
        INTEGER,FLOAT,STRING,BOOLEAN,RELATIONSHIP,NODE,PATH,NULL,UNKNOWN,MAP,LIST;

        public static Types of(Object value) {
            if (value==null) return NULL;
            Class type = value.getClass();
            if (type.isArray()) {
                type = type.getComponentType();
            }
            if (Number.class.isAssignableFrom(type)) {
                return double.class.isAssignableFrom(type) || Double.class.isAssignableFrom(type) ||
                       Float.class.isAssignableFrom(type) || float.class.isAssignableFrom(type) ? FLOAT : INTEGER;
            }
            if (type == Boolean.class || type == boolean.class) return BOOLEAN;
            if (value instanceof String) return STRING;
            if (Map.class.isAssignableFrom(type)) return MAP;
            if (Node.class.isAssignableFrom(type)) return NODE;
            if (Relationship.class.isAssignableFrom(type)) return RELATIONSHIP;
            if (Path.class.isAssignableFrom(type)) return PATH;
            if (Iterable.class.isAssignableFrom(type)) return LIST;
            return UNKNOWN;
        }

        public static Types from(String typeName) {
            typeName = typeName.toUpperCase();
            for (Types type : values()) {
                if (type.name().startsWith(typeName)) return type;
            }
            return STRING;
        }
    }
    public static class MetaResult {
        public String label;
        public String property;
        public long count;
        public boolean unique;
        public boolean index;
        public boolean existence;
        public String type;
        public boolean array;
        public List<Object> sample;
        public long leftCount; // 0,1,
        public long rightCount; // 0,1,many
        public long left; // 0,1,
        public long right; // 0,1,many
        public List<String> other = new ArrayList<>();

        public MetaResult(String label, String name) {
            this.label = label;
            this.property = name;
        }

        public MetaResult inc() {
            count ++;
            return this;
        }
        public MetaResult rel(long out, long in) {
            this.type = Types.RELATIONSHIP.name();
            if (out>1) array = true;
            leftCount += out;
            rightCount += in;
            left = leftCount / count;
            right = rightCount / count;
            return this;
        }

        public MetaResult other(List<String> labels) {
            for (String l : labels) {
                if (!this.other.contains(l)) this.other.add(l);
            }
            return this;
        }

        public MetaResult type(String type) {
            this.type = type;
            return this;
        }

        public MetaResult array(boolean array) {
            this.array = array;
            return this;
        }
    }
    static final int SAMPLE = 100;


    @Procedure
    @Description("apoc.meta.type(value)  - type name of a value (INTEGER,FLOAT,STRING,BOOLEAN,RELATIONSHIP,NODE,PATH,NULL,UNKNOWN,MAP,LIST)")
    public Stream<StringResult> type(@Name("value") Object value) {
        Types type = Types.of(value);
        String typeName = type == Types.UNKNOWN ? value.getClass().getSimpleName() : type.name();

        if (value != null && value.getClass().isArray()) typeName +="[]";
        return Stream.of(new StringResult(typeName));
    }

    @Procedure
    @Description("apoc.meta.isType(value,type) - returns a row if type name matches none if not (INTEGER,FLOAT,STRING,BOOLEAN,RELATIONSHIP,NODE,PATH,NULL,UNKNOWN,MAP,LIST)")
    public Stream<Empty> isType(@Name("value") Object value, @Name("type") String type) {
        String typeName = Types.of(value).name();
        if (value != null && value.getClass().isArray()) typeName +="[]";
        return Empty.stream(type.equalsIgnoreCase(typeName));
    }


    public static class MetaStats {
        public final long labelCount;
        public final long relTypeCount;
        public final long propertyKeyCount;
        public final long nodeCount;
        public final long relCount;
        public final Map<String,Long> labels;
        public final Map<String,Long> relTypes;
        public final Map<String,Object> stats;

        public MetaStats(long labelCount, long relTypeCount, long propertyKeyCount, long nodeCount, long relCount, Map<String, Long> labels, Map<String, Long> relTypes) {
            this.labelCount = labelCount;
            this.relTypeCount = relTypeCount;
            this.propertyKeyCount = propertyKeyCount;
            this.nodeCount = nodeCount;
            this.relCount = relCount;
            this.labels = labels;
            this.relTypes = relTypes;
            this.stats =  map("labelCount", labelCount, "relTypeCount", relTypeCount, "propertyKeyCount", propertyKeyCount,
                    "nodeCount", nodeCount, "relCount", relCount,
                    "labels", labels, "relTypes", relTypes);
        }
    }
    @Procedure
    @Description("apoc.meta.stats  yield labelCount, relTypeCount, propertyKeyCount, nodeCount, relCount, labels, relTypes, stats | returns the information stored in the transactional database statistics")
    public Stream<MetaStats> stats() {
        ReadOperations ops = kernelTx.acquireStatement().readOperations();
        int labelCount = ops.labelCount();
        int relTypeCount = ops.relationshipTypeCount();

        Map<String,Long> labelStats = new LinkedHashMap<>(labelCount);
        Map<String,Long> relStats =   new LinkedHashMap<>(labelCount*relTypeCount);

        Map<String, Integer> labels =
                db.getAllLabelsInUse().stream()
                        .collect(toMap(
                                Label::name,
                                label -> (int)ops.labelGetForName(label.name())));

        Map<String, Integer> relTypes =
                db.getAllRelationshipTypesInUse().stream()
                        .collect(toMap(
                                RelationshipType::name,
                                type -> ops.relationshipTypeGetForName(type.name())));

        labels.forEach((name,id) -> {
            long count = ops.countsForNodeWithoutTxState(id);
            if (count>0) {
                labelStats.put(name, count);
                relTypes.forEach((typeName, typeId) -> {
                    long relCount = ops.countsForRelationship(id, typeId, ANY_LABEL);
                    if (relCount > 0) {
                        relStats.put("(:" + name + ")-[:" + typeName + "]->()", relCount);
                    }
                    relCount = ops.countsForRelationship(ANY_LABEL, typeId, id);
                    if (relCount > 0) {
                        relStats.put("()-[:" + typeName + "]->(:"+name+")", relCount);
                    }
                    relCount = ops.countsForRelationship(ANY_LABEL, typeId, ANY_LABEL);
                    if (relCount > 0) {
                        relStats.put("()-[:" + typeName + "]->()", relCount);
                    }
                });
            }
        });
        relTypes.forEach((typeName, typeId) -> {
            long relCount = ops.countsForRelationship(ANY_LABEL, typeId, ANY_LABEL);
            if (relCount > 0) {
                relStats.put("()-[:" + typeName + "]->()", relCount);
            }
        });
        Map<String, Object> result = map("labelCount", labelCount,
                "relTypeCount", relTypeCount,
                "propertyKeyCount", ops.propertyKeyCount(),
                "nodeCount", ops.nodesGetCount(),
                "relCount", ops.relationshipsGetCount(),
                "labels",labelStats,
                "relTypes",relStats);
        return Stream.of(new MetaStats(labelCount,relTypeCount,ops.propertyKeyCount(),
                ops.nodesGetCount(),ops.relationshipsGetCount(),labelStats,relStats));
    }

    @Procedure
    @Description("apoc.meta.data  - examines a subset of the graph to provide a tabular meta information")
    public Stream<MetaResult> data() {
        // db size, all labels, all rel-types
        Map<String,Map<String,MetaResult>> metaData = new LinkedHashMap<>(100);
        Schema schema = db.schema();

        Map<String, Iterable<ConstraintDefinition>> relConstraints = new HashMap<>(20);
        for (RelationshipType type : db.getAllRelationshipTypesInUse()) {
            metaData.put(type.name(), new LinkedHashMap<>(10));
            relConstraints.put(type.name(),schema.getConstraints(type));
        }

        for (Label label : db.getAllLabelsInUse()) {
            Map<String,MetaResult> nodeMeta = new LinkedHashMap<>(50);
            String labelName = label.name();
            metaData.put(labelName, nodeMeta);
            Iterable<ConstraintDefinition> constraints = schema.getConstraints(label);
            Set<String> indexed = new LinkedHashSet<>();
            for (IndexDefinition index : schema.getIndexes(label)) {
                for (String prop : index.getPropertyKeys()) {
                    indexed.add(prop);
                }
            }
            try (ResourceIterator<Node> nodes = db.findNodes(label)) {
                int count = 0;
                while (nodes.hasNext() && count++ < SAMPLE) {
                    Node node = nodes.next();
                    addRelationships(metaData,nodeMeta, labelName, node,relConstraints);
                    addProperties(nodeMeta, labelName, constraints, indexed, node);
                }
            }
        }
        return metaData.values().stream().flatMap(x -> x.values().stream());
    }

    private void addProperties(Map<String, MetaResult> properties, String labelName, Iterable<ConstraintDefinition> constraints, Set<String> indexed, PropertyContainer pc) {
        for (String prop : pc.getPropertyKeys()) {
            if (properties.containsKey(prop)) continue;
            MetaResult res = metaResultForProp(pc, labelName, prop);
            addSchemaInfo(res, prop, constraints, indexed);
            properties.put(prop,res);
        }
    }

    private void addRelationships(Map<String, Map<String, MetaResult>> metaData, Map<String, MetaResult> nodeMeta, String labelName, Node node, Map<String, Iterable<ConstraintDefinition>> relConstraints) {
        for (RelationshipType type : node.getRelationshipTypes()) {
            int out = node.getDegree(type, Direction.OUTGOING);
            if (out == 0) continue;

            String typeName = type.name();

            Iterable<ConstraintDefinition> constraints = relConstraints.get(typeName);
            if (!nodeMeta.containsKey(typeName)) nodeMeta.put(typeName, new MetaResult(labelName,typeName));
//            int in = node.getDegree(type, Direction.INCOMING);

            Map<String, MetaResult> typeMeta = metaData.get(typeName);
            if (!typeMeta.containsKey(labelName)) typeMeta.put(labelName,new MetaResult(typeName,labelName));
            MetaResult relMeta = nodeMeta.get(typeName);
            addOtherNodeInfo(node, labelName, out, type, relMeta , typeMeta, constraints);
        }
    }

    private void addOtherNodeInfo(Node node, String labelName, int out, RelationshipType type, MetaResult relMeta, Map<String, MetaResult> typeMeta, Iterable<ConstraintDefinition> relConstraints) {
        MetaResult relNodeMeta = typeMeta.get(labelName);
        for (Relationship rel : node.getRelationships(type, Direction.OUTGOING)) {
            Node endNode = rel.getEndNode();
            List<String> labels = toStrings(endNode.getLabels());
            int in = endNode.getDegree(type, Direction.INCOMING);
            relMeta.inc().other(labels).rel(out , in);
            relNodeMeta.inc().other(labels).rel(out,in);
            addProperties(typeMeta, type.name(), relConstraints, Collections.emptySet(), rel);
        }
    }

    private void addSchemaInfo(MetaResult res, String prop, Iterable<ConstraintDefinition> constraints, Set<String> indexed) {
        if (indexed.contains(prop)) {
            res.index = true;
        }
        if (constraints == null) return;
        for (ConstraintDefinition constraint : constraints) {
            for (String key : constraint.getPropertyKeys()) {
                if (key.equals(prop)) {
                    switch (constraint.getConstraintType()) {
                        case UNIQUENESS: res.unique = true; break;
                        case NODE_PROPERTY_EXISTENCE:res.existence = true; break;
                        case RELATIONSHIP_PROPERTY_EXISTENCE: res.existence = true; break;
                    }
                }
            }
        }
    }

    private MetaResult metaResultForProp(PropertyContainer pc, String labelName, String prop) {
        MetaResult res = new MetaResult(labelName, prop);
        Object value = pc.getProperty(prop);
        res = res.type(Types.of(value).name());
        if (value.getClass().isArray()) {
            res.array = true;
        }
        return res;
    }

    private List<String> toStrings(Iterable<Label> labels) {
        List<String> res=new ArrayList<>(10);
        for (Label label : labels) {
            String name = label.name();
            res.add(name);
        }
        return res;
    }

    interface Sampler {
        void sample(Label label, int count, Node node);
        void sample(Label label, int count, Node node, RelationshipType type, Direction direction, int degree, Relationship rel);
    }
    public void sample(GraphDatabaseService db, Sampler sampler, int sampleSize) {
        for (Label label : db.getAllLabelsInUse()) {
            ResourceIterator<Node> nodes = db.findNodes(label);
            int count = 0;
            while (nodes.hasNext() && count++ < sampleSize) {
                Node node = nodes.next();
                sampler.sample(label,count,node);
                for (RelationshipType type : node.getRelationshipTypes()) {
                    sampleRels(sampleSize, sampler, label, count, node, type);
                }
            }
            nodes.close();
        }
    }

    private void sampleRels(int sampleSize, Sampler sampler, Label label, int count, Node node, RelationshipType type) {
        Direction direction = Direction.OUTGOING;
        int degree = node.getDegree(type, direction);
        sampler.sample(label,count,node,type,direction,degree,null);
        if (degree==0) return;
        ResourceIterator<Relationship> relIt = ((ResourceIterable<Relationship>)node.getRelationships(direction, type)).iterator();
        int relCount = 0;
        while (relIt.hasNext() && relCount++ < sampleSize) {
            sampler.sample(label,count,node,type,direction,degree,relIt.next());
        }
        relIt.close();
    }

    @Procedure
    @Description("apoc.meta.graph - examines the full graph to create the meta-graph")
    public Stream<GraphResult> graph() {
        Map<String,Node> labels = new TreeMap<>();
        Map<List<String>,Relationship> rels = new HashMap<>();
        for (Node node : db.getAllNodes()) {
            addLabels(labels,node, true);
        }
        for (Relationship rel : db.getAllRelationships()) {
            addRel(rels, labels, rel, false);
        }
        return Stream.of(new GraphResult(new ArrayList<>(labels.values()), new ArrayList<>(rels.values())));
    }


    @Procedure
    @Description("apoc.meta.graphSample(sampleSize) - examines a sample graph to create the meta-graph, default sampleSize is 100")
    public Stream<GraphResult> graphSample(@Name("sample") Long sampleSize ) {
        Map<String, Node> labels = new TreeMap<>();
        Map<List<String>,Relationship> rels = new HashMap<>();
        Sampler sampler = new Sampler() {
            public void sample(Label label, int count, Node node) {
                mergeMetaNode(label, labels,true);
            }
            public void sample(Label label, int count, Node node, RelationshipType type, Direction direction, int degree, Relationship rel) {
                if (rel!=null) {
                    addRel(rels, labels, rel, false);
                }
            }
        };
        long sample = sampleSize == null || sampleSize < 100 ? SAMPLE : sampleSize;
        sample(db,sampler, (int) sample);
        return Stream.of(new GraphResult(new ArrayList<>(labels.values()), new ArrayList<>(rels.values())));
    }

    @Procedure
    @Description("apoc.meta.subGraph({labels:[labels],rels:[rel-types],sample:sample,strict:true/false}) - examines a sample sub graph to create the meta-graph, default sampleSize is 100, default strict mode is false")
    public Stream<GraphResult> subGraph(@Name("config") Map<String,Object> config ) {
        Set<String> includeLabels = new HashSet<>((Collection<String>)config.getOrDefault("labels",emptyList()));
        Set<String> includeRels = new HashSet<>((Collection<String>)config.getOrDefault("rels",emptyList()));
        Map<String, Node> labels = new TreeMap<>();
        Map<List<String>,Relationship> rels = new HashMap<>();
        boolean strict = ((Boolean)config.getOrDefault("strict",Boolean.FALSE)).booleanValue();
        boolean restrictLabels = strict ? includeLabels.isEmpty() : false;
        strict = restrictLabels ? includeRels.isEmpty() : strict;
        boolean restrictRels = strict; // effectively final
        Sampler sampler = new Sampler() {
            public void sample(Label label, int count, Node node) {
                sample(label, true);
            }
            private void sample(Label label, boolean increment) {
                if (restrictLabels) return;
                if (includeLabels.isEmpty() || includeLabels.contains(label.name())) {
                    mergeMetaNode(label, labels, increment);
                }
            }
            public void sample(Label label, int count, Node node, RelationshipType type, Direction direction, int degree, Relationship rel) {
                if (rel!=null && (includeRels.isEmpty() || includeRels.contains(type.name()))) {
                    for (Label labelA : rel.getStartNode().getLabels()) {
                        sample(labelA, false);
                    }
                    for (Label labelB : rel.getEndNode().getLabels()) {
                        sample(labelB, false);
                    }
                    addRel(rels, labels, rel, restrictRels);
                }
            }
        };
        long sample = ((Number)config.getOrDefault("sample",100)).longValue();
        sample(db,sampler, (int) sample);
        return Stream.of(new GraphResult(new ArrayList<>(labels.values()), new ArrayList<>(rels.values())));
    }

    private Node mergeMetaNode(Label label, Map<String, Node> labels, boolean increment) {
        String name = label.name();
        Node vNode = labels.get(name);
        if (vNode == null) {
            vNode = new VirtualNode(new Label[] {label}, Collections.singletonMap("name", name),db);
            labels.put(name, vNode);
        }
        if (increment) vNode.setProperty("count",((int)vNode.getProperty("count",0))+1);
        return vNode;
    }

    private void addRel(Map<List<String>, Relationship> rels, Map<String, Node> labels, Relationship rel, boolean strict) {
        String typeName = rel.getType().name();
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();
        for (Label labelA : startNode.getLabels()) {
            Node nodeA = strict ? labels.get(labelA.name()) : mergeMetaNode(labelA,labels,false);
            if (nodeA == null) continue;
            for (Label labelB : endNode.getLabels()) {
                List<String> key = asList(labelA.name(), labelB.name(), typeName);
                Relationship vRel = rels.get(key);
                if (vRel==null) {
                    Node nodeB = strict ? labels.get(labelB.name()) : mergeMetaNode(labelB,labels,false);
                    if (nodeB == null) continue;
                    vRel = new VirtualRelationship(nodeA,nodeB,rel.getType()).withProperties(singletonMap("type",typeName));
                    rels.put(key,vRel);
                }
                vRel.setProperty("count",((int)vRel.getProperty("count",0))+1);
            }
        }
    }

    private void addLabels(Map<String, Node> labels, Node node, boolean increment) {
        for (Label label : node.getLabels()) {
            mergeMetaNode(label, labels, increment);
        }
    }

    static class RelInfo {
        final Set<String> properties = new HashSet<>();
        final NodeInfo from,to;
        final String type;
        int count;

        public RelInfo(NodeInfo from, NodeInfo to, String type) {
            this.from = from;
            this.to = to;
            this.type = type;
        }
        public void add(Relationship relationship) {
            for (String key : relationship.getPropertyKeys()) properties.add(key);
            count++;
        }
    }
    static class NodeInfo {
        final Set<String> labels=new HashSet<>();
        final Set<String> properties = new HashSet<>();
        long count, minDegree, maxDegree, sumDegree;

        private void add(Node node) {
            count++;
            int degree = node.getDegree();
            sumDegree += degree;
            if (degree > maxDegree) maxDegree = degree;
            if (degree < minDegree) minDegree = degree;

            for (Label label : node.getLabels()) labels.add(label.name());
            for (String key : node.getPropertyKeys()) properties.add(key);
        }
        Map<String,Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("labels",labels.toArray());
            map.put("properties",properties.toArray());
            map.put("count",count);
            map.put("minDegree",minDegree);
            map.put("maxDegree",maxDegree);
            map.put("avgDegree",sumDegree/count);
            return map;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof NodeInfo && labels.equals(((NodeInfo) o).labels);

        }

        @Override
        public int hashCode() {
            return labels.hashCode();
        }
    }
}
