package apoc.meta;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.*;
import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

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


    @UserFunction
    @Description("apoc.meta.type(value) - type name of a value (INTEGER,FLOAT,STRING,BOOLEAN,RELATIONSHIP,NODE,PATH,NULL,UNKNOWN,MAP,LIST)")
    public String type(@Name("value") Object value) {
        return typeName(value);
    }
    @UserFunction
    @Description("apoc.meta.typeName(value) - type name of a value (INTEGER,FLOAT,STRING,BOOLEAN,RELATIONSHIP,NODE,PATH,NULL,UNKNOWN,MAP,LIST)")
    public String typeName(@Name("value") Object value) {
        Types type = Types.of(value);
        String typeName = type == Types.UNKNOWN ? value.getClass().getSimpleName() : type.name();

        if (value != null && value.getClass().isArray()) typeName +="[]";
        return typeName;
    }
    @UserFunction
    @Description("apoc.meta.types(node-relationship-map)  - returns a map of keys to types")
    public Map<String,Object> types(@Name("properties") Object target) {
        Map<String,Object> properties = Collections.emptyMap();
        if (target instanceof Node) properties = ((Node)target).getAllProperties();
        if (target instanceof Relationship) properties = ((Relationship)target).getAllProperties();
        if (target instanceof Map) {
            //noinspection unchecked
            properties = (Map<String, Object>) target;
        }

        Map<String,Object> result = new LinkedHashMap<>(properties.size());
        properties.forEach((key,value) -> {
            Types type = Types.of(value);
            String typeName = type == Types.UNKNOWN ? value.getClass().getSimpleName() : type.name();
            if (value != null && value.getClass().isArray()) typeName +="[]";
            result.put(key, typeName);
        });

        return result;
    }

    @UserFunction
    @Description("apoc.meta.isType(value,type) - returns a row if type name matches none if not (INTEGER,FLOAT,STRING,BOOLEAN,RELATIONSHIP,NODE,PATH,NULL,UNKNOWN,MAP,LIST)")
    public boolean isType(@Name("value") Object value, @Name("type") String type) {
        String typeName = Types.of(value).name();
        if (value != null && value.getClass().isArray()) typeName +="[]";
        return type.equalsIgnoreCase(typeName);
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

    interface StatsCallback {
        void label(int labelId, String labelName, long count);
        void rel(int typeId, String typeName, long count);
        void rel(int typeId, String typeName, int labelId, String labelName, long out, long in);
    }

    @Procedure
    @Description("apoc.meta.stats  yield labelCount, relTypeCount, propertyKeyCount, nodeCount, relCount, labels, relTypes, stats | returns the information stored in the transactional database statistics")
    public Stream<MetaStats> stats() {
        return Stream.of(collectStats());
    }

    private MetaStats collectStats() {
        ReadOperations ops = kernelTx.acquireStatement().readOperations();
        int labelCount = ops.labelCount();
        int relTypeCount = ops.relationshipTypeCount();

        Map<String, Long> labelStats = new LinkedHashMap<>(labelCount);
        Map<String, Long> relStats = new LinkedHashMap<>(2 * relTypeCount);

        collectStats(null, null, new StatsCallback() {
            public void label(int labelId, String labelName, long count) {
                if (count > 0) labelStats.put(labelName, count);
            }

            public void rel(int typeId, String typeName, long count) {
                if (count > 0) relStats.put("()-[:" + typeName + "]->()", count);
            }

            public void rel(int typeId, String typeName, int labelId, String labelName, long out, long in) {
                if (out > 0) relStats.put("(:" + labelName + ")-[:" + typeName + "]->()", out);
                if (in > 0) relStats.put("()-[:" + typeName + "]->(:" + labelName + ")", in);
            }
        });
        return new MetaStats(labelCount, relTypeCount, ops.propertyKeyCount(),
                ops.nodesGetCount(), ops.relationshipsGetCount(), labelStats, relStats);
    }

    private void collectStats(Collection<String> labelNames, Collection<String> relTypeNames, StatsCallback cb) {
        ReadOperations ops = kernelTx.acquireStatement().readOperations();

        Map<String,Integer> labels = labelsInUse(ops, labelNames);
        Map<String,Integer> relTypes = relTypesInUse(ops, relTypeNames);

        labels.forEach((name,id) -> {
            long count = ops.countsForNodeWithoutTxState(id);
            if (count>0) {
                cb.label(id,name,count);
                relTypes.forEach((typeName, typeId) -> {
                    long relCountOut = ops.countsForRelationship(id, typeId, ANY_LABEL);
                    long relCountIn = ops.countsForRelationship(ANY_LABEL, typeId, id);
                    cb.rel(typeId, typeName, id, name, relCountOut, relCountIn);
                });
            }
        });
        relTypes.forEach((typeName, typeId) -> {
            cb.rel(typeId,typeName, ops.countsForRelationship(ANY_LABEL, typeId, ANY_LABEL));
        });
    }

    private Map<String, Integer> relTypesInUse(ReadOperations ops, Collection<String> relTypeNames) {
        Stream<String> types = (relTypeNames == null || relTypeNames.isEmpty()) ?
                db.getAllRelationshipTypesInUse().stream().map(RelationshipType::name) :
                relTypeNames.stream();
        return types.collect(toMap(t -> t, ops::relationshipTypeGetForName));
    }

    private Map<String, Integer> labelsInUse(ReadOperations ops, Collection<String> labelNames) {
        Stream<String> labels = (labelNames == null || labelNames.isEmpty()) ?
                db.getAllLabelsInUse().stream().map(Label::name) :
                labelNames.stream();
        return labels.collect(toMap(t -> t, ops::labelGetForName));
    }

    // todo ask index for distinct values if index size < 10 or so
    // todo put index sizes for indexed properties
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

    static class Pattern {
        private final String from;
        private final String type;
        private final String to;

        private Pattern(String from, String type, String to) {
            this.from = from;
            this.type = type;
            this.to = to;
        }
        public static Pattern of(String labelFrom, String type, String labelTo) {
            return new Pattern(labelFrom,type,labelTo);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Pattern) {
                Pattern pattern = (Pattern) o;
                return from.equals(pattern.from) && type.equals(pattern.type) && to.equals(pattern.to);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 31 * (31 * from.hashCode() + type.hashCode()) + to.hashCode();
        }

        public Label labelTo() {
            return Label.label(to);
        }
        public Label labelFrom() {
            return Label.label(from);
        }
        public RelationshipType relationshipType() {
            return RelationshipType.withName(type);
        }
    }
    @Procedure
    @Description("apoc.meta.graph - examines the full graph to create the meta-graph")
    public Stream<GraphResult> graph() {
        return metaGraph(null, null, true);
    }

    private Stream<GraphResult> metaGraph(Collection<String> labelNames, Collection<String> relTypeNames, boolean removeMissing) {
        ReadOperations ops = kernelTx.acquireStatement().readOperations();
        Map<String,Integer> labels = labelsInUse(ops, labelNames);
        Map<String,Integer> relTypes = relTypesInUse(ops, relTypeNames);
        Map<String,Node> vNodes = new TreeMap<>();
        Map<Pattern,Relationship> vRels = new HashMap<>(relTypes.size()*2);


        labels.forEach((labelName, id) -> {
            long count = ops.countsForNodeWithoutTxState(id);
            if (count>0) {
                mergeMetaNode(Label.label(labelName), vNodes, count);
            }
        });
        relTypes.forEach((typeName, typeId) -> {
            long global = ops.countsForRelationshipWithoutTxState(ANY_LABEL, typeId, ANY_LABEL);
            labels.forEach((labelNameA, labelIdA) -> {
                long relCountOut = ops.countsForRelationshipWithoutTxState(labelIdA, typeId, ANY_LABEL);
                if (relCountOut > 0) {
                    labels.forEach((labelNameB, labelIdB) -> {
                        long relCountIn = ops.countsForRelationshipWithoutTxState(ANY_LABEL, typeId, labelIdB);
                        if (relCountIn > 0) {
                            Node nodeA = vNodes.get(labelNameA);
                            Node nodeB = vNodes.get(labelNameB);
                            Relationship vRel = new VirtualRelationship(nodeA, nodeB, RelationshipType.withName(typeName))
                                    .withProperties(map("type", typeName, "out", relCountOut, "in", relCountIn, "count", global));
                            vRels.put(Pattern.of(labelNameA, typeName, labelNameB), vRel);
                        }
                    });
                }
            });
        });
        if (removeMissing) filterNonExistingRelationships(vRels);
        GraphResult graphResult = new GraphResult(new ArrayList<>(vNodes.values()), new ArrayList<>(vRels.values()));
        return Stream.of(graphResult);
    }

    private void filterNonExistingRelationships(Map<Pattern, Relationship> vRels) {
        Set<Pattern> rels = vRels.keySet();
        Map<Pair<String,String>,Set<Pattern>> aggregated = new HashMap<>();
        for (Pattern rel : rels) {
            combine(aggregated, Pair.of(rel.from, rel.type), rel);
            combine(aggregated, Pair.of(rel.type, rel.to), rel);
        }
        aggregated.values().stream()
                .filter( c -> c.size() > 1)
                .flatMap(Collection::stream)
                .filter( p -> !relationshipExists(p, vRels.get(p)))
                .forEach(vRels::remove);
    }

    private boolean relationshipExists(Pattern p, Relationship relationship) {
        if (relationship==null) return false;
        double degreeFrom = (double)(long)relationship.getProperty("out")  / (long)relationship.getStartNode().getProperty("count");
        double degreeTo = (double)(long)relationship.getProperty("in")  / (long)relationship.getEndNode().getProperty("count");

        if (degreeFrom < degreeTo) {
            if (relationshipExists(p.labelFrom(), p.labelTo(), p.relationshipType(), Direction.OUTGOING)) return true;
        } else {
            if (relationshipExists(p.labelTo(), p.labelFrom(), p.relationshipType(), Direction.INCOMING)) return true;
        }
        return false;
    }

    private boolean relationshipExists(Label labelFromLabel, Label labelToLabel, RelationshipType relationshipType, Direction direction) {
        int maxTotal = 300;
        try (ResourceIterator<Node> nodes = db.findNodes(labelFromLabel)) {
            while (nodes.hasNext() && maxTotal > 0) {
                Node node = nodes.next();
                int maxRels = 30;
                for (Relationship rel : node.getRelationships(direction, relationshipType)) {
                    Node otherNode = direction == Direction.OUTGOING ? rel.getEndNode() : rel.getStartNode();
                    if (otherNode.hasLabel(labelToLabel)) return true;
                    if (maxRels-- == 0 || maxTotal-- == 0) break;
                }
            }
        }
        return false;
    }

    private void combine(Map<Pair<String, String>, Set<Pattern>> aggregated, Pair<String, String> p, Pattern rel) {
        if (!aggregated.containsKey(p)) aggregated.put(p,new HashSet<>());
        aggregated.get(p).add(rel);
    }


    @Procedure
    @Description("apoc.meta.graphSample() - examines the database statistics to build the meta graph, very fast, might report extra relationships")
    public Stream<GraphResult> graphSample() {
        return metaGraph(null, null, false);
    }

    @Procedure
    @Description("apoc.meta.subGraph({labels:[labels],rels:[rel-types], excludes:[labels,rel-types]}) - examines a sample sub graph to create the meta-graph")
    public Stream<GraphResult> subGraph(@Name("config") Map<String,Object> config ) {
        Collection<String> includeLabels = (Collection<String>)config.getOrDefault("labels",emptyList());
        Collection<String> includeRels = (Collection<String>)config.getOrDefault("rels",emptyList());
        Set<String> excludes = new HashSet<>((Collection<String>)config.getOrDefault("excludes",emptyList()));

        return filterResultStream(excludes, metaGraph(includeLabels, includeRels,true));
    }

    private Stream<GraphResult> filterResultStream(Set<String> excludes, Stream<GraphResult> graphResultStream) {
        if (excludes == null || excludes.isEmpty()) return graphResultStream;
        return graphResultStream.map(gr -> {
            Iterator<Node> it = gr.nodes.iterator();
            while (it.hasNext()) {
                Node node = it.next();
                if (containsLabelName(excludes,node)) it.remove();
            }

            Iterator<Relationship> it2 = gr.relationships.iterator();
            while (it2.hasNext()) {
                Relationship relationship = it2.next();
                if (excludes.contains(relationship.getType().name()) ||
                        containsLabelName(excludes, relationship.getStartNode()) ||
                        containsLabelName(excludes, relationship.getEndNode())) {
                    it2.remove();
                }
            }

            return gr;
        });
    }

    private boolean containsLabelName(Set<String> excludes, Node node) {
        for (Label label : node.getLabels()) {
            if (excludes.contains(label.name())) return true;
        }
        return false;
    }

    private Node mergeMetaNode(Label label, Map<String, Node> labels, long increment) {
        String name = label.name();
        Node vNode = labels.get(name);
        if (vNode == null) {
            vNode = new VirtualNode(new Label[] {label}, Collections.singletonMap("name", name),db);
            labels.put(name, vNode);
        }
        if (increment > 0 ) vNode.setProperty("count",(((Number)vNode.getProperty("count",0L)).longValue())+increment);
        return vNode;
    }

    private void addRel(Map<List<String>, Relationship> rels, Map<String, Node> labels, Relationship rel, boolean strict) {
        String typeName = rel.getType().name();
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();
        for (Label labelA : startNode.getLabels()) {
            Node nodeA = strict ? labels.get(labelA.name()) : mergeMetaNode(labelA,labels,0);
            if (nodeA == null) continue;
            for (Label labelB : endNode.getLabels()) {
                List<String> key = asList(labelA.name(), labelB.name(), typeName);
                Relationship vRel = rels.get(key);
                if (vRel==null) {
                    Node nodeB = strict ? labels.get(labelB.name()) : mergeMetaNode(labelB,labels,0);
                    if (nodeB == null) continue;
                    vRel = new VirtualRelationship(nodeA,nodeB,rel.getType()).withProperties(singletonMap("type",typeName));
                    rels.put(key,vRel);
                }
                vRel.setProperty("count",((long)vRel.getProperty("count",0L))+1);
            }
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
