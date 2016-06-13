package apoc.algo;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;

/**
 * @author mh
 * @since 11.06.16
 * sources:
 * http://www.frankmcsherry.org/graph/scalability/cost/2015/01/15/COST.html
 * https://en.wikipedia.org/wiki/Hilbert_curve
 * https://github.com/MicrosoftResearch/NaiadSamples
 * http://stackoverflow.com/questions/106237/calculate-the-hilbert-value-of-a-point-for-use-in-a-hilbert-r-tree/106277
 */
public class CoreGraphAlgorithms {
    private final Statement stmt;
    private int nodeCount;
    private int relCount;
    private int[] nodeRelOffsets;
    private int[] rels;
    public static final float ALPHA = 0.15f;
    private int labelId;
    private int relTypeId;

    private long spreadBits32(int y) {
        long x = y;
        x = (x | (x << 32)) & 0x00000000ffffffffL;
        x = (x | (x << 16)) & 0x0000ffff0000ffffL;
        x = (x | (x << 8)) & 0x00ff00ff00ff00ffL;
        x = (x | (x << 4)) & 0x0f0f0f0f0f0f0f0fL;
        x = (x | (x << 2)) & 0x3333333333333333L;
        x = (x | (x << 1)) & 0x5555555555555555L;
        return x;
    }

    private long interleave64(int x, int y) {
        return spreadBits32(x) | (spreadBits32(y) << 1);
    }

    /**
     * Find the Hilbert order (=vertex index) for the given grid cell
     * coordinates.
     *
     * @param x cell column (from 0)
     * @param y cell row (from 0)
     * @param r resolution of Hilbert curve (grid will have Math.pow(2,r)
     *          rows and cols)
     * @return Hilbert order
     */
    // java code adapted from C code in the paper "Encoding and decoding the Hilbert order" by Xian Lu and Gunther Schrack, published in Software: Practice and Experience Vol. 26 pp 1335-46 (1996).
    private int encode(int x, int y, int r) {

        int mask = (1 << r) - 1;
        int hodd = 0;
        int heven = x ^ y;
        int notx = ~x & mask;
        int noty = ~y & mask;
        int temp = notx ^ y;

        int v0 = 0, v1 = 0;
        for (int k = 1; k < r; k++) {
            v1 = ((v1 & heven) | ((v0 ^ noty) & temp)) >> 1;
            v0 = ((v0 & (v1 ^ notx)) | (~v0 & (v1 ^ noty))) >> 1;
        }
        hodd = (~v0 & (v1 ^ x)) | (v0 & (v1 ^ noty));

        return interleaveBits(hodd, heven);
    }

    /**
     * Interleave the bits from two input integer values
     *
     * @param odd  integer holding bit values for odd bit positions
     * @param even integer holding bit values for even bit positions
     * @return the integer that results from interleaving the input bits
     * @todo: I'm sure there's a more elegant way of doing this !
     */
    private static int interleaveBits(int odd, int even) {
        int val = 0;
        // Replaced this line with the improved code provided by Tuska
        // int n = Math.max(Integer.highestOneBit(odd), Integer.highestOneBit(even));
        int max = Math.max(odd, even);
        int n = 0;
        while (max > 0) {
            n++;
            max >>= 1;
        }

        for (int i = 0; i < n; i++) {
            int bitMask = 1 << i;
            int a = (even & bitMask) > 0 ? (1 << (2 * i)) : 0;
            int b = (odd & bitMask) > 0 ? (1 << (2 * i + 1)) : 0;
            val += a + b;
        }

        return val;
    }


    //convert (x,y) to d
    int xy2d(int n, int x, int y) {
        int[] xy = new int[2];
        int rx, ry, d = 0;
        // loop over bits not value
        for (int s = n >> 1; s != 0; s >>= 1) {
            rx = (x & s) != 0 ? 1 : 0; // todo better, perhaps 0*x or / s aka >>
            ry = (y & s) != 0 ? 1 : 0;
            d += s * s * ((3 * rx) ^ ry);
            xy[0] = x;
            xy[1] = y;
            rot(s, xy, rx, ry);
        }
        return d;
    }

    //convert d to (x,y)
    void d2xy(int n, int d, int[] xy) {
        int rx, ry, s, t = d;
        xy[0] = 0;
        xy[1] = 0;
        for (s = 1; s != n; s <<= 1) {
            rx = 1 & (t >> 1);
            ry = 1 & (t ^ rx);
            rot(s, xy, rx, ry);
            // if s was bits then shift
            xy[0] += s * rx;
            xy[1] += s * ry;
            t /= 4;
        }
    }

    //rotate/flip a quadrant appropriately
    void rot(int n, int[] xy, int rx, int ry) {
        if (ry == 0) {
            if (rx == 1) {
                xy[0] = n - 1 - xy[0];
                xy[1] = n - 1 - xy[1];
            }
            int t = xy[0];
            xy[0] = xy[1];
            xy[1] = t;
        }
    }

    // TODO
    /*
    https://github.com/MicrosoftResearch/NaiadSamples/blob/master/COST/COST/HilbertCurve.cs#L199

    JMH tests

    sparse node-id -> incremental id int[] = nodeId write as they come in
    alternatively insertion sort ?
    write out reverse by iterating over array
    quick lookup ?
    or parallel buckets

    methods to load page-cache into datastructure in parallel

    all nodes / nodes by label (LSS or filter depending on percentage -> measure) / label/property / index lookup
    all rels / rels by type / by type & property


    original data structure McSherry
    nodes: id -> degree
    rels: degree * end-node-id
    */

    public static int toInt( double value )
    {
        return (int) (100_000 * value);
    }

    public static double toFloat( int value )
    {
        return value / 100_000.0;
    }

    // todo parallel, see if java8 streams are performing
    // pass in array, use array batches (pass in batch-no)
    private int[] loadNodes(ReadOperations ops, PrimitiveLongIterator nodeIds, int size, int relType, Direction direction) throws EntityNotFoundException {
        // todo reuse array
        int[] offsets = new int[size];
        int offset = 0;
        while (nodeIds.hasNext()) {
            long nodeId = nodeIds.next();
            int degree = relType == ANY_RELATIONSHIP_TYPE ? ops.nodeGetDegree(nodeId, direction) : ops.nodeGetDegree(nodeId, direction, relType);
            offsets[mapId(nodeId)] = offset;
            offset += degree;
        }
        return offsets;
    }

    private int[] loadDegrees(ReadOperations ops, PrimitiveLongIterator nodeIds, int size, int relType, Direction direction) throws EntityNotFoundException {
        int[] degrees = new int[size];
        while (nodeIds.hasNext()) {
            long nodeId = nodeIds.next();
            degrees[mapId(nodeId)] = relType == ANY_RELATIONSHIP_TYPE ? ops.nodeGetDegree(nodeId, direction) : ops.nodeGetDegree(nodeId, direction, relType);
        }
        return degrees;
    }

    public int[] loadDegrees(String relName, Direction direction) throws EntityNotFoundException {
        ReadOperations ops = stmt.readOperations();
        int[] degrees = new int[nodeCount];
        if (relName == null) {
            for (int nodeIdx=0; nodeIdx<nodeCount; nodeIdx++) {
                degrees[nodeIdx] = ops.nodeGetDegree(unMapId(nodeIdx), direction);
            }
        }
        else {
            int relType = ops.relationshipTypeGetForName(relName);
            for (int nodeIdx=0; nodeIdx<nodeCount; nodeIdx++) {
                degrees[nodeIdx] = ops.nodeGetDegree(unMapId(nodeIdx), direction, relType);
            }
        }
        return degrees;
    }
    private int[] loadDegrees(ReadOperations ops, int relType, Direction direction) {
        try {
            int[] degrees = new int[nodeCount];
            for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
                degrees[nodeIdx] = relType == ANY_RELATIONSHIP_TYPE ?
                        ops.nodeGetDegree(unMapId(nodeIdx), direction) :
                        ops.nodeGetDegree(unMapId(nodeIdx), direction, relType);
            }
            return degrees;
        } catch (EntityNotFoundException e) {
            throw new RuntimeException("Error loading node degrees",e);
        }
    }

    private int[] loadNodes(ReadOperations ops, PrimitiveLongIterator nodeIds, int labelId, int size, int relType) throws EntityNotFoundException {
        // todo reuse array
        int[] degrees = new int[size];
        while (nodeIds.hasNext()) {
            long nodeId = nodeIds.next();
            if (labelId != -1) {
                Cursor<NodeItem> c = ops.nodeCursor(nodeId);
                if (!(c.next() && c.get().hasLabel(labelId))) continue;
            }
            degrees[mapId(nodeId)] = relType == ANY_RELATIONSHIP_TYPE ? ops.nodeGetDegree(nodeId, OUTGOING) : ops.nodeGetDegree(nodeId, OUTGOING, relType);
        }
        return degrees;
    }

    // todo fix node offsets when no rel found
    private int loadNodeRels(ReadOperations ops, PrimitiveLongIterator nodeIds, int relType, int[] rels) throws EntityNotFoundException {
        int idx = 0;
        int count = 0;
        while (nodeIds.hasNext()) {
            long id = nodeIds.next();
            RelationshipIterator relIds = relType == ANY_RELATIONSHIP_TYPE ? ops.nodeGetRelationships(id, OUTGOING) : ops.nodeGetRelationships(id, OUTGOING, relType);
            while (relIds.hasNext()) {
                Cursor<RelationshipItem> c = ops.relationshipCursor(relIds.next());
                if (c.next()) {
                    rels[idx] = mapId(c.get().endNode());
                    count++;
                }
                idx++;
            }
        }
        return count;
    }

    interface RelationshipProgram {
        void accept(int start, int end);
    }

    private void runProgram(RelationshipProgram consumer) {
        runProgram(nodeCount, nodeRelOffsets,rels,consumer);
    }

    private static void runProgram(int nodeCount, int[] offsets, int[] rels, RelationshipProgram consumer) {
        int start;
        for (start = 0; start < nodeCount ; start++) {
            int offset = offsets[start];
            int nextOffset = start+1 == nodeCount ? rels.length : offsets[start+1];
            while (offset != nextOffset) {
                int end = rels[offset];
                if (end == -1) break;
                consumer.accept(start, end);
                offset++;
            }
        }
    }

    /*
        let mut src: Vec<f32> = (0..nodes).map(|_| 0f32).collect();
    let mut dst: Vec<f32> = (0..nodes).map(|_| 0f32).collect();
    let mut deg: Vec<f32> = (0..nodes).map(|_| 0f32).collect();
    graph.map_edges(|x, _| { deg[x] += 1f32 });
    for _iteration in (0 .. 20) {
        for node in (0 .. nodes) {
            src[node] = alpha * dst[node] / deg[node];
            dst[node] = 1f32 - alpha;
        }
        graph.map_edges(|x, y| { dst[y] += src[x]; });
    }

     */

    public float[] pageRank(int iterations) {
        float oneMinusAlpha = 1 - ALPHA;
        int[] degrees = loadDegrees(stmt.readOperations(), relTypeId , OUTGOING);
        float[] dst = new float[nodeCount]; float[] src = new float[nodeCount];

        for (int it = 0; it < iterations; it++) {
            for (int node = 0; node < nodeCount; node++) {
                src[node] = ALPHA * dst[node] / (float) degrees[node];
                dst[node] = oneMinusAlpha;
            }
            runProgram((start, end) -> dst[end] += src[start]);
        }
        for (int node = 0; node < nodeCount; node++) {
            if (degrees[node] == 0 && dst[node] == oneMinusAlpha) dst[node] = 0;
        }
        return dst;
    }

    interface SuperStep {
        boolean run();
    }

    public void pregel(RelationshipProgram program, SuperStep superStep) {
        while (superStep.run()) {
            runProgram(program);
        }
    }


    public float[] pageRank2(int iterations) {
        class PageRank implements SuperStep, RelationshipProgram {
            private int iterations;
            float alpha = 0.15f; float oneMinusAlpha = 1 - alpha;
            float[] dst = new float[nodeCount]; float[] src = new float[nodeCount];

            public PageRank(int iterations) {
                this.iterations = iterations;
            }

            @Override
            public void accept(int start, int end) {
                dst[end] += src[start];
            }

            @Override
            public boolean run() {
                for (int node = 0; node < nodeCount; node++) {
                    src[node] = alpha * dst[node] / (float) nodeRelOffsets[node];
                    dst[node] = oneMinusAlpha;
                }
                return iterations-- > 0;
            }
        }

        PageRank program = new PageRank(iterations);
        pregel(program,program);
        return program.dst;
    }

    /*
    fn label_propagation<G: EdgeMapper>(graph: &G, nodes: u32) {
    let mut label: Vec<u32> = (0..nodes).collect();
    let mut done = false;
    while !done {
        done = true;
        graph.map_edges(|x, y| {
            if label[x] != label[y] {
                done = false;
                label[x] = min(label[x], label[y]);
                label[y] = min(label[x], label[y]);
            }
        });
    }
}
     */

    public int[] labelPropagation() {
        int[] labels = new int[nodeCount];
        for (int nodeId = 0; nodeId < nodeCount; nodeId++) labels[nodeId] = nodeId;

        boolean[] done = {false};
        while (!done[0]) {
            done[0] = true;
            runProgram((start, end) -> {
                if (labels[start] != labels[end]) {
                    done[0] = false;
                    labels[start] = labels[end] = Math.min(labels[start],labels[end]);
                }
            });
        }
        return labels;
    }

    /*
    fn union_find<G: EdgeMapper>(graph: &G, nodes: u32) {
    let mut root: Vec<u32> = (0..nodes).collect();
    let mut rank: Vec<u8> = (0..nodes).map(|_| 0u8).collect();
    graph.map_edges(|mut x, mut y| {
        while x != root[x] { x = root[x]; }
        while y != root[y] { y = root[y]; }
        if x != y {
            match rank[x].cmp(&rank[y]) {
                Less    => root[x] = y,
                Greater => root[y] = x,
                Equal   => { root[y] = x; rank[x] += 1 },
            }
        }
    });
}
     */

    public int[] unionFind() {
        byte[] rank = new byte[nodeCount];
        int[] root = new int[nodeCount];
        for (int nodeId = 0; nodeId < nodeCount; nodeId++) root[nodeId] = nodeId;

        runProgram((x, y) -> {
            while (x != root[x]) x = root[x];
            while (y != root[y]) y = root[y];
            if (x != y) {
                if (rank[x] >= rank[y]) {
                    root[y] = x;
                    if (rank[x] == rank[y]) rank[x] += 1;
                } else {
                    root[x] = y;
                }
            }
        });
        return root;
    }

    private int loadRels(ReadOperations ops, PrimitiveLongIterator relIds, int size, int relType, int[] rels) throws EntityNotFoundException {
        // todo reuse array
        int idx = 0, count = 0;
        while (relIds.hasNext()) {
            long relId = relIds.next();
            Cursor<RelationshipItem> c = ops.relationshipCursor(relId);
            if (c.next()) {
                if (relType == -1 || c.get().type() == relType) {
                    rels[idx] = mapId(c.get().endNode());
                    count ++;
                }
            }
            idx++;
        }
        return count;
    }


    private static int mapId(long id) {
        return (int) id; // TODO proper mapping to smaller array
    }
    private static long unMapId(int id) {
        return (long) id; // TODO proper mapping to smaller array
    }

    public CoreGraphAlgorithms(Statement stmt) {
        this.stmt = stmt;
    }

    private void loadRels(ReadOperations ops, int labelId, int relTypeId) throws EntityNotFoundException {
        int allRelCount = (int) ops.relationshipsGetCount();
        this.relCount = (int) ops.countsForRelationshipWithoutTxState(labelId,relTypeId, ANY_LABEL);
        this.rels = new int[relCount];
        float percentage = (float) relCount / (float) allRelCount;
//        if (percentage > 0.5) {
//            this.relCount = loadRels(ops, ops.relationshipsGetAll(), relCount, relTypeId,rels);
//        }
//        else {
            PrimitiveLongIterator nodeIds = labelId == -1 ? ops.nodesGetAll() : ops.nodesGetForLabel(labelId);
            this.relCount = loadNodeRels(ops, nodeIds, relTypeId, rels);
//        }
    }

    private void loadNodes(ReadOperations ops, int labelId, int relTypeId) throws EntityNotFoundException {
        this.labelId = labelId;
        this.relTypeId = relTypeId;
        int allNodeCount = (int) ops.nodesGetCount();
        if (labelId == ANY_LABEL) {
            this.nodeCount = allNodeCount;
            this.nodeRelOffsets = loadNodes(ops, ops.nodesGetAll(), nodeCount, relTypeId, OUTGOING);
        } else {
            this.nodeCount = (int) ops.countsForNodeWithoutTxState(labelId);
            float percentage = (float)nodeCount / (float)allNodeCount;

            this.nodeRelOffsets = (percentage > 0.5f) ?
                    loadNodes(ops, ops.nodesGetAll(), labelId,    nodeCount, relTypeId) :
                    loadNodes(ops, ops.nodesGetForLabel(labelId), nodeCount, relTypeId, OUTGOING);
        }
    }

    public CoreGraphAlgorithms init(String label) throws EntityNotFoundException {
        ReadOperations ops = stmt.readOperations();

        int labelId = ops.labelGetForName(label);
        loadNodes(ops, labelId, ANY_RELATIONSHIP_TYPE);
        loadRels(ops, labelId, ANY_RELATIONSHIP_TYPE);
        return this;
    }

    // todo fix offset in node array while iterating over rels
    // only provide source label, no entries for dst node with diffent label in node array ?
    // store offsets in node, array (initialize with summed degrees to know where to put rels, fill rest with -1), compact later
    // degrees only for pageRank
    // optionally check target node label?
    // multiple rel-types
    // parallel loading (parallel degrees + serial offsets + parallel rels + serial compaction (flag if need to)
    // keep threads with open worker-tx for reads
    public CoreGraphAlgorithms init(String label, String rel) throws EntityNotFoundException {
        ReadOperations ops = stmt.readOperations();
        int labelId = ops.labelGetForName(label);
        int relTypeId = ops.relationshipTypeGetForName(rel);
        loadNodes(ops,labelId,relTypeId);
        loadRels(ops, labelId, relTypeId);
        return this;
    }

    public CoreGraphAlgorithms init() throws EntityNotFoundException {
        ReadOperations ops = stmt.readOperations();
        loadNodes(ops, ANY_LABEL, ANY_RELATIONSHIP_TYPE);
        loadRels(ops, ANY_LABEL,ANY_RELATIONSHIP_TYPE);
        return this;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getRelCount() {
        return relCount;
    }

    public int[] getNodeRelOffsets() {
        return nodeRelOffsets;
    }

    public int[] getRels() {
        return rels;
    }
}
