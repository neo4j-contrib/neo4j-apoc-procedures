/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.algo;

import apoc.Extended;
import java.util.*;
import java.util.stream.Stream;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

/**
 * Batched cosine and euclidean similarity procedures for efficient vector
 * similarity computation across sets of nodes.
 *
 * <p>Accepts a list of nodes, a property name containing float[] embeddings, a
 * target embedding, and optional top-k and threshold parameters. Optimised for
 * batch use: the target embedding is converted and its magnitude pre-computed
 * once, all math operates on primitive float arrays without boxing, and a
 * bounded priority queue maintains the top-k results in O(N log K) time.</p>
 *
 * <p>Issue: <a href="https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/4447">#4447</a></p>
 */
@Extended
public class VectorSimilarity {

    @Context
    public Transaction tx;

    // =========================================================================
    // Result type
    // =========================================================================

    public static class SimilarityResult {
        /** The matched node. */
        public final Node node;
        /** Cosine similarity, euclidean distance, or euclidean similarity score. */
        public final double score;

        public SimilarityResult(Node node, double score) {
            this.node = node;
            this.score = score;
        }
    }

    // =========================================================================
    // Procedures
    // =========================================================================

    @Procedure(name = "apoc.algo.cosineSimilarity", mode = Mode.READ)
    @Description(
            "apoc.algo.cosineSimilarity(nodes, propertyName, targetEmbedding, topK, threshold) - "
                    + "Computes cosine similarity between targetEmbedding and the embedding stored on each "
                    + "node under propertyName. Returns results sorted by similarity descending. "
                    + "topK (0 = all) limits the result set; threshold (0.0 = disabled) filters nodes "
                    + "below the minimum similarity. Node properties may be float[], double[], or List<Number>.")
    public Stream<SimilarityResult> cosineSimilarity(
            @Name("nodes") List<Node> nodes,
            @Name("propertyName") String propertyName,
            @Name("targetEmbedding") List<Double> targetEmbedding,
            @Name(value = "topK", defaultValue = "0") long topK,
            @Name(value = "threshold", defaultValue = "0.0") double threshold) {
        return computeSimilarity(nodes, propertyName, targetEmbedding, topK, threshold, Metric.COSINE);
    }

    @Procedure(name = "apoc.algo.euclideanDistance", mode = Mode.READ)
    @Description(
            "apoc.algo.euclideanDistance(nodes, propertyName, targetEmbedding, topK, threshold) - "
                    + "Computes euclidean distance between targetEmbedding and the embedding stored on each "
                    + "node under propertyName. Returns results sorted by distance ascending (closest first). "
                    + "topK (0 = all) limits the result set; threshold (0.0 = disabled) filters nodes "
                    + "above the maximum distance.")
    public Stream<SimilarityResult> euclideanDistance(
            @Name("nodes") List<Node> nodes,
            @Name("propertyName") String propertyName,
            @Name("targetEmbedding") List<Double> targetEmbedding,
            @Name(value = "topK", defaultValue = "0") long topK,
            @Name(value = "threshold", defaultValue = "0.0") double threshold) {
        return computeSimilarity(nodes, propertyName, targetEmbedding, topK, threshold, Metric.EUCLIDEAN);
    }

    @Procedure(name = "apoc.algo.euclideanSimilarity", mode = Mode.READ)
    @Description(
            "apoc.algo.euclideanSimilarity(nodes, propertyName, targetEmbedding, topK, threshold) - "
                    + "Computes euclidean similarity (1 / (1 + distance)) between targetEmbedding and the "
                    + "embedding stored on each node under propertyName. Returns results sorted by "
                    + "similarity descending. topK (0 = all) limits the result set; threshold "
                    + "(0.0 = disabled) filters nodes below the minimum similarity.")
    public Stream<SimilarityResult> euclideanSimilarity(
            @Name("nodes") List<Node> nodes,
            @Name("propertyName") String propertyName,
            @Name("targetEmbedding") List<Double> targetEmbedding,
            @Name(value = "topK", defaultValue = "0") long topK,
            @Name(value = "threshold", defaultValue = "0.0") double threshold) {
        return computeSimilarity(nodes, propertyName, targetEmbedding, topK, threshold, Metric.EUCLIDEAN_SIM);
    }

    // =========================================================================
    // Core computation
    // =========================================================================

    private enum Metric {
        COSINE,
        EUCLIDEAN,
        EUCLIDEAN_SIM
    }

    private Stream<SimilarityResult> computeSimilarity(
            List<Node> nodes,
            String propertyName,
            List<Double> targetEmbedding,
            long topK,
            double threshold,
            Metric metric) {

        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        if (targetEmbedding == null || targetEmbedding.isEmpty()) {
            throw new IllegalArgumentException("targetEmbedding must not be null or empty");
        }

        final int dims = targetEmbedding.size();

        // Convert target once — avoids repeated boxing/unboxing inside the loop.
        final float[] target = toFloatArray(targetEmbedding, dims);

        // Pre-compute target magnitude for cosine (reused across all nodes).
        final float targetMag;
        if (metric == Metric.COSINE) {
            targetMag = magnitude(target);
            if (targetMag == 0f) {
                throw new IllegalArgumentException(
                        "targetEmbedding must not be a zero vector for cosine similarity");
            }
        } else {
            targetMag = 0f;
        }

        // Ascending order for distance (keep smallest); descending for similarity (keep largest).
        final boolean ascending = metric == Metric.EUCLIDEAN;
        final int k = (topK > 0) ? (int) topK : Integer.MAX_VALUE;

        // Min-heap keeps the best results: evicts the worst candidate when full.
        // For descending metrics (similarity): evict lowest score  → min-heap on score.
        // For ascending metrics (distance):    evict highest score → max-heap on score.
        final Comparator<SimilarityResult> heapOrder = ascending
                ? (a, b) -> Double.compare(b.score, a.score)   // max-heap: poll removes largest distance
                : (a, b) -> Double.compare(a.score, b.score);  // min-heap: poll removes smallest similarity

        final PriorityQueue<SimilarityResult> heap =
                new PriorityQueue<>(Math.min(k + 1, nodes.size() + 1), heapOrder);

        for (Node node : nodes) {
            final float[] emb = extractEmbedding(node, propertyName, dims);
            if (emb == null) continue;

            final double score;
            switch (metric) {
                case COSINE:
                    score = cosineSim(target, emb, targetMag);
                    break;
                case EUCLIDEAN:
                    score = euclideanDist(target, emb);
                    break;
                default: // EUCLIDEAN_SIM
                    score = 1.0 / (1.0 + euclideanDist(target, emb));
            }

            // Threshold filter
            if (threshold > 0.0) {
                if (ascending && score > threshold) continue;
                if (!ascending && score < threshold) continue;
            }

            heap.offer(new SimilarityResult(node, score));
            if (heap.size() > k) heap.poll(); // drop worst
        }

        // Drain heap into list; heap drains in worst-first order, so reverse.
        final List<SimilarityResult> results = new ArrayList<>(heap.size());
        while (!heap.isEmpty()) results.add(heap.poll());
        Collections.reverse(results);
        return results.stream();
    }

    // =========================================================================
    // Math — operates on primitive float[] to avoid per-element boxing
    // =========================================================================

    /**
     * Cosine similarity with a pre-computed target magnitude.
     * Single pass: dot product and candidate magnitude computed together.
     */
    static float cosineSim(float[] a, float[] b, float aMag) {
        float dot = 0f, bMagSq = 0f;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            bMagSq += b[i] * b[i];
        }
        final float bMag = (float) Math.sqrt(bMagSq);
        return bMag == 0f ? 0f : dot / (aMag * bMag);
    }

    /** Euclidean distance. Single pass, no intermediate allocations. */
    static float euclideanDist(float[] a, float[] b) {
        float sumSq = 0f;
        for (int i = 0; i < a.length; i++) {
            final float d = a[i] - b[i];
            sumSq += d * d;
        }
        return (float) Math.sqrt(sumSq);
    }

    /** L2 norm of a vector. */
    static float magnitude(float[] v) {
        float sq = 0f;
        for (float x : v) sq += x * x;
        return (float) Math.sqrt(sq);
    }

    // =========================================================================
    // Property extraction and conversion
    // =========================================================================

    /**
     * Extracts a float[] embedding from a node property.
     * Returns null if the property is absent, the wrong type, or has the wrong
     * number of dimensions (silently skipped so a single bad node does not abort
     * the whole batch).
     */
    static float[] extractEmbedding(Entity entity, String propertyName, int expectedDims) {
        if (!entity.hasProperty(propertyName)) return null;
        final Object val = entity.getProperty(propertyName);

        final float[] result;
        if (val instanceof float[]) {
            result = (float[]) val;
        } else if (val instanceof double[]) {
            final double[] d = (double[]) val;
            result = new float[d.length];
            for (int i = 0; i < d.length; i++) result[i] = (float) d[i];
        } else if (val instanceof List) {
            final List<?> list = (List<?>) val;
            result = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                final Object item = list.get(i);
                if (!(item instanceof Number)) return null;
                result[i] = ((Number) item).floatValue();
            }
        } else {
            return null;
        }

        return result.length == expectedDims ? result : null;
    }

    private static float[] toFloatArray(List<Double> list, int size) {
        final float[] arr = new float[size];
        for (int i = 0; i < size; i++) arr[i] = list.get(i).floatValue();
        return arr;
    }
}
