package apoc.algo.algorithms;

import java.util.Arrays;
import java.util.function.IntConsumer;

/*
Ideas for improvements

* use a pool for chunks that are returned on clear
* only use one BASE_CHUNK per default value globally
* increase chunk-size when merging chunks
*
*/

/**
 * a "chunked" growable int-array that can have gaps and grows dynamically,
 * meant to be used from a single thread, so no guards or volatiles
 * @author mh
 * @since 17.10.16
 */
public class Chunks {
    private int[] baseChunk;
    public static final int CHUNK_BITS = 16;
    private final int chunkSize;
    private final int chunkBits;
    private final int mask;
    int[][] chunks;
    int numChunks = 0;
    int maxIndex = 0;
    private int defaultValue = 0;

    public Chunks() {
        this(1, CHUNK_BITS);
    }

    public Chunks(int capacity) {
        this((capacity >> CHUNK_BITS ) +1,CHUNK_BITS);
    }
    Chunks(int numChunks, int chunkBits) {
        this.chunkSize = 1 << chunkBits;
        this.chunkBits = chunkBits;
        this.mask = chunkSize - 1;
        this.chunks = new int[numChunks][chunkSize];
        this.numChunks = numChunks;
    }

    // call directly after construction, todo move to constructor or static factory
    public Chunks withDefault(int defaultValue) {
        this.defaultValue = defaultValue;
        if (defaultValue != 0) {
            for (int[] chunk : chunks) {
                Arrays.fill(chunk, defaultValue);
            }
        }
        return this;
    }

    /**
     * Sets a value an grows chunks dynamically if exceeding size or missing chunk encountered
     */
    void set(int index, int value) {
        int chunk = assertSpace(index);
        chunks[chunk][index & mask] = value;
    }

    public void increment(int index) {
        int chunk = assertSpace(index);
        chunks[chunk][index & mask]++;
    }

    private int assertSpace(int index) {
        int chunk = index >> chunkBits;
        if (index > maxIndex) {
            maxIndex = index;
            if (chunk >= numChunks) growTo(chunk);
        }
        if (chunks[chunk]==null) growTo(chunk);
        return chunk;
    }

    int get(int index) {
        if (index>maxIndex) return defaultValue;
        int chunk = index >> chunkBits;
        if (chunks[chunk]==null) return defaultValue;
        return chunks[chunk][index & mask];
    }

    public int getNumChunks() {
        return numChunks;
    }

    public int size() {
        return maxIndex+1;
    }

    public int getFilledChunks() {
        return size() / chunkSize;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    int[][] getChunks() {
        return chunks;
    }

    private void growTo(int chunk) {
        int newNumChunks = Math.max(chunk + 1,numChunks);
        if (newNumChunks!=numChunks) {
            int[][] newChunks = new int[newNumChunks][];
            System.arraycopy(chunks, 0, newChunks, 0, numChunks);
            chunks = newChunks;
            numChunks = newNumChunks;
        }
        if (chunks[chunk]==null) {
            chunks[chunk] = newChunk();
        }
    }

    // todo use pool
    private int[] newChunk() {
        if (defaultValue==0) return new int[chunkSize];

        if (baseChunk==null) {
            baseChunk = new int[chunkSize];
            Arrays.fill(baseChunk,defaultValue);
        }
        return baseChunk.clone();
    }

    /**
     * special operation implemented inline to compute and store sum up until here
     */
    public void sumUp() {
        int offset=0;
        int tmp=0;
        for (int i = 0; i < numChunks; i++) {
            int[] chunk = chunks[i];
            if (chunk==null) throw new IllegalStateException("Chunks are not continous, null fragement at offset "+i);
            for (int j = 0; j < chunkSize; j++) {
                tmp = chunk[j];
                chunk[j] = offset;
                offset += tmp;
            }
        }
    }

    /**
     * adds values from another Chunks to the current one
     * missing chunks are cloned over
     * @param c other Chunks
     */
    public void add(Chunks c) {
        assert c.chunkBits == chunkBits;
        assert c.defaultValue == defaultValue;

        int[][] oChunks = c.chunks;
        if (c.numChunks > numChunks) growTo(c.numChunks-1);
        for (int i = 0; i < oChunks.length; i++) {
            int[] oChunk = oChunks[i];
            if (oChunk!=null) {
                if (chunks[i]==null) {
                    chunks[i]=oChunk.clone();
                } else {
                    int[] chunk = chunks[i];
                    for (int j = 0; j < oChunk.length; j++) {
                        chunk[j] += oChunk[j];
                    }
                }
            }
        }
        this.maxIndex = Math.max(this.maxIndex,c.maxIndex);
    }

    /**
     * Clones this chunk by copying over all internal arrays
     */
    public Chunks clone() {
        Chunks result = new Chunks(0, chunkBits);
        int[][] newChunks = new int[numChunks][];
        for (int i = 0; i < numChunks; i++) {
            if (this.chunks[i]!=null) {
                newChunks[i]=this.chunks[i].clone();
            }
        }
        result.defaultValue = defaultValue;
        result.chunks = newChunks;
        result.maxIndex = maxIndex;
        result.numChunks = numChunks;
        return result;
    }

    /**
     * turn this chunked array into a regular int-array, mostly for compatibility
     * also copying unused space at the end filled with default value
     */
    public int[] mergeAllChunks() {
        int[] merged = new int[chunkSize*numChunks];
        int filledChunks = Math.min(numChunks,getFilledChunks()+1);
        for (int i = 0; i < filledChunks ; i++) {
            if (chunks[i]!=null) {
                System.arraycopy(chunks[i],0,merged,i*chunkSize,chunkSize);
            }
        }
        if (defaultValue!=0) Arrays.fill(merged, size(),merged.length,defaultValue);
        return merged;
    }

    /**
     * turn this chunked array into a regular int-array, mostly for compatibility
     * cuts off unused space at the end
     */
    public int[] mergeChunks() {
        int filledChunks = getFilledChunks();
        int[] merged = new int[size()];
        for (int i = 0; i < filledChunks ; i++) {
            if (chunks[i]!=null) {
                System.arraycopy(chunks[i], 0, merged, i * chunkSize, chunkSize);
            }
        }
        int remainder = size() % chunkSize;
        if (remainder != 0 && chunks[filledChunks]!=null) {
            System.arraycopy(chunks[filledChunks], 0, merged, (filledChunks) * chunkSize, remainder);
        }
        return merged;
    }

    /**
     * free resources (arrays) associated with this chunk
     */
    // todo return chunks to pool
    public void clear() {
        this.numChunks=0;
        this.maxIndex=-1;
        this.chunks=new int[0][];
    }

    interface IndexValueConsumer {
        void accept(int index, int value);
    }

    /**
     * internal iteration over data, avoids checks in {@link #get(int)}
     * @return total size of this Chunks
     */
    public int consume(IndexValueConsumer consumer) {
        int filledChunks = getFilledChunks();
        int offset;
        for (int i = 0; i < filledChunks ; i++) {
            offset = i * chunkSize;
            int[] chunk = chunks[i];
            if (chunk!=null) {
                for (int j = 0; j < chunkSize; j++) {
                    consumer.accept(offset+j, chunk[i]);
                }
            }
        }
        int remainder = size() % chunkSize;
        if (remainder != 0) {
            offset = filledChunks * chunkSize;
            int[] chunk = chunks[filledChunks];
            for (int j = 0; j < remainder; j++) {
                consumer.accept(offset+j, chunk[j]);
            }
        }
        return size();
    }
}
