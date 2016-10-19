package apoc.algo.algorithms;

/**
 * @author mh
 * @since 17.10.16
 */
public class Chunks {
    final int chunkSize;
    int[][] chunks;
    int numChunks = 0;

    public Chunks() {
        this(1, 100_000);
    }

    public Chunks(int numChunks, int chunkSize) {
        this.chunkSize = chunkSize;
        this.chunks = new int[numChunks][chunkSize];
        this.numChunks = numChunks;
    }

    void set(int index, int value) {
        int chunk = index / chunkSize;
        if (chunk >= numChunks) {
            growTo(chunk);
        }
        chunks[chunk][index % chunkSize] = value;
    }

    private void growTo(int chunk) {
        int newNumChunks = chunk + 1;
        int[][] newChunks = new int[newNumChunks][];
        System.arraycopy(chunks, 0, newChunks, 0, numChunks);
        for (int i = numChunks; i < newNumChunks; i++) {
            newChunks[i] = new int[chunkSize];
        }
        chunks = newChunks;
        numChunks = newNumChunks;
    }

    int get(int index) {
        int chunk = index / chunkSize;
        if (chunk >= numChunks) throw new ArrayIndexOutOfBoundsException(chunk);
        return chunks[chunk][index % chunkSize];
    }

}
