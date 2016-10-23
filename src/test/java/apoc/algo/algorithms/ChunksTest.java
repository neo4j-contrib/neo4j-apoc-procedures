package apoc.algo.algorithms;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.10.16
 */
public class ChunksTest {

    public static final int[] EMPTY = new int[0];

    @Test
    public void testWriteChunks() throws Exception {
        testChunking(3, 100);
        testChunking(3, 80);
        testChunking(8, 80);
        testChunking(8, 256);
    }

    public void testChunking(int chunkBits, int count) {
        int minChunks = 2;
        Chunks chunks = new Chunks(minChunks, chunkBits).withDefault(-1);
        int chunkSize = chunks.getChunkSize();
        String msg = String.format("Chunks: bits %d size %d count %d - ", chunkBits, chunkSize, count);
        int[] merged = new int[count];
        for (int i = 0; i< count; i++) {
            chunks.set(i,i);
            assertEquals(msg+"size", i+1,chunks.size());
            assertEquals(msg+"default", -1,chunks.get(i+1));
            merged[i]=i;
        }
        assertEquals(msg+"filled-chunks", (count / chunkSize), chunks.getFilledChunks());
        int expectedNumChunks = count / chunkSize + (int) Math.signum(count % chunkSize);
        assertEquals(msg+"chunks", Math.max(minChunks, expectedNumChunks),chunks.getNumChunks());

        for (int i = 0; i< count; i++) {
            assertEquals(msg+"get", i,chunks.get(i));
        }
        assertArrayEquals(msg+"merged",merged,chunks.mergeChunks());
        chunks.clear();
        assertEquals(0,chunks.size());
        assertEquals(0,chunks.getNumChunks());
        assertEquals(0,chunks.getFilledChunks());
        assertArrayEquals(EMPTY,chunks.mergeChunks());
    }
}
