package apoc.algo;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.*;

import static apoc.util.TestUtil.assumeTravis;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 21.07.16
 */
public class PregelTest {

    public static final RelationshipType TYPE = RelationshipType.withName("FOO");

    @Before
    public void skipOnTravis() {
        assumeTravis();
    }

    @Test
    public void runDegreeProgram() throws Exception {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig(GraphDatabaseSettings.pagecache_memory,"100M").newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 100_001; i++) {
                Node n = db.createNode();
                int degree = i % 100 == 0 ? i / 100 : 0;
                for (int rel = 0; rel < degree; rel++) {
                    n.createRelationshipTo(n, TYPE);
                }
            }
            tx.success();
        }
        Pregel pregel = new Pregel(db).withBatchSize(1000);


        PrimitiveLongIterator nodes;
        long nodeCount;
        try (Transaction tx = db.beginTx()) {
            ReadOperations reads = pregel.statement().readOperations();
            nodes = reads.nodesGetAll();
            nodeCount = reads.nodesGetCount();
            tx.success();
        }
        long start = System.currentTimeMillis();
        int[] degrees = pregel.runProgram(nodes, new Pregel.AllExpander(), new OutDegrees(nodeCount));
        long time = System.currentTimeMillis() - start;
        System.err.println("Program ran for "+nodeCount+" nodes in "+time+" ms.");
        assertEquals(0, degrees[0]);
        assertEquals(1, degrees[100]);
        assertEquals(10, degrees[1000]);
        assertEquals(100, degrees[10000]);
        db.shutdown();
    }

    @Test
    public void runPageRank() throws Exception {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig(GraphDatabaseSettings.pagecache_memory,"100M").newGraphDatabase();
        int nodeCount = 100_001;
        int[] degrees = new int[nodeCount];

        createRankTestData(db, nodeCount, degrees);

        long start = System.currentTimeMillis();
        Pregel pregel = new Pregel(db).withBatchSize(10000);

        PrimitiveLongIterator nodes;
        try (Transaction tx = db.beginTx()) {
            ReadOperations reads = pregel.statement().readOperations();
            nodes = reads.nodesGetAll();
            tx.success();
        }


        float[] ranks = pregel.runProgram2(nodes, new Pregel.AllExpander(), new PageRankProgram(nodeCount, degrees, new float[nodeCount], new LinkedList<>()));
        long time = System.currentTimeMillis() - start;

        Arrays.sort(ranks);
        System.err.println("PageRank Program ran for "+nodeCount+" nodes in "+time+" ms. 10 hightest Ranks "+Arrays.toString(Arrays.copyOfRange(ranks,ranks.length-10,ranks.length)));
        db.shutdown();
    }

    @Test
    public void runPageRank3() throws Exception {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig(GraphDatabaseSettings.pagecache_memory,"100M").newGraphDatabase();
        int nodeCount = 100_001;
        int[] degrees = new int[nodeCount];

        createRankTestData(db, nodeCount, degrees);

        long start = System.currentTimeMillis();
        Pregel pregel = new Pregel(db).withBatchSize(10000);

        PrimitiveLongIterator nodes;
        try (Transaction tx = db.beginTx()) {
            nodes = pregel.statement().readOperations().nodesGetAll();
            tx.success();
        }


        float[] ranks = pregel.runProgram3(nodes, new Pregel.AllExpander(), new PageRankProgram(nodeCount, degrees, new float[nodeCount], null));
        long time = System.currentTimeMillis() - start;

        Arrays.sort(ranks);
        System.err.println("PageRank Program ran for "+nodeCount+" nodes in "+time+" ms. 10 hightest Ranks "+Arrays.toString(Arrays.copyOfRange(ranks,ranks.length-10,ranks.length)));
        db.shutdown();
    }

    private void createRankTestData(GraphDatabaseAPI db, int nodeCount, int[] degrees) {
        long start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < nodeCount; i++) {
                db.createNode();
            }
            tx.success();
        }
        Random random = new Random(42L);
        int rels = 0;
        try (Transaction tx = db.beginTx()) {
            for (int node = 0; node < nodeCount; node++) {
                Node n1 = db.getNodeById(node);
                int degree = random.nextInt(20);
                for (int j = 0; j < degree; j++) {
                    Node n2 = db.getNodeById(random.nextInt(nodeCount));
                    n1.createRelationshipTo(n2, TYPE);
                }
                degrees[node]=degree;
                rels += degree;
            }
            tx.success();
        }
        System.err.println("Generating Data took "+(System.currentTimeMillis() - start)+" ms. nodes "+nodeCount+ " rels "+rels);
    }

    private static class OutDegrees implements Pregel.PregelProgram<int[], int[]> {
        private final long nodeCount;

        public OutDegrees(long nodeCount) {
            this.nodeCount = nodeCount;
        }

        public boolean accept(long relId, long start, long end, int type, Statement stmt, int[] degrees) {
            degrees[(int) start]++;
            return true;
        }

        public int[] next(List<int[]> allDegrees) {
            if (allDegrees.isEmpty()) return state();

            Iterator<int[]> it = allDegrees.iterator();
            int[] result = it.next();
            int len = result.length;
            while (it.hasNext()) {
                int[] next = it.next();
                for (int i = 0; i < len; i++) {
                    result[i] += next[i];
                }
            }
            return result;
        }

        private int[] degrees;

        public int[] state() {
            if (degrees != null) return degrees;
            else return new int[(int) nodeCount];
        }

        @Override
        public Pregel.PregelProgram<int[], int[]> newInstance() {
            OutDegrees program = new OutDegrees(nodeCount);
            program.degrees = new int[(int) nodeCount];
            return program;
        }
    }

    @Test
    @Ignore
    public void testCreateGraph() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 10000; i++) {
                Node n = db.createNode();
                int degree = i / 100;
                for (int rel = 0; rel < degree; rel++) {
                    n.createRelationshipTo(n, TYPE);
                }
            }
            tx.success();
        } finally {
            db.shutdown();
        }
    }

    private static class PageRankProgram implements Pregel.PregelProgram<float[], float[]> {
        public static final float ALPHA = 0.15f;
        public static final float ONE_MINUS_ALPHA = 1f - ALPHA;

        final float[] src;
        private final int nodeCount;
        private final int[] degrees;
        int iteration;

        public PageRankProgram(int nodeCount, int[] degrees, float[] src, LinkedList<float[]> pool) {
            this.nodeCount = nodeCount;
            this.degrees = degrees;
            this.pool = pool;
            this.src = src;
            iteration = 20;
        }

        @Override
        public boolean accept(long relId, long start, long end, int type, Statement stmt, float[] dst) {
            dst[(int) end] += src[(int) start]; // todo global state?
            return true;
        }

        @Override
        public float[] next(List<float[]> ranks) {
            iteration--;
            Arrays.fill(src,0);
            // sum up individual results
            for (float[] rank : ranks) {
                for (int node = 0; node < nodeCount; node++) {
                    src[node] += rank[node];
                }
            }
            if (pool != null) pool.addAll(ranks);
            if (iteration == 0) {
                return src;
            }
            for (int node = 0; node < nodeCount; node++) {
                src[node] = (ALPHA * (ONE_MINUS_ALPHA + src[node])) / (float) degrees[node];
            }
            return null;
        }

        private float[] rank = null;
        @Override
        public Pregel.PregelProgram<float[], float[]> newInstance() {
            PageRankProgram program = new PageRankProgram(nodeCount, degrees, src, null);
            program.rank = new float[nodeCount];
            return program;
        }

        @Override
        public float[] state() {
            if (rank != null) {
                Arrays.fill(rank,0);
                return rank;
            }

            float[] result = null;
            if (pool != null) {
                synchronized (pool) {
                    if (!pool.isEmpty()) result = pool.removeFirst();
                }
            }
            if (result == null) return new float[nodeCount];
            Arrays.fill(result,0);
            return result;
        }
        private LinkedList<float[]> pool = null;
    }
    /*

org.neo4j.graphdb.TransactionFailureException: Transaction was marked as successful, but unable to commit transaction so rolled back.

	at org.neo4j.kernel.impl.coreapi.TopLevelTransaction.close(TopLevelTransaction.java:100)
	at apoc.algo.PregelTest.testCreateGraph(PregelTest.java:104)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:497)
	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
	at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)
	at org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)
	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)
	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)
	at org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)
	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)
	at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:117)
	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:42)
	at com.intellij.rt.execution.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:253)
	at com.intellij.rt.execution.junit.JUnitStarter.main(JUnitStarter.java:84)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:497)
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:147)
Caused by: org.neo4j.kernel.api.exceptions.TransactionFailureException: Could not apply the transaction to the store after written to log
	at org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess.applyToStore(TransactionRepresentationCommitProcess.java:82)
	at org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess.commit(TransactionRepresentationCommitProcess.java:51)
	at org.neo4j.kernel.impl.api.KernelTransactionImplementation.commit(KernelTransactionImplementation.java:470)
	at org.neo4j.kernel.impl.api.KernelTransactionImplementation.close(KernelTransactionImplementation.java:386)
	at org.neo4j.kernel.impl.coreapi.TopLevelTransaction.close(TopLevelTransaction.java:76)
	... 28 more
Caused by: org.neo4j.kernel.impl.store.UnderlyingStorageException: java.io.IOException: Exception in the page eviction thread
	at org.neo4j.kernel.impl.store.CommonAbstractStore.updateRecord(CommonAbstractStore.java:1088)
	at org.neo4j.kernel.impl.transaction.command.NeoStoreTransactionApplier.visitRelationshipCommand(NeoStoreTransactionApplier.java:82)
	at org.neo4j.kernel.impl.transaction.command.Command$RelationshipCommand.handle(Command.java:260)
	at org.neo4j.kernel.impl.api.TransactionApplierFacade.visit(TransactionApplierFacade.java:61)
	at org.neo4j.kernel.impl.api.TransactionApplierFacade.visit(TransactionApplierFacade.java:35)
	at org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation.accept(PhysicalTransactionRepresentation.java:69)
	at org.neo4j.kernel.impl.api.TransactionToApply.accept(TransactionToApply.java:108)
	at org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine.apply(RecordStorageEngine.java:334)
	at org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess.applyToStore(TransactionRepresentationCommitProcess.java:78)
	... 32 more
Caused by: java.io.IOException: Exception in the page eviction thread
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.assertHealthy(MuninnPageCache.java:574)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.grabFreeAndExclusivelyLockedPage(MuninnPageCache.java:628)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPagedFile.grabFreeAndExclusivelyLockedPage(MuninnPagedFile.java:493)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor.pageFault(MuninnPageCursor.java:312)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor.initiatePageFault(MuninnPageCursor.java:285)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor.uncommonPin(MuninnPageCursor.java:266)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor.pin(MuninnPageCursor.java:249)
	at org.neo4j.io.pagecache.impl.muninn.MuninnWritePageCursor.next(MuninnWritePageCursor.java:72)
	at org.neo4j.kernel.impl.store.CommonAbstractStore.updateRecord(CommonAbstractStore.java:1065)
	... 40 more
Caused by: java.io.IOException: java.lang.IllegalArgumentException: java.nio.DirectByteBuffer[pos=0 lim=6291456 cap=6291456], 6772800
	at org.neo4j.io.pagecache.impl.SingleFilePageSwapper.swapOut(SingleFilePageSwapper.java:278)
	at org.neo4j.io.pagecache.impl.SingleFilePageSwapper.write(SingleFilePageSwapper.java:435)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPage.doFlush(MuninnPage.java:154)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPage.flush(MuninnPage.java:142)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPage.evict(MuninnPage.java:209)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.evictPage(MuninnPageCache.java:895)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.evictPages(MuninnPageCache.java:850)
	at org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.continuouslySweepPages(MuninnPageCache.java:776)
	at org.neo4j.io.pagecache.impl.muninn.EvictionTask.run(EvictionTask.java:39)
	at org.neo4j.io.pagecache.impl.muninn.BackgroundTask.run(BackgroundTask.java:45)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	at java.lang.Thread.run(Thread.java:745)
Caused by: java.lang.IllegalArgumentException: java.nio.DirectByteBuffer[pos=0 lim=6291456 cap=6291456], 6772800
	at org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction$DynamicByteBuffer.put(EphemeralFileSystemAbstraction.java:1130)
	at org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction$EphemeralFileData.write(EphemeralFileSystemAbstraction.java:866)
	at org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction$EphemeralFileChannel.write(EphemeralFileSystemAbstraction.java:747)
	at org.neo4j.io.fs.StoreFileChannel.write(StoreFileChannel.java:50)
	at org.neo4j.io.fs.StoreFileChannel.writeAll(StoreFileChannel.java:65)
	at org.neo4j.io.pagecache.impl.SingleFilePageSwapper.swapOut(SingleFilePageSwapper.java:270)
	... 12 more
Caused by: java.lang.IllegalArgumentException
	at java.nio.Buffer.position(Buffer.java:244)
	at org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction$DynamicByteBuffer.put(EphemeralFileSystemAbstraction.java:1126)
	... 17 more

     */
}
