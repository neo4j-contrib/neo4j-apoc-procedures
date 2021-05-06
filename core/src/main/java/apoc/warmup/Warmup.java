package apoc.warmup;

import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class Warmup {

    @Context
    public Transaction tx;
    @Context
    public GraphDatabaseAPI db;
    @Context
    public TerminationGuard guard;
    @Context
    public Log log;

    static class PageResult {
        public final String file;
        public final boolean index;
        public final long fileSize;
        public final long pages;
        public final String error;
        public final long time;

        public PageResult(String file, boolean index, long fileSize, long pages, String error, long start) {
            this.file = file;
            this.index = index;
            this.fileSize = fileSize;
            this.pages = pages;
            this.error = error;
            this.time = System.currentTimeMillis() - start;
        }
    }

    private String subPath(File file, String fromParent) {
        StringBuilder sb = new StringBuilder(file.getAbsolutePath().length());
        while (true) {
            sb.insert(0,file.getName());
            file = file.getParentFile();
            if (file == null || file.getName().equals(fromParent)) break;
            sb.insert(0, File.separator);
        }
        return sb.toString();
    }

    @Procedure
    @Description("apoc.warmup.run(loadProperties=false,loadDynamicProperties=false,loadIndexes=false) - quickly loads all nodes and rels into memory by skipping one page at a time")
    public Stream<WarmupResult> run(@Name(value = "loadProperties", defaultValue = "false") boolean loadProperties, @Name(value = "loadDynamicProperties", defaultValue = "false") boolean loadDynamicProperties, @Name(value = "loadIndexes", defaultValue = "false") boolean loadIndexes) throws IOException {
        PageCache pageCache = db.getDependencyResolver().resolveDependency(PageCache.class);
        KernelTransaction ktx = ((InternalTransaction)tx).kernelTransaction();

        List<PagedFile> pagedFiles = pageCache.listExistingMappings();

        Map<String, PageResult> records = pagedFiles.parallelStream()
                .filter(pF -> {
                    String name = pF.path().toFile().getName();
                    if (isSchema(pF.path().toFile()) && !loadIndexes) return false;
                    if ((name.endsWith("propertystore.db.strings") || name.endsWith("propertystore.db.arrays")) && !loadDynamicProperties) return false;
                    if ((name.startsWith("propertystore.db")) && !loadProperties) return false;
                    return true;
                })
                .map((pagedFile -> {
                    File file = pagedFile.path().toFile();
                    boolean index = isSchema(file);
                    String fileName = index ? subPath(file, "schema") : file.getName();
                    long pages = 0;
                    long start = System.currentTimeMillis();
                    try {
                        if (pagedFile.fileSize() > 0) {
                            PageCursor cursor = pagedFile.io(0L, PagedFile.PF_READ_AHEAD | PagedFile.PF_SHARED_READ_LOCK,  ktx.cursorContext());
                            while (cursor.next()) {
                                cursor.getByte();
                                pages++;
                                if (pages % 1000 == 0 && Util.transactionIsTerminated(guard)) {
                                    break;
                                }
                            }
                        }
                        return new PageResult(fileName, index, pagedFile.fileSize(), pages, null, start);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return new PageResult(fileName, index, -1L, pages, e.getMessage(), start);
                    } 
                })).collect(Collectors.toMap(r -> r.file, r -> r));

        WarmupResult result = new WarmupResult(
                pageCache.pageSize(),
                Util.nodeCount(tx),
                records.get("neostore.nodestore.db"),
                Util.relCount(tx),
                records.get("neostore.relationshipstore.db"),
                records.get("neostore.relationshipgroupstore.db"),
                loadProperties,
                records.get("neostore.propertystore.db"),
                records.values().stream().mapToLong((r)->r.time).sum(),
                Util.transactionIsTerminated(guard),
                loadDynamicProperties,
                records.get("neostore.propertystore.db.strings"),
                records.get("neostore.propertystore.db.arrays"),
                loadIndexes,
                records.values().stream().filter(r -> r.index).collect(Collectors.toList())
                );
        return Stream.of(result);
    }

    public boolean isSchema(File file) {
        return file.getAbsolutePath().contains(File.separator+"schema"+File.separator);
    }

    public static class WarmupResult {
        public final long pageSize;
        public final long totalTime;
        public final boolean transactionWasTerminated;

        public long nodesPerPage;
        public final long nodesTotal;
        public final long nodePages;
        public final long nodesTime;

        public long relsPerPage;
        public final long relsTotal;
        public final long relPages;
        public final long relsTime;
        public long relGroupsPerPage;
        public long relGroupsTotal;
        public final long relGroupPages;
        public final long relGroupsTime;
        public final boolean propertiesLoaded;
        public final boolean dynamicPropertiesLoaded;
        public long propsPerPage;
        public long propRecordsTotal;
        public long propPages;
        public long propsTime;
        public long stringPropsPerPage;
        public long stringPropRecordsTotal;
        public long stringPropPages;
        public long stringPropsTime;
        public long arrayPropsPerPage;
        public long arrayPropRecordsTotal;
        public long arrayPropPages;
        public long arrayPropsTime;
        public final boolean indexesLoaded;
        public long indexPages;
        public long indexTime;

        public WarmupResult(long pageSize,
                            long nodesTotal,
                            PageResult nodes,
                            long relsTotal,
                            PageResult rels,
                            PageResult relGroups,
                            boolean propertiesLoaded,
                            PageResult props,
                            long totalTime, boolean transactionWasTerminated,
                            boolean dynamicPropertiesLoaded,
                            PageResult stringProps,
                            PageResult arrayProps,
                            boolean loadIndexes,
                            List<PageResult> indexes
                            ) {
            this.pageSize = pageSize;
            this.transactionWasTerminated = transactionWasTerminated;
            this.totalTime = totalTime;
            this.propertiesLoaded = propertiesLoaded;
            this.dynamicPropertiesLoaded = dynamicPropertiesLoaded;

            this.nodesTotal = nodesTotal;
            this.nodePages = nodes.pages;
            this.nodesTime = nodes.time;

            this.relsTotal = relsTotal;
            this.relPages = rels.pages;
            this.relsTime = rels.time;

            this.relGroupPages = relGroups.pages;
            this.relGroupsTime = relGroups.time;

            if (props!=null) {
                this.propPages = props.pages;
                this.propsTime = props.time;
            }
            if (stringProps != null) {
                this.stringPropPages = stringProps.pages;
                this.stringPropsTime = stringProps.time;
            }
            if (arrayProps != null) {
                this.arrayPropPages = arrayProps.pages;
                this.arrayPropsTime = arrayProps.time;
            }
            this.indexesLoaded = loadIndexes;
            if (!indexes.isEmpty()) {
                this.indexPages = indexes.stream().mapToLong(pr -> pr.pages).sum();
                this.indexTime = indexes.stream().mapToLong(pr -> pr.time).sum();
            }
        }
    }
}
