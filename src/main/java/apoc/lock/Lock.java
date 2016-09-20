package apoc.lock;

import org.neo4j.procedure.*;
import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Lock {


    @Context
    public GraphDatabaseService db;

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.lock.all([nodes],[relationships]) acquires a write lock on the given nodes and relationships")
    public void all(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels) {
        try (Transaction tx = db.beginTx()) {
            for (Node node : nodes) {
                tx.acquireWriteLock(node);
            }
            for (Relationship rel : rels) {
                tx.acquireReadLock(rel);
            }
            tx.success();
        }
    }
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.lock.nodes([nodes]) acquires a write lock on the given nodes")
    public void nodes(@Name("nodes") List<Node> nodes) {
        try (Transaction tx = db.beginTx()) {
            for (Node node : nodes) {
                tx.acquireWriteLock(node);
            }
            tx.success();
        }
    }
    @Procedure(mode = Mode.WRITE)
    @Description("apoc.lock.rels([relationships]) acquires a write lock on the given relationship")
    public void rels(@Name("rels") List<Relationship> rels) {
        try (Transaction tx = db.beginTx()) {
            for (Relationship rel : rels) {
                tx.acquireWriteLock(rel);
            }
            tx.success();
        }
    }
}
