package apoc.lock;

import org.neo4j.procedure.*;
import org.neo4j.graphdb.*;

import java.util.List;

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

    @Procedure(mode = Mode.READ, name = "apoc.lock.read.nodes")
    @Description("apoc.lock.read.nodes([nodes]) acquires a read lock on the given nodes")
    public void readLockOnNodes(@Name("nodes") List<Node> nodes) {
        try (Transaction tx = db.beginTx()) {
            for (Node node : nodes) {
                tx.acquireReadLock(node);
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

    @Procedure(mode = Mode.READ, name = "apoc.lock.read.rels")
    @Description("apoc.lock.read.rels([relationships]) acquires a read lock on the given relationship")
    public void readLocksOnRels(@Name("rels") List<Relationship> rels) {
        try (Transaction tx = db.beginTx()) {
            for (Relationship rel : rels) {
                tx.acquireReadLock(rel);
            }
            tx.success();
        }
    }
}
