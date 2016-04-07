package apoc.lock;

import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Lock {


    @Context
    public GraphDatabaseService db;

    @Procedure
    @PerformsWrites
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
    @Procedure
    @PerformsWrites
    public void nodes(@Name("nodes") List<Node> nodes) {
        try (Transaction tx = db.beginTx()) {
            for (Node node : nodes) {
                tx.acquireWriteLock(node);
            }
            tx.success();
        }
    }
    @Procedure
    @PerformsWrites
    public void rels(@Name("rels") List<Relationship> rels) {
        try (Transaction tx = db.beginTx()) {
            for (Relationship rel : rels) {
                tx.acquireWriteLock(rel);
            }
            tx.success();
        }
    }
}
