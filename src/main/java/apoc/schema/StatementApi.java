package apoc.schema;

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class StatementApi {

    public interface TxConsumer {
        void accept(KernelTransaction transaction) throws KernelException;
    }

    public interface TxFunction<T> {
        T apply(KernelTransaction transaction) throws KernelException;
    }


    protected final GraphDatabaseAPI api;
    private final TransactionWrapper tx;

    protected StatementApi(GraphDatabaseAPI api) {
        this.api = api;
        this.tx = new TransactionWrapper(api);
    }

    protected final <T> T applyInTransaction(TxFunction<T> fun) {
        return tx.apply(ktx -> {
            try {
                return fun.apply(ktx);
            } catch (KernelException e) {
                return ExceptionUtil.throwKernelException(e);
            }
        });
    }

    protected final void acceptInTransaction(TxConsumer fun) {
        tx.accept(ktx -> {
            try {
                fun.accept(ktx);
            } catch (KernelException e) {
                ExceptionUtil.throwKernelException(e);
            }
        });
    }
}


