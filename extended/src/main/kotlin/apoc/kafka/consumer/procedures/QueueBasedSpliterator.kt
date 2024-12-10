package apoc.kafka.consumer.procedures

import org.neo4j.graphdb.NotInTransactionException
import org.neo4j.graphdb.TransactionTerminatedException
import org.neo4j.procedure.TerminationGuard
import java.util.Spliterator
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * @author mh
 * @since 08.05.16 in APOC
 */
class QueueBasedSpliterator<T> constructor(private val queue: BlockingQueue<T>,
                                           private val tombstone: T,
                                           private val terminationGuard: TerminationGuard,
                                           private val timeout: Long = 10) : Spliterator<T?> {
    private var entry: T?

    init {
        entry = poll()
    }

    override fun tryAdvance(action: Consumer<in T?>): Boolean {
        if (transactionIsTerminated(terminationGuard)) return false
        if (isEnd) return false
        action.accept(entry)
        entry = poll()
        return !isEnd
    }

    private fun transactionIsTerminated(terminationGuard: TerminationGuard): Boolean {
        return try {
            terminationGuard.check()
            false
        } catch (e: Exception) {
            when (e) {
                is TransactionTerminatedException, is NotInTransactionException -> true
                else -> throw e
            }
        }
    }

    private val isEnd: Boolean
        private get() = entry == null || entry === tombstone

    private fun poll(): T? {
        return try {
            queue.poll(timeout, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            null
        }
    }

    override fun trySplit(): Spliterator<T?>? {
        return null
    }

    override fun estimateSize(): Long {
        return Long.MAX_VALUE
    }

    override fun characteristics(): Int {
        return Spliterator.NONNULL
    }
}