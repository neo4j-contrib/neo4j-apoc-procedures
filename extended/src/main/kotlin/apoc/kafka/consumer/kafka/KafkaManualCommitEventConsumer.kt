package apoc.kafka.consumer.kafka

import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.neo4j.logging.Log
import apoc.kafka.extensions.offsetAndMetadata
import apoc.kafka.extensions.topicPartition
import apoc.kafka.service.StreamsSinkEntity
import java.time.Duration

class KafkaManualCommitEventConsumer(config: KafkaSinkConfiguration,
                                     private val log: Log,
                                     topics: Set<String>,
                                     dbName: String): KafkaAutoCommitEventConsumer(config, log, topics, dbName) {

    private val asyncCommit = config.asyncCommit

    override fun stop() {
        if (asyncCommit) {
            doCommitSync()
        }
        super.stop()
    }

    private fun doCommitSync() {
        try {
            /*
             * While everything is fine, we use commitAsync.
             * It is faster, and if one commit fails, the next commit will serve as a retry.
             * But if we are closing, there is no "next commit". We call commitSync(),
             * because it will retry until it succeeds or suffers unrecoverable failure.
             */
            consumer.commitSync()
        } catch (e: WakeupException) {
            // we're shutting down, but finish the commit first and then
            // rethrow the exception so that the main loop can exit
            doCommitSync()
            throw e
        } catch (e: CommitFailedException) {
            // the commit failed with an unrecoverable error. if there is any
            // internal state which depended on the commit, you can clean it
            // up here. otherwise it's reasonable to ignore the error and go on
            log.warn("Commit failed", e)
        }
    }

    override fun start() {
        if (asyncCommit) {
            if (topics.isEmpty()) {
                log.info("No topics specified Kafka Consumer will not started")
                return
            }
            this.consumer.subscribe(topics, object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) = doCommitSync()

                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {}
            })
        } else {
            super.start()
        }
    }

    private fun commitData(commit: Boolean, topicMap: Map<TopicPartition, OffsetAndMetadata>) {
        if (commit && topicMap.isNotEmpty()) {
            if (asyncCommit) {
                if (log.isDebugEnabled) {
                    log.debug("Committing data in async")
                }
                consumer.commitAsync(topicMap) { offsets: MutableMap<TopicPartition, OffsetAndMetadata>, exception: Exception? ->
                    if (exception != null) {
                        log.warn("""
                            |These offsets `$offsets`
                            |cannot be committed because of the following exception:
                        """.trimMargin(), exception)
                    }
                }
            } else {
                if (log.isDebugEnabled) {
                    log.debug("Committing data in sync")
                }
                consumer.commitSync(topicMap)
            }
        }
    }

    override fun read(action: (String, List<StreamsSinkEntity>) -> Unit) {
        val topicMap = readSimple(action)
        commitData(true, topicMap)
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<StreamsSinkEntity>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        val topicMap = if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
        commitData(kafkaTopicConfig.commit, topicMap)
    }

    private fun readSimple(action: (String, List<StreamsSinkEntity>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        val records = consumer.poll(Duration.ZERO)
        return when (records.isEmpty) {
            true -> emptyMap()
            else -> records.partitions()
                    .map { topicPartition ->
                        val topicRecords = records.records(topicPartition)
                        executeAction(action, topicPartition.topic(), topicRecords)
                        val last = topicRecords.last()
                        last.topicPartition() to last.offsetAndMetadata()
                    }
                    .toMap()
        }
    }
}
