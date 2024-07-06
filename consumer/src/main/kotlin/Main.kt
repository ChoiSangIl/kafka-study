package org.example

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*

val currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata> = mutableMapOf()

fun main() {
    val kafkaProps = Properties()
    kafkaProps["bootstrap.servers"] = "localhost:9092,localhost:9093,localhost:9094"
    kafkaProps["group.id"] = "BART_GROUP1"
    kafkaProps["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    kafkaProps["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

    val consumer: KafkaConsumer<String, String> = KafkaConsumer(kafkaProps)
    consumer.subscribe(Collections.singleton("test"), HandleRebalance())

    val timeout = Duration.ofMillis(100)
    while (true){
        val records :ConsumerRecords<String, String> = consumer.poll(timeout)

        for (record in records){
            println("topic : ${record.topic()}, partition : ${record.partition()}, offset: ${record.offset()}, key: ${record.key()}, record : ${record.value()}")
            currentOffsets[TopicPartition(record.topic(), record.partition())] = OffsetAndMetadata(record.offset()+1, null)
        }

        // 동기적 커밋, 비동기적 커밋
        /*
        try {
            consumer.commitSync()
        } catch (exception: CommitFailedException) {
            exception.printStackTrace()
        }
        consumer.commitAsync(
         */
    }
}

class HandleRebalance : ConsumerRebalanceListener{
    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
        println("call onPartitionsRevoked")
        println("currentOffsets: $currentOffsets")

    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        println("call onPartitionsAssigned")
        println("Lost partitions in rebalance. $partitions")
    }
}