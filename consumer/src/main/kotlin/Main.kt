package org.example

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

fun main() {
    val kafkaProps = Properties()
    kafkaProps["bootstrap.servers"] = "localhost:9092,localhost:9093,localhost:9094"
    kafkaProps["group.id"] = "BART_GROUP1"
    kafkaProps["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    kafkaProps["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
    // kafkaProps["partition.assignment.strategy"] = "org.apache.kafka.clients.consumer.RangeAssignor" 리밸런스 전략

    val consumer: KafkaConsumer<String, String> = KafkaConsumer(kafkaProps)

    consumer.subscribe(Collections.singleton("createdOrder"))

    val timeout = Duration.ofMillis(100)
    while (true){
        val records :ConsumerRecords<String, String> = consumer.poll(timeout)

        for (record in records){
            println("${record.topic()}, ${record.partition()}, ${record.offset()}, ${record.key()}, ${record.value()}")
        }
    }
}