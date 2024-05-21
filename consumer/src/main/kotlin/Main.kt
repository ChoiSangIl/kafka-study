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

    val consumer: KafkaConsumer<String, String> = KafkaConsumer(kafkaProps)

    consumer.subscribe(Collections.singleton("test"))

    val timeout = Duration.ofMillis(100)
    while (true){
        val records :ConsumerRecords<String, String> = consumer.poll(timeout)

        for (record in records){
            println("topic : ${record.topic()}, partition : ${record.partition()}, offset: ${record.offset()}, key: ${record.key()}, record : ${record.value()}")
        }
    }
}