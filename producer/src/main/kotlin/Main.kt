package org.example

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*

fun main() {
    val kafkaProps = Properties()
    kafkaProps["bootstrap.servers"] = "localhost:9092"
    kafkaProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    kafkaProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

    val producer = KafkaProducer<String, String>(kafkaProps)

    println("test")
}