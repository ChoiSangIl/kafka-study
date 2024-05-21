package org.example

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.LocalDateTime
import java.util.*
import kotlin.random.Random
import kotlin.random.nextInt

object KafkaProducerProvider{
    val producer: KafkaProducer<String, String>

    init {
        val kafkaProps = Properties()
        kafkaProps["bootstrap.servers"] = "localhost:9092,localhost:9093,localhost:9094"
        kafkaProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        kafkaProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

        this.producer = KafkaProducer<String, String>(kafkaProps)
    }

}

object KafkaRecordHelper{
    fun createOrder(): ProducerRecord<String, String> = ProducerRecord("test", (1..10000).random().toString(), "{\"orderId\":\"${Random.nextInt(1..100000)}\",\"createdAt\":\"${LocalDateTime.now()}\"}")
}

class DemoProducerCallback: Callback{
    override fun onCompletion(recordMetadata: RecordMetadata, error: Exception?) {
        println("비동기 콜백 recordMetadata: $recordMetadata")
        error?.printStackTrace()
    }
}

fun main() {
    //동기적 전송
    /*
    try {
        KafkaProducerProvider.producer.send(KafkaRecordHelper.createOrder()).get()
    } catch (e: Exception) {
        e.printStackTrace()
    }

    //비동기 (콜백) 전송
    try {
        KafkaProducerProvider.producer.send(KafkaRecordHelper.createOrder(), DemoProducerCallback())
    } catch (e: Exception) {
        e.printStackTrace()
    }

     */

    while(true){
        try {
            KafkaProducerProvider.producer.send(KafkaRecordHelper.createOrder(), DemoProducerCallback())
            Thread.sleep((100..1000).random().toLong())
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}