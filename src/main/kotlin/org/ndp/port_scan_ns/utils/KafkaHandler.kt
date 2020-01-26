package org.ndp.port_scan_ns.utils

import com.squareup.moshi.JsonAdapter
import com.squareup.moshi.Moshi
import com.squareup.moshi.Types
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.ndp.port_scan_ns.Log
import org.ndp.port_scan_ns.bean.KafkaResult
import org.ndp.port_scan_ns.bean.KafkaTask
import java.time.Duration
import java.util.*

object KafkaHandler {
    private val producer: KafkaProducer<String, String>
    private val consumer: KafkaConsumer<String, String>
    private val kafkaTaskAdapter: JsonAdapter<KafkaTask>
    private val kafkaResultAdapter: JsonAdapter<KafkaResult>

    init {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] =
            Settings.setting["BOOTSTRAP_SERVERS_CONFIG"] as String
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            Settings.setting["KEY_SERIALIZER_CLASS_CONFIG"] as String
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            Settings.setting["VALUE_SERIALIZER_CLASS_CONFIG"] as String
        producer = KafkaProducer(producerProps)

        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] =
            Settings.setting["BOOTSTRAP_SERVERS_CONFIG"] as String
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] =
            Settings.setting["GROUP_ID_CONFIG"] as String
        consumerProps[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] =
            Settings.setting["SESSION_TIMEOUT_MS_CONFIG"] as String
        consumerProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] =
            Settings.setting["MAX_POLL_RECORDS_CONFIG"] as String
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            Settings.setting["KEY_DESERIALIZER_CLASS_CONFIG"] as String
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            Settings.setting["VALUE_DESERIALIZER_CLASS_CONFIG"] as String
        consumer = KafkaConsumer(consumerProps)
        consumer.subscribe(arrayListOf(Settings.setting["topic.task"] as String))

        val moshi = Moshi.Builder().add(KotlinJsonAdapterFactory()).build()
        kafkaTaskAdapter = moshi.adapter(KafkaTask::class.java)
        kafkaResultAdapter = moshi.adapter(KafkaResult::class.java)
    }

    fun consumeTaskParam(): KafkaTask {
        val msgList = consumer.poll(Duration.ofSeconds(1))
        var param = KafkaTask(0, "", "")
        Log.debug("kafka task: ${msgList.count()}")
        if (!msgList.isEmpty) {
            for (record in msgList.records(Settings.setting["topic.task"] as String)) {
                param = kafkaTaskAdapter.fromJson(record.value().toString())!!
            }
        }
        return param
    }

    fun produceResult(result: KafkaResult) {
        val value = ProducerRecord<String, String>(
            Settings.setting["topic.result"] as String,
            kafkaResultAdapter.toJson(result)
        )
        producer.send(value)
    }
}