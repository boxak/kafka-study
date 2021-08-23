package com.example.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class ProducerApp {
  public static void main(String[] args) {
    HashMap<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    try {
      String msg = "";
      while(!"stop".equals(msg)) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        msg = br.readLine();
        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", null, msg);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        describeMetadata(metadata);
      }
    } catch (InterruptedException | ExecutionException | IOException e) {
      e.printStackTrace();
    }

  }

  private static void describeMetadata(RecordMetadata metadata) {
    log.info("=== Metadata...");
    log.info("topic : " + metadata.topic());
    log.info("partition : " + metadata.partition());
    log.info("offset : " + metadata.offset());
    log.info("timestamp : " + metadata.timestamp());
  }
}
