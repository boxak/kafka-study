package com.example;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer2 {
  private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer2.class);
  private final static String TOPIC_NAME = "test";
  private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
  private final static String GROUP_ID = "test-group";
  private static KafkaConsumer<String, String> consumer;
  private static Map<TopicPartition, OffsetAndMetadata> currentOffset;

  public static void main(String[] args) {
    Properties configs = new Properties();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    consumer = new KafkaConsumer<String, String>(configs);
    consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());

    while(true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
      currentOffset = new HashMap<>();
      for (ConsumerRecord<String, String> record : records) {
        logger.info("{}", record);
        currentOffset.put(new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset()+1, null));
        consumer.commitSync(currentOffset);
      }
    }

  }

  private static class RebalanceListener implements ConsumerRebalanceListener {
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      logger.warn("Partitions are assigned");
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      logger.warn("Partitions are revoked");
      consumer.commitSync(currentOffset);
    }
  }
}
