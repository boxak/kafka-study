package com.example;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleAdmin {

  private static final Logger logger = LoggerFactory.getLogger(SimpleAdmin.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties configs = new Properties();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");

    AdminClient admin = AdminClient.create(configs);

    //broker 정보 조회
    logger.info("== Get broker information");
    for (Node node : admin.describeCluster().nodes().get()) {
      logger.info("node : {}",node);
      ConfigResource cr = new ConfigResource(Type.BROKER, node.idString());
      DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
      describeConfigs.all().get().forEach((broker,config) -> {
        config.entries().forEach(
            configEntry -> logger.info(
                configEntry.name() + " = " + configEntry.value()
            )
        );
      });
    }

    //토픽 정보 조회
    logger.info("====== Topic Information");
    Map<String, TopicDescription> topicInformation = admin.describeTopics(
        Collections.singleton("test")
    ).all().get();
    logger.info("{}",topicInformation);

    admin.close();
  }
}
