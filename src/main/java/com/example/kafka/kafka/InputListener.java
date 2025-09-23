package com.example.kafka.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafka.service.Transformer;

@Component
public class InputListener {

  private static final Logger log = LoggerFactory.getLogger(InputListener.class);

  private final KafkaProducers producers;
  private final Transformer transformer;
  private final String outputTopic;

  public InputListener(KafkaProducers producers,
                       Transformer transformer,
                       @Value("${app.kafka.output-topic}") String outputTopic) {
    this.producers = producers;
    this.transformer = transformer;
    this.outputTopic = outputTopic;
  }

  @KafkaListener(topics = "${app.kafka.input-topic}", groupId = "${spring.kafka.consumer.group-id}")
  public void onMessage(ConsumerRecord<String, String> record) {
    String key = record.key();                  // may be null
    String value = record.value();

    if (log.isDebugEnabled()) {
      log.debug("Consumed {}-{}@{} key={}", record.topic(), record.partition(), record.offset(), key);
    }

    String outJson = transformer.transform(value);

    // produce to output with source metadata headers
    producers.sendWithSourceMeta(outputTopic, outJson, record);
  }
}
