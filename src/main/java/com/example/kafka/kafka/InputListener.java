// src/main/java/com/example/kafka/kafka/InputListener.java
package com.example.kafka.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafka.service.Transformer;
import com.example.kafka.sink.SinkRouter;

@Component
public class InputListener {

  private static final Logger log = LoggerFactory.getLogger(InputListener.class);

  private final SinkRouter sinks;
  private final Transformer transformer;

  public InputListener(SinkRouter sinks, Transformer transformer) {
    this.sinks = sinks;
    this.transformer = transformer;
  }

  @KafkaListener(
      topics = "${app.kafka.input-topic}",
      groupId = "${spring.kafka.consumer.group-id}"
  )
  public void onMessage(ConsumerRecord<String, String> record) {
    String key = record.key();            // may be null
    String value = record.value();

    if (log.isDebugEnabled()) {
      log.debug("Consumed {}-{}@{} key={}", record.topic(), record.partition(), record.offset(), key);
    }

    // Transform (may throw BadInputException / TransformFailureException → handled by DefaultErrorHandler)
    String outJson = transformer.transform(value);

    // Add provenance headers so you can see where this came from (also used by file sink)
    Map<String, String> headers = new HashMap<>();
    headers.put("source-topic", record.topic());
    headers.put("source-partition", String.valueOf(record.partition()));
    headers.put("source-offset", String.valueOf(record.offset()));

    // Route to whichever sink is configured for "output" (kafka topic or file)
    sinks.sendOutput(key, outJson, headers);
  }
}
