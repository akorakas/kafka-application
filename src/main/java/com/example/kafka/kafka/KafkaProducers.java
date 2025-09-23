package com.example.kafka.kafka;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducers {

  private static final Logger log = LoggerFactory.getLogger(KafkaProducers.class);

  private final KafkaTemplate<String, Object> template;

  public KafkaProducers(KafkaTemplate<String, Object> template) {
    this.template = template;
  }

  /** Simple send using the configured serializers. */
  public void send(String topic, String key, Object value) {
    template.send(topic, key, value);
  }

  /** Send with source topic/partition/offset as headers (using a ConsumerRecord). */
  public void sendWithSourceMeta(String topic, Object value, ConsumerRecord<String, ?> src) {
    ProducerRecord<String, Object> rec =
        new ProducerRecord<>(topic, null, src.timestamp(), src.key(), value, null);

    rec.headers().add("source-topic", src.topic().getBytes(StandardCharsets.UTF_8));
    rec.headers().add("source-partition",
        Integer.toString(src.partition()).getBytes(StandardCharsets.UTF_8));
    rec.headers().add("source-offset",
        Long.toString(src.offset()).getBytes(StandardCharsets.UTF_8));

    template.send(rec);
    if (log.isDebugEnabled()) {
      log.debug("Produced to {} (src {}-{}@{})", topic, src.topic(), src.partition(), src.offset());
    }
  }

  /** Alternative overload if you don’t have the ConsumerRecord handy. */
  public void sendWithSourceMeta(String topic, String key, Object value,
                                 String srcTopic, Integer srcPartition, Long srcOffset) {
    ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, key, value);
    if (srcTopic != null) {
      rec.headers().add("source-topic", srcTopic.getBytes(StandardCharsets.UTF_8));
    }
    if (srcPartition != null) {
      rec.headers().add("source-partition",
          srcPartition.toString().getBytes(StandardCharsets.UTF_8));
    }
    if (srcOffset != null) {
      rec.headers().add("source-offset", srcOffset.toString().getBytes(StandardCharsets.UTF_8));
    }
    template.send(rec);
    if (log.isDebugEnabled()) {
      log.debug("Produced to {} (src {}-{}@{})", topic, srcTopic, srcPartition, srcOffset);
    }
  }
}
