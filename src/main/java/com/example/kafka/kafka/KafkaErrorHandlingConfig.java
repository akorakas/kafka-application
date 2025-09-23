package com.example.kafka.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.example.kafka.service.errors.BadInputException;
import com.example.kafka.service.errors.TransformFailureException;

@Configuration
public class KafkaErrorHandlingConfig {

  @Bean
  DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
      KafkaTemplate<Object, Object> template,
      @Value("${app.kafka.dlt-topic}") String dltTopic,
      @Value("${app.kafka.error-topic}") String errorTopic) {

    // Route by exception type
    return new DeadLetterPublishingRecoverer(template, (ConsumerRecord<?, ?> r, Exception e) -> {
      Throwable t = unwrap(e);
      if (t instanceof BadInputException) {
        return new TopicPartition(errorTopic, r.partition());     // invalid JSON -> errors topic
      }
      // default & pipeline failures
      return new TopicPartition(dltTopic, r.partition());         // transform failures -> DLT
    });
  }

  @Bean
  DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
    // Publish once, immediately
    var eh = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0));
    eh.setCommitRecovered(true); // advance offset after publishing to avoid duplicates

    // Never retry “known bad” exceptions
    eh.addNotRetryableExceptions(
        BadInputException.class,
        TransformFailureException.class
        // optionally: org.apache.kafka.common.errors.SerializationException.class
    );
    return eh;
  }

  private static Throwable unwrap(Throwable e) {
    // unwrap common wrappers
    while (e.getCause() != null
        && (e.getClass().getName().startsWith("org.springframework.")
         || e.getClass().getName().startsWith("java.util.concurrent"))) {
      e = e.getCause();
    }
    return e;
  }
}
