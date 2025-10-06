package com.example.kafka.boot;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;

import com.example.kafka.service.config.SinksProperties;

/**
 * Verifies at startup that all required Kafka topics already exist.
 * If any topic is missing, the application fails fast with an IllegalStateException.
 *
 * Required topics:
 *  - app.kafka.input-topic
 *  - app.sinks.output.topic (only if app.sinks.output.type == "kafka")
 *  - app.sinks.dlt.topic    (only if app.sinks.dlt.type    == "kafka")
 *  - app.sinks.error.topic  (only if app.sinks.error.type  == "kafka")
 *
 * TIP: In application.yml also set:
 *   spring.kafka.listener.auto-startup: false
 * so consumers don't subscribe before verification; this class will start them after success.
 */
@Configuration
public class RequireTopics {

  private static final Logger log = LoggerFactory.getLogger(RequireTopics.class);

  /** Reuse Spring Boot's KafkaAdmin config to create an AdminClient. */
  @Bean
  @SuppressWarnings("unused") // used by Spring, not referenced directly
  AdminClient adminClient(KafkaAdmin admin) {
    return AdminClient.create(admin.getConfigurationProperties());
  }

  /**
   * On application startup:
   *  1) Check cluster connectivity/auth
   *  2) List existing topics (no auto-creation)
   *  3) Compare with required; if any missing -> throw and abort
   *  4) Start Kafka listeners after successful verification
   */
  @Bean
  @SuppressWarnings("unused") // used by Spring, not referenced directly
  ApplicationRunner verifyAllTopics(
      AdminClient admin,
      KafkaListenerEndpointRegistry registry,
      @Value("${app.kafka.input-topic}") String inputTopic,
      SinksProperties sinksProps,
      @Value("${app.kafka.verify-timeout-sec:10}") int verifyTimeoutSec
  ) {
    return args -> {
      // Build the set of required topics (non-blank; only for "kafka" sinks)
      Set<String> required = new LinkedHashSet<>();
      required.add(safe(inputTopic));

      if (isKafka(sinksProps.getOutput().getType())) required.add(safe(sinksProps.getOutput().getTopic()));
      if (isKafka(sinksProps.getDlt().getType()))    required.add(safe(sinksProps.getDlt().getTopic()));
      if (isKafka(sinksProps.getError().getType()))  required.add(safe(sinksProps.getError().getTopic()));

      required.removeIf(t -> t == null || t.isBlank());
      List<String> topics = new ArrayList<>(required);

      if (topics.isEmpty()) {
        throw new IllegalStateException(
            "No required topics configured. Check app.kafka.input-topic and app.sinks.*.topic");
      }

      log.info("Verifying required Kafka topics: {}", topics);

      // 1) Connectivity/auth check — fail clearly if we can't reach the cluster
      try {
        admin.describeCluster().nodes().get(verifyTimeoutSec, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Cannot reach/authenticate to Kafka cluster. " +
            "Verify bootstrap.servers and security.* client properties.", e);
      }

      // 2) List current topics (no auto-creation side effects)
      final Set<String> existing;
      try {
        var options = new ListTopicsOptions().listInternal(false);
        existing = admin.listTopics(options).names().get(verifyTimeoutSec, TimeUnit.SECONDS);
      } catch (java.util.concurrent.TimeoutException te) {
        throw new IllegalStateException(
            "Timed out listing topics. Likely authentication or network issue. " +
            "Ensure AdminClient security settings match the broker.", te);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to list topics via AdminClient.", e);
      }

      // 3) Compare and fail if any required topics are missing
      var missing = topics.stream()
          .filter(t -> !existing.contains(t))
          .collect(Collectors.toList());

      if (!missing.isEmpty()) {
        String msg = "Missing required Kafka topic(s): " + String.join(", ", missing);
        log.error(msg);
        throw new IllegalStateException(msg);
      }

      log.info("All required Kafka topics are present.");

      // 4) Start Kafka listeners after successful verification
      try {
        registry.start();
        log.info("Kafka listeners started after topic verification.");
      } catch (Exception e) {
        throw new IllegalStateException("Failed to start Kafka listeners after verification.", e);
      }
    };
  }

  private static boolean isKafka(String type) {
    return type != null && "kafka".equalsIgnoreCase(type.trim());
  }

  private static String safe(String s) {
    return s == null ? null : s.trim();
  }
}
