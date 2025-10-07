package com.example.kafka.boot;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;

import com.example.kafka.service.config.SinksProperties;

/**
 * Verifies at startup that all required Kafka topics already exist.
 * If any required topic is missing, the application fails fast.
 *
 * Also starts Kafka listeners only after successful verification.
 *
 * Recommended in application.yml:
 *   spring.kafka.listener.auto-startup: false
 */
@Configuration
public class RequireTopics {

  private static final Logger log = LoggerFactory.getLogger(RequireTopics.class);

  /** Build an AdminClient from Spring Boot's KafkaAdmin properties. */
  @Bean
  public AdminClient adminClient(KafkaAdmin admin) {
    return AdminClient.create(admin.getConfigurationProperties());
  }

  /**
   * On startup:
   *  1) Check cluster connectivity/auth
   *  2) List existing topics (without triggering auto-create)
   *  3) Compare with required topics; if any are missing -> throw
   *  4) Start listener containers on success
   */
  @Bean
  public ApplicationRunner verifyAllTopics(
      AdminClient admin,
      KafkaListenerEndpointRegistry registry,
      @Value("${app.kafka.input-topic}") String inputTopic,
      SinksProperties sinksProps,
      @Value("${app.kafka.verify-timeout-sec:10}") int verifyTimeoutSec
  ) {
    return (ApplicationArguments args) -> {
      // Touch args to satisfy IDE “unused lambda parameter” hint (only logs in DEBUG)
      if (log.isDebugEnabled() && args != null) {
        String[] src = args.getSourceArgs();
        log.debug("Application startup args count: {}", (src == null ? 0 : src.length));
      }

      // Collect required topics (trim, skip blanks). Only include sink topics if type == "kafka"
      Set<String> required = new LinkedHashSet<>();
      required.add(safe(inputTopic));

      if (isKafka(sinksProps.getOutput().getType())) {
        required.add(safe(sinksProps.getOutput().getTopic()));
      }
      if (isKafka(sinksProps.getDlt().getType())) {
        required.add(safe(sinksProps.getDlt().getTopic()));
      }
      if (isKafka(sinksProps.getError().getType())) {
        required.add(safe(sinksProps.getError().getTopic()));
      }

      required.removeIf(t -> t == null || t.isBlank());
      List<String> topics = new ArrayList<>(required);

      if (topics.isEmpty()) {
        throw new IllegalStateException(
            "No required topics configured. Check app.kafka.input-topic and app.sinks.*.topic");
      }

      log.info("Verifying required Kafka topics: {}", topics);

      // 1) Verify cluster reachability/auth
      try {
        admin.describeCluster().nodes().get(verifyTimeoutSec, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while contacting Kafka cluster.", ie);
      } catch (ExecutionException | TimeoutException e) {
        throw new IllegalStateException(
            "Cannot reach/authenticate to Kafka cluster. Check bootstrap.servers and security.*.", e);
      }

      // 2) List current topics (no auto-creation)
      final Set<String> existing;
      try {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(false);
        existing = admin.listTopics(options).names().get(verifyTimeoutSec, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while listing topics.", ie);
      } catch (ExecutionException | TimeoutException e) {
        throw new IllegalStateException("Failed to list topics via AdminClient (network/auth/timeout).", e);
      }

      // 3) Determine missing topics
      List<String> missing = topics.stream()
          .filter(t -> !existing.contains(t))
          .collect(Collectors.toList());

      if (!missing.isEmpty()) {
        String msg = "Missing required Kafka topic(s): " + String.join(", ", missing);
        log.error(msg);
        throw new IllegalStateException(msg);
      }

      log.info("All required Kafka topics are present.");

      // 4) Start Kafka listeners
      registry.start();
      log.info("Kafka listeners started after topic verification.");
    };
  }

  private static boolean isKafka(String type) {
    return type != null && "kafka".equalsIgnoreCase(type.trim());
  }

  private static String safe(String s) {
    return (s == null) ? null : s.trim();
  }
}
