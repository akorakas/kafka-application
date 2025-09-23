package com.example.kafka.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.example.kafka.service.config.TransformProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class TemplateSyntaxStartupValidator implements ApplicationRunner {

  private static final ObjectMapper M = new ObjectMapper();

  private final TransformProperties props;
  private final boolean enabled;

  public TemplateSyntaxStartupValidator(
      TransformProperties props,
      @Value("${transform.validate-template-on-start:true}") boolean enabled) {
    this.props = props;
    this.enabled = enabled;
  }

  @Override
  public void run(ApplicationArguments args) {
    if (!enabled) return;

    if (props.getPipeline() == null || props.getPipeline().isEmpty()) {
      // Nothing to validate
      return;
    }

    int idx = 0;
    for (var step : props.getPipeline()) {
      idx++;
      if (!"template".equals(step.getType())) continue;

      String tpl = step.getTemplate();
      if (tpl == null || tpl.isBlank()) {
        throw new IllegalStateException("Startup validation failed: template step #" + idx + " has empty template.");
      }

      // Replace ${...} placeholders with a JSON-safe literal so we can parse structure.
      // Using "0" works for both quoted and unquoted positions.
      String renderedForCheck = tpl.replaceAll("\\$\\{[^}]+\\}", "0");

      try {
        M.readTree(renderedForCheck); // must parse as JSON
      } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
        String preview = renderedForCheck.length() > 400
            ? renderedForCheck.substring(0, 400) + "...(truncated)"
            : renderedForCheck;
        throw new IllegalStateException(
            "Startup validation failed: template step #" + idx + " is not valid JSON after placeholder substitution.\n"
          + "Preview:\n" + preview, ex);
      }
    }
  }
}
