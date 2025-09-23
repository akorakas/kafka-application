package com.example.kafka.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import com.example.kafka.service.config.TransformProperties;
import com.example.kafka.service.errors.BadInputException;
import com.example.kafka.service.errors.TransformFailureException;
import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;
import com.example.kafka.service.pipeline.steps.ExtractStep;
import com.example.kafka.service.pipeline.steps.FlattenStep;
import com.example.kafka.service.pipeline.steps.HashStep;
import com.example.kafka.service.pipeline.steps.RegexExtractStep;
import com.example.kafka.service.pipeline.steps.TemplateStep;
import com.example.kafka.service.pipeline.steps.UpdateStep;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Runs the configured transform pipeline over an input JSON string.
 * - Invalid input JSON -> BadInputException (route to errors topic)
 * - Step/pipeline failures -> TransformFailureException (route to DLT)
 */
@Service
@EnableConfigurationProperties(TransformProperties.class)
public class Transformer {

  private static final ObjectMapper M = new ObjectMapper();
  private final List<TransformStep> steps;

  public Transformer(TransformProperties props) {
    this.steps = buildSteps(props);
  }

  /** Execute the pipeline; return the final rendered JSON (or original input if no template step). */
  public String transform(String inputJson) {
    try {
      var root = M.readTree(inputJson);           // invalid JSON -> JsonProcessingException
      var ctx  = new TransformContext(root);

      for (var s : steps) {
        try {
          s.apply(ctx);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
          // Any JSON building/parsing done inside a step failed -> transform failure
          throw new TransformFailureException("Step JSON processing failed", e);
        } catch (java.io.IOException e) {
          // Any IO from a step (if any)
          throw new TransformFailureException("Step IO failed", e);
        } catch (IllegalArgumentException | IllegalStateException e) {
          // Business validation from steps
          throw new TransformFailureException(e.getMessage(), e);
        } catch (Exception e) {
          // Satisfy checked signature of TransformStep.apply(...)
          throw new TransformFailureException("Step failed", e);
        }
      }

      return (ctx.rendered != null) ? ctx.rendered : inputJson;

    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      // Input payload is not valid JSON
      throw new BadInputException("Input is not valid JSON", e);
    }
  }

  /** Build the pipeline from YAML properties. */
  private static List<TransformStep> buildSteps(TransformProperties props) {
    var out = new ArrayList<TransformStep>();
    var placeholder = props.getPlaceholder();

    for (var s : props.getPipeline()) {
      switch (s.getType()) {
        case "extract" -> out.add(new ExtractStep(
            s.getMappings(),
            s.getFromVar(),
            Boolean.TRUE.equals(s.getFailOnMissing()),
            Boolean.TRUE.equals(s.getFailOnBadJson())
        ));
        case "update" -> out.add(new UpdateStep(
            s.getStripCr(),
            s.getCompute(),
            placeholder
        ));
        case "regexExtract" -> {
          final int grp = Objects.requireNonNullElse(s.getGroup(), 1);
          out.add(new RegexExtractStep(
              s.getSource(),
              s.getPattern(),
              grp,
              s.getTarget(),
              s.getFallback()
          ));
        }
        case "flatten"  -> out.add(new FlattenStep(s.getRoots(), s.getIncludeTop(), s.getTarget()));
        case "hash"     -> out.add(new HashStep(s.getAlgorithm(), s.getFields(), s.getTarget()));
        case "template" -> out.add(new TemplateStep(s.getTemplate(), s.getTarget()));
        default -> throw new IllegalArgumentException("Unknown step type: " + s.getType());
      }
    }
    return out;
  }
}
