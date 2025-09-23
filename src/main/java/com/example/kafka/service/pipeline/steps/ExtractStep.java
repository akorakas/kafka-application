package com.example.kafka.service.pipeline.steps;

import java.util.Map;

import com.example.kafka.service.errors.TransformFailureException;
import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extracts values from JSON into the pipeline context.
 * - If fromVar is null -> read from ctx.root.
 * - If fromVar is set   -> read from ctx.vars[fromVar] (expected JSON string).
 * Supports both JsonPointer ("/a/b") and dotted ("a.b") paths.
 * Optional strictness:
 *  - failOnMissing: throw if any mapped path is missing/null/blank
 *  - failOnBadJson: throw if fromVar is a non-JSON string
 */
public class ExtractStep implements TransformStep {

  private static final ObjectMapper M = new ObjectMapper();

  private final Map<String, String> mappings;
  private final String fromVar;         // may be null
  private final boolean failOnMissing;
  private final boolean failOnBadJson;

  public ExtractStep(Map<String, String> mappings, String fromVar) {
    this(mappings, fromVar, false, false);
  }

  public ExtractStep(Map<String, String> mappings, String fromVar,
                     boolean failOnMissing, boolean failOnBadJson) {
    this.mappings = mappings;
    this.fromVar = fromVar;
    this.failOnMissing = failOnMissing;
    this.failOnBadJson = failOnBadJson;
  }

  @Override
  public void apply(TransformContext ctx) throws Exception {
    JsonNode source;

    if (fromVar == null) {
      source = ctx.root;
    } else {
      Object v = ctx.get(fromVar);
      if (v instanceof String s && !s.isBlank()) {
        try {
          source = M.readTree(s);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
          if (failOnBadJson) {
            throw new TransformFailureException("ExtractStep: fromVar '" + fromVar + "' is not valid JSON", e);
          }
          source = M.nullNode();
        }
      } else {
        if (failOnBadJson && v != null) {
          throw new TransformFailureException("ExtractStep: fromVar '" + fromVar + "' is not a JSON string");
        }
        source = M.nullNode();
      }
    }

    for (var e : mappings.entrySet()) {
      String outKey = e.getKey();
      String pathExpr = e.getValue();

      JsonNode n = source.at(toPointer(pathExpr));
      Object val = (n == null || n.isMissingNode() || n.isNull())
          ? null
          : (n.isNumber() ? n.numberValue() : n.asText());

      if (failOnMissing) {
        boolean missing = (val == null) || (val instanceof String s && s.trim().isEmpty());
        if (missing) {
          throw new TransformFailureException("ExtractStep: missing required path '" + pathExpr + "' for key '" + outKey + "'");
        }
      }

      ctx.put(outKey, val);
    }
  }

  /** Allow both "/a/b" (JsonPointer) and "a.b" (dotted) expressions. */
  private static String toPointer(String expr) {
    if (expr == null || expr.isBlank()) return "/";
    return expr.startsWith("/") ? expr : "/" + expr.replace(".", "/");
  }
}
