package com.example.kafka.service.pipeline.steps;

import java.util.List;
import java.util.Objects;

import com.example.kafka.service.config.TransformProperties;
import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;

public class UpdateStep implements TransformStep {
  private final List<String> stripCr;
  private final List<TransformProperties.ComputeAssignment> compute;
  private final String placeholder;

  public UpdateStep(List<String> stripCr, List<TransformProperties.ComputeAssignment> compute, String placeholder) {
    this.stripCr = stripCr;
    this.compute = compute;
    this.placeholder = placeholder;
  }

  @Override
  public void apply(TransformContext ctx) {
    if (stripCr != null) {
      for (String k : stripCr) {
        Object v = ctx.get(k);
        if (v instanceof String s) {
          ctx.put(k, s.replace("\r", "").trim());
        }
      }
    }
    if (compute != null) {
      for (var c : compute) {
        String val = evalMiniExpr(c.getExpr(), ctx, placeholder);
        ctx.put(c.getSet(), val);
      }
    }
  }

  /**
   * Supports:
   *  - ${var}
   *  - ${left == placeholder ? 'A' : 'B'}
   *  - ${left == '' ? 'A' : 'B'}
   */
  private static String evalMiniExpr(String expr, TransformContext ctx, String placeholder) {
    if (expr == null) return null;

    String e = expr.trim();
    if (e.startsWith("${") && e.endsWith("}")) {
      e = e.substring(2, e.length() - 1);
    }

    if (e.contains("?")) {
      String[] parts = e.split("\\?", 2);         // split only once
      String cond = parts[0].trim();              // e.g. resolvedAt == ''
      String[] arms = parts[1].split(":", 2);     // split only once
      String thenV = unquote(arms[0].trim());
      String elseV = unquote(arms[1].trim());

      boolean condTrue = false;
      if (cond.contains("==")) {
        String[] c = cond.split("==", 2);
        String left = c[0].trim();
        String right = c[1].trim();

        Object lv = ctx.get(left);
        String lvStr = (lv == null) ? null : lv.toString();
        String rv = "placeholder".equals(right) ? placeholder : unquote(right);

        condTrue = Objects.equals(lvStr, rv);
      }
      return condTrue ? thenV : elseV;
    } else {
      Object v = ctx.get(e);
      return (v == null) ? null : v.toString();
    }
  }

  private static String unquote(String s) {
    if (s == null) return null;
    if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }
}
