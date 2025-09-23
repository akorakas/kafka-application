package com.example.kafka.service.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "transform")
public class TransformProperties {

  private String placeholder = "{EVENT.RECOVERY.DATE} {EVENT.RECOVERY.TIME}";
  private List<Step> pipeline;

  public String getPlaceholder() { return placeholder; }
  public void setPlaceholder(String p) { this.placeholder = p; }

  public List<Step> getPipeline() { return pipeline; }
  public void setPipeline(List<Step> p) { this.pipeline = p; }

  public static class Step {
    // common
    private String type;                        // extract | update | regexExtract | flatten | hash | template | requireFields

    // ----- extract -----
    private Map<String, String> mappings;       // outputVar -> json path
    private String fromVar;                     // read from ctx var instead of root (JSON string)
    private Boolean failOnMissing;              // throw if a mapping path is missing/null/blank
    private Boolean failOnBadJson;              // throw if fromVar isn't valid JSON

    // ----- update -----
    private List<String> stripCr;
    private List<ComputeAssignment> compute;

    // ----- regexExtract -----
    private String source;                      // context var name that holds the source text
    private String pattern;
    private Integer group;
    private String target;
    private String fallback;                    // optional var name to use when no match

    // ----- flatten -----
    private List<String> roots;                 // e.g., ["fields", "tags"]
    private List<String> includeTop;            // e.g., ["name", "timestamp"]

    // ----- hash -----
    private String algorithm;                   // e.g., MD5, SHA-256
    private List<String> fields;                // vars to concatenate before hashing

    // ----- template -----
    private String template;                    // final JSON template as string

    // ----- requireFields (optional future step) -----
    private List<String> required;              // list of JSON pointers to require (e.g., "/fields/event_id")

    // getters/setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Map<String, String> getMappings() { return mappings; }
    public void setMappings(Map<String, String> mappings) { this.mappings = mappings; }

    public String getFromVar() { return fromVar; }
    public void setFromVar(String fromVar) { this.fromVar = fromVar; }

    public Boolean getFailOnMissing() { return failOnMissing; }
    public void setFailOnMissing(Boolean failOnMissing) { this.failOnMissing = failOnMissing; }

    public Boolean getFailOnBadJson() { return failOnBadJson; }
    public void setFailOnBadJson(Boolean failOnBadJson) { this.failOnBadJson = failOnBadJson; }

    public List<String> getStripCr() { return stripCr; }
    public void setStripCr(List<String> stripCr) { this.stripCr = stripCr; }

    public List<ComputeAssignment> getCompute() { return compute; }
    public void setCompute(List<ComputeAssignment> compute) { this.compute = compute; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getPattern() { return pattern; }
    public void setPattern(String pattern) { this.pattern = pattern; }

    public Integer getGroup() { return group; }
    public void setGroup(Integer group) { this.group = group; }

    public String getTarget() { return target; }
    public void setTarget(String target) { this.target = target; }

    public String getFallback() { return fallback; }
    public void setFallback(String fallback) { this.fallback = fallback; }

    public List<String> getRoots() { return roots; }
    public void setRoots(List<String> roots) { this.roots = roots; }

    public List<String> getIncludeTop() { return includeTop; }
    public void setIncludeTop(List<String> includeTop) { this.includeTop = includeTop; }

    public String getAlgorithm() { return algorithm; }
    public void setAlgorithm(String algorithm) { this.algorithm = algorithm; }

    public List<String> getFields() { return fields; }
    public void setFields(List<String> fields) { this.fields = fields; }

    public String getTemplate() { return template; }
    public void setTemplate(String template) { this.template = template; }

    public List<String> getRequired() { return required; }
    public void setRequired(List<String> required) { this.required = required; }
  }

  public static class ComputeAssignment {
    private String set;
    private String expr;

    public String getSet() { return set; }
    public void setSet(String set) { this.set = set; }
    public String getExpr() { return expr; }
    public void setExpr(String expr) { this.expr = expr; }
  }
}
