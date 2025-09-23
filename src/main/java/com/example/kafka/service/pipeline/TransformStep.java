package com.example.kafka.service.pipeline;

public interface TransformStep {
  void apply(TransformContext ctx) throws Exception;
}
