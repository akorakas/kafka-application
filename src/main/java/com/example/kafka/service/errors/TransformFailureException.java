package com.example.kafka.service.errors;

public class TransformFailureException extends IllegalStateException {
  public TransformFailureException(String message, Throwable cause) { super(message, cause); }
  public TransformFailureException(String message) { super(message); }
}
