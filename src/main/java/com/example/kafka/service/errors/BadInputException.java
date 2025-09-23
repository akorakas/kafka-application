package com.example.kafka.service.errors;

public class BadInputException extends IllegalArgumentException {
  public BadInputException(String message, Throwable cause) { super(message, cause); }
  public BadInputException(String message) { super(message); }
}
