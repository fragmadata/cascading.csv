package com.datascience.util;

/**
 * Csv parse exception
 *
 * Created by hcavalle on 7/30/15.
 */
public class CsvParseException extends RuntimeException{

  public CsvParseException() {
  }

  public CsvParseException(String message) {
    super(message);
  }

  public CsvParseException(String message, Throwable cause) {
    super(message, cause);
  }

  public CsvParseException(Throwable cause) {
    super(cause);
  }
}
