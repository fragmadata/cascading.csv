package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;

/**
 * CSV configuration constants.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvConf {
  public static final String CSV_COLUMNS = "csv.columns";
  public static final String CSV_DELIMITER = "csv.delimiter";
  public static final String CSV_RECORD_SEPARATOR = "csv.record_separator";
  public static final String CSV_QUOTE_CHARACTER = "csv.quote";
  public static final String CSV_QUOTE_MODE = "csv.quote.mode";
  public static final String CSV_ESCAPE_CHARACTER = "csv.escape";
  public static final String CSV_IGNORE_EMPTY_LINES = "csv.ignore_empty_lines";
  public static final String CSV_IGNORE_SURROUNDING_SPACES = "csv.ignore_surrounding_lines";
  public static final String CSV_NULL_STRING = "csv.null";

  public static final String DEFAULT_CSV_DELIMITER = String.valueOf(CSVFormat.DEFAULT.getDelimiter());
  public static final String DEFAULT_CSV_RECORD_SEPARATOR = CSVFormat.DEFAULT.getRecordSeparator();
  public static final String DEFAULT_CSV_QUOTE_CHARACTER = String.valueOf(CSVFormat.DEFAULT.getQuoteCharacter());
  public static final String DEFAULT_CSV_QUOTE_MODE = CSVFormat.DEFAULT.getQuoteMode().name();
  public static final String DEFAULT_CSV_ESCAPE_CHARACTER = String.valueOf(CSVFormat.DEFAULT.getEscapeCharacter());
  public static final String DEFAULT_CSV_NULL_STRING = CSVFormat.DEFAULT.getNullString();

  private CsvConf() {
  }

  /**
   * Creates a CSV format from a Hadoop configuration.
   */
  static CSVFormat createFormat(Configuration conf) {
    CSVFormat format = CSVFormat.newFormat(conf.get(CsvConf.CSV_DELIMITER, CsvConf.DEFAULT_CSV_DELIMITER).charAt(0))
      .withRecordSeparator(conf.get(CsvConf.CSV_RECORD_SEPARATOR, CsvConf.DEFAULT_CSV_RECORD_SEPARATOR).charAt(0))
      .withQuote(conf.get(CsvConf.CSV_QUOTE_CHARACTER, CsvConf.DEFAULT_CSV_QUOTE_CHARACTER).charAt(0))
      .withEscape(conf.get(CsvConf.CSV_ESCAPE_CHARACTER, CsvConf.DEFAULT_CSV_ESCAPE_CHARACTER).charAt(0))
      .withQuoteMode(QuoteMode.valueOf(conf.get(CsvConf.CSV_QUOTE_MODE, CsvConf.DEFAULT_CSV_QUOTE_MODE)))
      .withIgnoreEmptyLines(conf.getBoolean(CsvConf.CSV_IGNORE_EMPTY_LINES, true))
      .withIgnoreSurroundingSpaces(conf.getBoolean(CsvConf.CSV_IGNORE_SURROUNDING_SPACES, true))
      .withNullString(conf.get(CsvConf.CSV_NULL_STRING, CsvConf.DEFAULT_CSV_NULL_STRING));

    String[] header = conf.getStrings(CsvConf.CSV_COLUMNS);
    if (header != null && header.length > 0) {
      format = format.withHeader(header);
    }
    return format;
  }

}
