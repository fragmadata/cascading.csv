package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * CSV record writer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvRecordWriter implements RecordWriter<LongWritable, ListWritable<Text>> {
  public static final String CSV_WRITER_COLUMNS = "csv.writer.columns";
  public static final String CSV_WRITER_SKIP_HEADER = "csv.writer.skip_header";
  public static final String CSV_WRITER_DELIMITER = "csv.writer.delimiter";
  public static final String CSV_WRITER_RECORD_SEPARATOR = "csv.writer.record_separator";
  public static final String CSV_WRITER_QUOTE_CHARACTER = "csv.writer.quote";
  public static final String CSV_WRITER_QUOTE_MODE = "csv.writer.quote.mode";
  public static final String CSV_WRITER_ESCAPE_CHARACTER = "csv.writer.escape";
  public static final String CSV_WRITER_IGNORE_EMPTY_LINES = "csv.writer.ignore_empty_lines";
  public static final String CSV_WRITER_IGNORE_SURROUNDING_SPACES = "csv.writer.ignore_surrounding_lines";
  public static final String CSV_WRITER_NULL_STRING = "csv.writer.null";

  public static final boolean DEFAULT_CSV_WRITER_SKIP_HEADER = CSVFormat.DEFAULT.getSkipHeaderRecord();
  public static final String DEFAULT_CSV_WRITER_DELIMITER = String.valueOf(CSVFormat.DEFAULT.getDelimiter());
  public static final String DEFAULT_CSV_WRITER_RECORD_SEPARATOR = CSVFormat.DEFAULT.getRecordSeparator();
  public static final String DEFAULT_CSV_WRITER_QUOTE_CHARACTER = String.valueOf(CSVFormat.DEFAULT.getQuoteCharacter());
  public static final String DEFAULT_CSV_WRITER_QUOTE_MODE = CSVFormat.DEFAULT.getQuoteMode() != null ? CSVFormat.DEFAULT.getQuoteMode().name() : null;
  public static final String DEFAULT_CSV_WRITER_ESCAPE_CHARACTER = String.valueOf(CSVFormat.DEFAULT.getEscapeCharacter());
  public static final boolean DEFAULT_CSV_WRITER_IGNORE_EMPTY_LINES = CSVFormat.DEFAULT.getIgnoreEmptyLines();
  public static final boolean DEFAULT_CSV_WRITER_IGNORE_SURROUNDING_SPACES = CSVFormat.DEFAULT.getIgnoreSurroundingSpaces();
  public static final String DEFAULT_CSV_WRITER_NULL_STRING = CSVFormat.DEFAULT.getNullString();

  private final CSVPrinter out;

  public CsvRecordWriter(FileSystem fs, Path path, Configuration conf) throws IOException {
    out = new CSVPrinter(new OutputStreamWriter(fs.create(path)), createFormat(conf));
  }

  @Override
  public void write(LongWritable key, ListWritable<Text> value) throws IOException {
    out.printRecord(value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    out.close();
  }

  /**
   * Creates a CSV format from a Hadoop configuration.
   */
  private static CSVFormat createFormat(Configuration conf) {
    CSVFormat format = CSVFormat.newFormat(conf.get(CSV_WRITER_DELIMITER, DEFAULT_CSV_WRITER_DELIMITER).charAt(0))
      .withSkipHeaderRecord(conf.getBoolean(CSV_WRITER_SKIP_HEADER, DEFAULT_CSV_WRITER_SKIP_HEADER))
      .withRecordSeparator(conf.get(CSV_WRITER_RECORD_SEPARATOR, DEFAULT_CSV_WRITER_RECORD_SEPARATOR).charAt(0))
      .withQuote(conf.get(CSV_WRITER_QUOTE_CHARACTER, DEFAULT_CSV_WRITER_QUOTE_CHARACTER).charAt(0))
      .withEscape(conf.get(CSV_WRITER_ESCAPE_CHARACTER, DEFAULT_CSV_WRITER_ESCAPE_CHARACTER).charAt(0))
      .withIgnoreEmptyLines(conf.getBoolean(CSV_WRITER_IGNORE_EMPTY_LINES, DEFAULT_CSV_WRITER_IGNORE_EMPTY_LINES))
      .withIgnoreSurroundingSpaces(conf.getBoolean(CSV_WRITER_IGNORE_SURROUNDING_SPACES, DEFAULT_CSV_WRITER_IGNORE_SURROUNDING_SPACES))
      .withNullString(conf.get(CSV_WRITER_NULL_STRING, DEFAULT_CSV_WRITER_NULL_STRING));

    String[] header = conf.getStrings(CSV_WRITER_COLUMNS);
    if (header != null && header.length > 0) {
      format = format.withHeader(header);
    }

    String quoteMode = conf.get(CSV_WRITER_QUOTE_MODE, DEFAULT_CSV_WRITER_QUOTE_MODE);
    if (quoteMode != null) {
      format = format.withQuoteMode(QuoteMode.valueOf(quoteMode));
    }
    return format;
  }

}
