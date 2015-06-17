package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * CSV output format.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvOutputFormat extends FileOutputFormat<LongWritable, ListWritable<Text>> {
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
  public static final String DEFAULT_CSV_WRITER_ESCAPE_CHARACTER = null;
  public static final boolean DEFAULT_CSV_WRITER_IGNORE_EMPTY_LINES = CSVFormat.DEFAULT.getIgnoreEmptyLines();
  public static final boolean DEFAULT_CSV_WRITER_IGNORE_SURROUNDING_SPACES = CSVFormat.DEFAULT.getIgnoreSurroundingSpaces();
  public static final String DEFAULT_CSV_WRITER_NULL_STRING = CSVFormat.DEFAULT.getNullString();

  @Override
  public RecordWriter<LongWritable, ListWritable<Text>> getRecordWriter(FileSystem fileSystem, JobConf conf, String name, Progressable progress) throws IOException {
    Path path;
    if (FileOutputFormat.getCompressOutput(conf)) {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(conf, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      path = FileOutputFormat.getTaskOutputPath(conf, name + codec.getDefaultExtension());
    } else {
      path = FileOutputFormat.getTaskOutputPath(conf, name);
    }
    return new CsvRecordWriter(path.getFileSystem(conf).create(path, progress), createFormat(conf));
  }

  /**
   * Creates a CSV format from a Hadoop configuration.
   */
  private static CSVFormat createFormat(Configuration conf) {
    CSVFormat format = CSVFormat.newFormat(conf.get(CSV_WRITER_DELIMITER, DEFAULT_CSV_WRITER_DELIMITER).charAt(0))
      .withSkipHeaderRecord(conf.getBoolean(CSV_WRITER_SKIP_HEADER, DEFAULT_CSV_WRITER_SKIP_HEADER))
      .withRecordSeparator(conf.get(CSV_WRITER_RECORD_SEPARATOR, DEFAULT_CSV_WRITER_RECORD_SEPARATOR))
      .withIgnoreEmptyLines(conf.getBoolean(CSV_WRITER_IGNORE_EMPTY_LINES, DEFAULT_CSV_WRITER_IGNORE_EMPTY_LINES))
      .withIgnoreSurroundingSpaces(conf.getBoolean(CSV_WRITER_IGNORE_SURROUNDING_SPACES, DEFAULT_CSV_WRITER_IGNORE_SURROUNDING_SPACES))
      .withNullString(conf.get(CSV_WRITER_NULL_STRING, DEFAULT_CSV_WRITER_NULL_STRING));

    String[] header = conf.getStrings(CSV_WRITER_COLUMNS);
    if (header != null && header.length > 0)
      format = format.withHeader(header);

    String escape = conf.get(CSV_WRITER_ESCAPE_CHARACTER, DEFAULT_CSV_WRITER_ESCAPE_CHARACTER);
    if (escape != null)
      format = format.withEscape(escape.charAt(0));

    String quote = conf.get(CSV_WRITER_QUOTE_CHARACTER, DEFAULT_CSV_WRITER_QUOTE_CHARACTER);
    if (quote != null)
      format = format.withQuote(quote.charAt(0));

    String quoteMode = conf.get(CSV_WRITER_QUOTE_MODE, DEFAULT_CSV_WRITER_QUOTE_MODE);
    if (quoteMode != null)
      format = format.withQuoteMode(QuoteMode.valueOf(quoteMode));
    return format;
  }

}
