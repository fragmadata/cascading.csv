package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * CSV input format.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvInputFormat extends FileInputFormat<LongWritable, ListWritable<Text>> {
  public static final String CSV_READER_COLUMNS = "csv.reader.columns";
  public static final String CSV_READER_SKIP_HEADER = "csv.reader.skip_header";
  public static final String CSV_READER_DELIMITER = "csv.reader.delimiter";
  public static final String CSV_READER_RECORD_SEPARATOR = "csv.reader.record_separator";
  public static final String CSV_READER_QUOTE_CHARACTER = "csv.reader.quote";
  public static final String CSV_READER_QUOTE_MODE = "csv.reader.quote.mode";
  public static final String CSV_READER_ESCAPE_CHARACTER = "csv.reader.escape";
  public static final String CSV_READER_IGNORE_EMPTY_LINES = "csv.reader.ignore_empty_lines";
  public static final String CSV_READER_IGNORE_SURROUNDING_SPACES = "csv.reader.ignore_surrounding_lines";
  public static final String CSV_READER_NULL_STRING = "csv.reader.null";

  public static final boolean DEFAULT_CSV_READER_SKIP_HEADER = CSVFormat.DEFAULT.getSkipHeaderRecord();
  public static final String DEFAULT_CSV_READER_DELIMITER = String.valueOf(CSVFormat.DEFAULT.getDelimiter());
  public static final String DEFAULT_CSV_READER_RECORD_SEPARATOR = CSVFormat.DEFAULT.getRecordSeparator();
  public static final String DEFAULT_CSV_READER_QUOTE_CHARACTER = String.valueOf(CSVFormat.DEFAULT.getQuoteCharacter());
  public static final String DEFAULT_CSV_READER_QUOTE_MODE = CSVFormat.DEFAULT.getQuoteMode() != null ? CSVFormat.DEFAULT.getQuoteMode().name() : null;
  public static final String DEFAULT_CSV_READER_ESCAPE_CHARACTER = null;
  public static final boolean DEFAULT_CSV_READER_IGNORE_EMPTY_LINES = CSVFormat.DEFAULT.getIgnoreEmptyLines();
  public static final boolean DEFAULT_CSV_READER_IGNORE_SURROUNDING_SPACES = CSVFormat.DEFAULT.getIgnoreSurroundingSpaces();
  public static final String DEFAULT_CSV_READER_NULL_STRING = CSVFormat.DEFAULT.getNullString();

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return true;
  }

  @Override
  public RecordReader<LongWritable, ListWritable<Text>> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
    FileSplit split = (FileSplit) inputSplit;
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    InputStream is = fs.open(path);

    long start = split.getStart();
    long end = split.getStart() + split.getLength();

    // If the input is compressed, load the compression codec.
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory.getCodec(path);
    if (codec != null) {
      // Get the codec decompressor and determine whether the decompressor is splittable. If the codec is splittable, calculate
      // a split input stream. This is necessary because the compressed split will differ from the decompressed split.
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      if (codecFactory instanceof SplittableCompressionCodec) {
        SplitCompressionInputStream cin = ((SplittableCompressionCodec) codec).createInputStream(is, decompressor, split.getStart(), split.getStart() + split.getLength(), SplittableCompressionCodec.READ_MODE.BYBLOCK);
        start = cin.getAdjustedStart();
        end = cin.getAdjustedEnd();
        is = cin;
      } else {
        if (split.getStart() != 0) {
          throw new IOException("Cannot seek in " + codec.getClass().getSimpleName() + " compressed stream");
        }
        is = codec.createInputStream(is, decompressor);
      }
    }
    return new CsvRecordReader(is, createFormat(conf), start, end);
  }

  /**
   * Creates a CSV format from a Hadoop configuration.
   */
  private static CSVFormat createFormat(Configuration conf) {
    CSVFormat format = CSVFormat.newFormat(conf.get(CSV_READER_DELIMITER, DEFAULT_CSV_READER_DELIMITER).charAt(0))
      .withSkipHeaderRecord(conf.getBoolean(CSV_READER_SKIP_HEADER, DEFAULT_CSV_READER_SKIP_HEADER))
      .withRecordSeparator(conf.get(CSV_READER_RECORD_SEPARATOR, DEFAULT_CSV_READER_RECORD_SEPARATOR))
      .withIgnoreEmptyLines(conf.getBoolean(CSV_READER_IGNORE_EMPTY_LINES, DEFAULT_CSV_READER_IGNORE_EMPTY_LINES))
      .withIgnoreSurroundingSpaces(conf.getBoolean(CSV_READER_IGNORE_SURROUNDING_SPACES, DEFAULT_CSV_READER_IGNORE_SURROUNDING_SPACES))
      .withNullString(conf.get(CSV_READER_NULL_STRING, DEFAULT_CSV_READER_NULL_STRING));

    String[] header = conf.getStrings(CSV_READER_COLUMNS);
    if (header != null && header.length > 0)
      format = format.withHeader(header);

    String escape = conf.get(CSV_READER_ESCAPE_CHARACTER, DEFAULT_CSV_READER_ESCAPE_CHARACTER);
    if (escape != null)
      format = format.withEscape(escape.charAt(0));

    String quote = conf.get(CSV_READER_QUOTE_CHARACTER, DEFAULT_CSV_READER_QUOTE_CHARACTER);
    if (quote != null)
      format = format.withQuote(quote.charAt(0));

    String quoteMode = conf.get(CSV_READER_QUOTE_MODE, DEFAULT_CSV_READER_QUOTE_MODE);
    if (quoteMode != null)
      format = format.withQuoteMode(QuoteMode.valueOf(quoteMode));
    return format;
  }

}
