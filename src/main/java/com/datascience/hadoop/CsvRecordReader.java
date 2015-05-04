package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;

/**
 * CSV record reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvRecordReader implements RecordReader<LongWritable, ListWritable<Text>> {
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
  public static final String DEFAULT_CSV_READER_ESCAPE_CHARACTER = String.valueOf(CSVFormat.DEFAULT.getEscapeCharacter());
  public static final boolean DEFAULT_CSV_READER_IGNORE_EMPTY_LINES = CSVFormat.DEFAULT.getIgnoreEmptyLines();
  public static final boolean DEFAULT_CSV_READER_IGNORE_SURROUNDING_SPACES = CSVFormat.DEFAULT.getIgnoreSurroundingSpaces();
  public static final String DEFAULT_CSV_READER_NULL_STRING = CSVFormat.DEFAULT.getNullString();

  private final Text[] cache = new Text[1024];
  private final CSVParser parser;
  private final Iterator<CSVRecord> iterator;
  private final float start;
  private final float length;
  private long position;
  private long end;

  public CsvRecordReader(FileSplit split, Configuration conf) throws IOException {
    this.start = split.getStart();
    this.length = split.getLength();
    this.end = split.getStart() + split.getLength();
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream is = fs.open(path);
    Reader isr = new InputStreamReader(is);
    parser = new CSVParser(isr, createFormat(conf), split.getStart(), split.getStart());
    iterator = parser.iterator();
  }

  @Override
  public boolean next(LongWritable key, ListWritable<Text> value) throws IOException {
    value.clear();
    if (position < end && iterator.hasNext()) {
      CSVRecord record = iterator.next();
      key.set(record.getRecordNumber());
      for (int i = 0; i < record.size(); i++) {
        Text text = cache[i];
        if (text == null) {
          text = new Text();
          cache[i] = text;
        }
        text.set(record.get(i));
        value.add(text);
      }
      position = record.getCharacterPosition();
      return true;
    }
    return false;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public ListWritable<Text> createValue() {
    return new ListWritable<>(Text.class);
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public float getProgress() throws IOException {
    return Math.min(1.0f, (float) position - start / length);
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

  /**
   * Creates a CSV format from a Hadoop configuration.
   */
  private static CSVFormat createFormat(Configuration conf) {
    CSVFormat format = CSVFormat.newFormat(conf.get(CSV_READER_DELIMITER, DEFAULT_CSV_READER_DELIMITER).charAt(0))
      .withSkipHeaderRecord(conf.getBoolean(CSV_READER_SKIP_HEADER, DEFAULT_CSV_READER_SKIP_HEADER))
      .withRecordSeparator(conf.get(CSV_READER_RECORD_SEPARATOR, DEFAULT_CSV_READER_RECORD_SEPARATOR).charAt(0))
      .withQuote(conf.get(CSV_READER_QUOTE_CHARACTER, DEFAULT_CSV_READER_QUOTE_CHARACTER).charAt(0))
      .withEscape(conf.get(CSV_READER_ESCAPE_CHARACTER, DEFAULT_CSV_READER_ESCAPE_CHARACTER).charAt(0))
      .withIgnoreEmptyLines(conf.getBoolean(CSV_READER_IGNORE_EMPTY_LINES, DEFAULT_CSV_READER_IGNORE_EMPTY_LINES))
      .withIgnoreSurroundingSpaces(conf.getBoolean(CSV_READER_IGNORE_SURROUNDING_SPACES, DEFAULT_CSV_READER_IGNORE_SURROUNDING_SPACES))
      .withNullString(conf.get(CSV_READER_NULL_STRING, DEFAULT_CSV_READER_NULL_STRING));

    String[] header = conf.getStrings(CSV_READER_COLUMNS);
    if (header != null && header.length > 0) {
      format = format.withHeader(header);
    }

    String quoteMode = conf.get(CSV_READER_QUOTE_MODE, DEFAULT_CSV_READER_QUOTE_MODE);
    if (quoteMode != null) {
      format = format.withQuoteMode(QuoteMode.valueOf(quoteMode));
    }
    return format;
  }

}
