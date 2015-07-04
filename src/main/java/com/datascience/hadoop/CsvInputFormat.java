/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * CSV input format.
 * <p>
 * This custom input format supports reading records from a CSV file using <a href="https://commons.apache.org/proper/commons-csv/">Apache Commons CSV</a>.
 * As with more basic Hadoop input formats such as {@link org.apache.hadoop.mapred.TextInputFormat}, this input format
 * supports basic decompression through Hadoop's standard {@link org.apache.hadoop.io.compress.CompressionCodec codecs}.
 * However, the limitations of the {@link com.datascience.hadoop.CsvRecordReader} mean that this input format <em>does not
 * currently support input splits</em>.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvInputFormat extends FileInputFormat<LongWritable, ListWritable<Text>> implements JobConfigurable {
  public static final String STRICT_MODE = "csv.strict";
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

  private static final boolean DEFAULT_CSV_READER_SKIP_HEADER = CSVFormat.DEFAULT.getSkipHeaderRecord();
  private static final String DEFAULT_CSV_READER_DELIMITER = String.valueOf(CSVFormat.DEFAULT.getDelimiter());
  private static final String DEFAULT_CSV_READER_RECORD_SEPARATOR = CSVFormat.DEFAULT.getRecordSeparator();
  private static final String DEFAULT_CSV_READER_QUOTE_CHARACTER = String.valueOf(CSVFormat.DEFAULT.getQuoteCharacter());
  private static final String DEFAULT_CSV_READER_QUOTE_MODE = CSVFormat.DEFAULT.getQuoteMode() != null ? CSVFormat.DEFAULT.getQuoteMode().name() : null;
  private static final String DEFAULT_CSV_READER_ESCAPE_CHARACTER = null;
  private static final boolean DEFAULT_CSV_READER_IGNORE_EMPTY_LINES = CSVFormat.DEFAULT.getIgnoreEmptyLines();
  private static final boolean DEFAULT_CSV_READER_IGNORE_SURROUNDING_SPACES = CSVFormat.DEFAULT.getIgnoreSurroundingSpaces();
  private static final String DEFAULT_CSV_READER_NULL_STRING = CSVFormat.DEFAULT.getNullString();

  private CompressionCodecFactory codecs;

  @Override
  public void configure(JobConf conf) {
    codecs = new CompressionCodecFactory(conf);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path path) {
    // TODO: CsvRecordReader cannot currently support input splits.
    // final CompressionCodec codec = codecs.getCodec(path);
    // return codec == null || codec instanceof SplittableCompressionCodec;
    return false;
  }

  @Override
  public RecordReader<LongWritable, ListWritable<Text>> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
    FileSplit split = (FileSplit) inputSplit;
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    InputStream is = fs.open(path);

    // If the input is compressed, load the compression codec.
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory.getCodec(path);
    if (codec != null) {
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      is = codec.createInputStream(is, decompressor);
    }
    return new CsvRecordReader(is, createFormat(conf), split.getLength(), conf.getBoolean(STRICT_MODE, true));
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
