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
package com.datascience.cascading.scheme;

import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.Visibility;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import com.datascience.hadoop.CsvInputFormat;
import com.datascience.hadoop.CsvOutputFormat;
import com.datascience.hadoop.ListWritable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.log4j.Logger;

/**
 * The CSV scheme provides support for parsing and formatting CSV files using
 * <a href="https://commons.apache.org/proper/commons-csv/">Apache Commons CSV</a>.
 * <p>
 * This scheme is designed for use a source or a sink in a Hadoop MR2 flow.
 * <p>
 * To use the scheme, simply construct a new instance, passing either the {@link cascading.tuple.Fields} or a
 * {@link org.apache.commons.csv.CSVFormat} defining the structure of the CSV file.
 * <pre>
 *   {@code
 *     CSVFormat format = CSVFormat.newFormat(',')
 *       .withHeader("user_id", "first_name", "last_name")
 *       .withSkipHeaderRecord(true);
 *     CsvScheme scheme = new CsvScheme(format);
 *     Tap tap = new Hfs(scheme, "hdfs://users.csv");
 *   }
 * </pre>
 * <p>
 * The CSV scheme changes its behavior according to the {@link cascading.tuple.Fields} or {@link org.apache.commons.csv.CSVFormat}
 * provided to the constructor.
 * <p>
 * For sources, this scheme detects headers and applies values to fields according to a set of rules based on the
 * arguments provided. Generally speaking, {@link org.apache.commons.csv.CSVFormat#getHeader() headers} define the columns
 * as they are present in the underlying CSV file, and {@link cascading.tuple.Fields} specify how those columns should
 * be presented to Cascading. The scheme may automatically detect CSV header information depending on the fields and
 * headers provided to this constructor.
 * <p>
 * Users can provided different combinations of {@link cascading.tuple.Fields} and {@link org.apache.commons.csv.CSVFormat#getHeader()}
 * values to control the input and output of the scheme. The rules that the scheme uses to resolve {@link cascading.tuple.Fields}
 * and {@link org.apache.commons.csv.CSVFormat#getHeader() headers} for sources are as follows:
 * <ul>
 * <li>If {@code Fields} is {@link Fields#UNKNOWN} and {@link org.apache.commons.csv.CSVFormat#getHeader()} is {@code null}
 * or empty and {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()} is {@code false}, the scheme assigns positional
 * names to the columns (e.g. {@code col1}, {@code col2}, {@code col3}) and perform no additional validation.</li>
 * <li>If {@code Fields} is {@link Fields#UNKNOWN} and {@link org.apache.commons.csv.CSVFormat#getHeader()} is {@code null}
 * or empty but {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()} is {@code true}, the scheme will automatically
 * detect the CSV headers using the first record in the file and perform no further validation.</li>
 * <li>If {@code Fields} are defined, {@link org.apache.commons.csv.CSVFormat#getHeader()} is {@code null} or empty and
 * {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()} is {@code true}, the scheme will detect the header
 * from the first record in the CSV file and verify that the provided {@code Fields} names are present in the header names.</li>
 * <li>If {@code Fields} are defined, {@link org.apache.commons.csv.CSVFormat#getHeader()} is {@code null} or empty and
 * {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()} is {@code false}, the scheme will verify that the number
 * of provided fields matches the number of provided columns.</li>
 * <li>If {@code Fields} are defined and {@link org.apache.commons.csv.CSVFormat#getHeader()} are defined, the scheme will
 * verify that the provided headers match the number of values in the first record in the CSV file, and verify that the
 * provided field names are all present in the header names.</li>
 * <li>If {@code Fields} are {@link Fields#UNKNOWN} and {@link org.apache.commons.csv.CSVFormat#getHeader()} is defined,
 * the scheme will verify that the provided headers match the number of values in the first record in the CSV file and assign
 * header names to unknown {@code Fields}.</li>
 * </ul>
 * Similarly, the differences between sink {@link cascading.tuple.Fields} and {@link org.apache.commons.csv.CSVFormat#getHeader() headers}
 * allows users to control how fields are written to CSV files. The rules that the scheme uses to resolve {@link cascading.tuple.Fields}
 * and {@link org.apache.commons.csv.CSVFormat#getHeader() headers} are as follows:
 * <ul>
 * <li>If {@link org.apache.commons.csv.CSVFormat#getHeader()} is defined and {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()}
 * is {@code true}, the provided header record will be written to the output CSV file(s).</li>
 * <li>If {@link org.apache.commons.csv.CSVFormat#getHeader()} is not defined and {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()}
 * is {@code true}, the input {@link cascading.tuple.Fields} will be written to the CSV file(s) as the header.</li>
 * <li>If {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()} is {@code false}, no header will be written
 * to the output file(s).</li>
 * <li>If both sink {@link cascading.tuple.Fields} and {@link org.apache.commons.csv.CSVFormat#getHeader() headers} are defined,
 * all field names must be present in the output header record. When tuples are written out to the CSV file, any fields
 * not present in the header record will be written as {@code null}. You can configure the output {@code null} string
 * via {@link org.apache.commons.csv.CSVFormat#withNullString(String)}</li>
 * </ul>
 * Internally, {@code CsvScheme} uses {@link com.datascience.hadoop.CsvInputFormat} and {@link com.datascience.hadoop.CsvOutputFormat}
 * for sourcing and sinking data respectively. These custom Hadoop input/output formats allow the scheme complete control
 * over the encoding, compression, parsing, and formatting of bytes beneath Cascading's abstractions.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  private static final Logger LOGGER = Logger.getLogger(CsvScheme.class);
  private final CSVFormat format;
  private final boolean strict;
  private final String charset;
  private Map<String, Integer> indices;

  /**
   * Creates a new CSV scheme with {@link org.apache.commons.csv.CSVFormat#DEFAULT}.
   * <p>
   * Strict mode is enabled when using this constructor.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme() {
    this(Fields.ALL, Fields.ALL, CSVFormat.DEFAULT, StandardCharsets.UTF_8, true);
  }

  /**
   * Creates a new CSV scheme with {@link org.apache.commons.csv.CSVFormat#DEFAULT}.
   * <p>
   * Strict mode is enabled when using this constructor.
   *
   * @param charset The character set with which to read and write CSV files.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Charset charset) {
    this(Fields.ALL, Fields.ALL, CSVFormat.DEFAULT, charset, true);
  }

  /**
   * Creates a new CSV scheme with the {@link org.apache.commons.csv.CSVFormat#DEFAULT} format.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param strict Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *               parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(boolean strict) {
    this(Fields.ALL, Fields.ALL, CSVFormat.DEFAULT, StandardCharsets.UTF_8, strict);
  }

  /**
   * Creates a new CSV scheme with the {@link org.apache.commons.csv.CSVFormat#DEFAULT} format.
   *
   * @param charset The character set with which to read and write CSV files.
   * @param strict  Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Charset charset, boolean strict) {
    this(Fields.ALL, Fields.ALL, CSVFormat.DEFAULT, charset, strict);
  }

  /**
   * Creates a new CSV scheme with the given {@link org.apache.commons.csv.CSVFormat}.
   * <p>
   * Strict mode is enabled when using this constructor.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param format The format with which to parse (source) or format (sink) records.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(CSVFormat format) {
    this(Fields.ALL, Fields.ALL, format, StandardCharsets.UTF_8, true);
  }

  /**
   * Creates a new CSV scheme with the given {@link org.apache.commons.csv.CSVFormat}.
   * <p>
   * Strict mode is enabled when using this constructor.
   *
   * @param charset The character set with which to read and write CSV files.
   * @param format  The format with which to parse (source) or format (sink) records.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(CSVFormat format, Charset charset) {
    this(Fields.ALL, Fields.ALL, format, charset, true);
  }

  /**
   * Creates a new CSV scheme with the given {@link org.apache.commons.csv.CSVFormat}.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param format The format with which to parse (source) or format (sink) records.
   * @param strict Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *               parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(CSVFormat format, boolean strict) {
    this(Fields.ALL, Fields.ALL, format, StandardCharsets.UTF_8, strict);
  }

  /**
   * Creates a new CSV scheme with the given {@link org.apache.commons.csv.CSVFormat}.
   *
   * @param format  The format with which to parse (source) or format (sink) records.
   * @param charset The character set with which to read and write CSV files.
   * @param strict  Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(CSVFormat format, Charset charset, boolean strict) {
    this(Fields.ALL, Fields.ALL, format, charset, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * Strict mode is enabled when using this constructor.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param fields The source and sink fields.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields) {
    this(fields, fields, CSVFormat.DEFAULT, StandardCharsets.UTF_8, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * Strict mode is enabled when using this constructor.
   *
   * @param fields  The source and sink fields.
   * @param charset The character set with which to read and write CSV files.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields, Charset charset) {
    this(fields, fields, CSVFormat.DEFAULT, charset, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param fields The source and sink fields.
   * @param strict Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *               parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields, boolean strict) {
    this(fields, fields, CSVFormat.DEFAULT, StandardCharsets.UTF_8, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   *
   * @param fields  The source and sink fields.
   * @param charset The character set with which to read and write CSV files.
   * @param strict  Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields, Charset charset, boolean strict) {
    this(fields, fields, CSVFormat.DEFAULT, charset, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * Strict mode is enabled when using this constructor.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param fields The source and sink fields.
   * @param format The format with which to parse (source) or format (sink) records.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields, CSVFormat format) {
    this(fields, fields, format, StandardCharsets.UTF_8, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * Strict mode is enabled when using this constructor.
   *
   * @param fields  The source and sink fields.
   * @param format  The format with which to parse (source) or format (sink) records.
   * @param charset The character set with which to read and write CSV files.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields, CSVFormat format, Charset charset) {
    this(fields, fields, format, charset, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param fields The source and sink fields.
   * @param format The format with which to parse (source) or format (sink) records.
   * @param strict Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *               parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields, CSVFormat format, boolean strict) {
    this(fields, fields, format, StandardCharsets.UTF_8, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   *
   * @param fields  The source and sink fields.
   * @param format  The format with which to parse (source) or format (sink) records.
   * @param charset The character set with which to read and write CSV files.
   * @param strict  Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields fields, CSVFormat format, Charset charset, boolean strict) {
    this(fields, fields, format, charset, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * Strict mode is enabled when using this constructor.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields) {
    this(sourceFields, sinkFields, CSVFormat.DEFAULT, StandardCharsets.UTF_8, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * Strict mode is enabled when using this constructor.
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @param charset      The character set with which to read and write CSV files.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, Charset charset) {
    this(sourceFields, sinkFields, CSVFormat.DEFAULT, charset, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @param strict       Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                     parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, boolean strict) {
    this(sourceFields, sinkFields, CSVFormat.DEFAULT, StandardCharsets.UTF_8, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @param charset      The character set with which to read and write CSV files.
   * @param strict       Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                     parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, Charset charset, boolean strict) {
    this(sourceFields, sinkFields, CSVFormat.DEFAULT, charset, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * Strict mode is enabled when using this constructor.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @param format       The format with which to parse (source) or format (sink) records.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, CSVFormat format) {
    this(sourceFields, sinkFields, format, StandardCharsets.UTF_8, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * Strict mode is enabled when using this constructor.
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @param format       The format with which to parse (source) or format (sink) records.
   * @param charset      The character set with which to read and write CSV files.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, CSVFormat format, Charset charset) {
    this(sourceFields, sinkFields, format, charset, true);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * The CSV input/output encoding set defaults to {@code UTF-8}
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @param format       The format with which to parse (source) or format (sink) records.
   * @param strict       Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                     parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, CSVFormat format, boolean strict) {
    this(sourceFields, sinkFields, format, StandardCharsets.UTF_8, strict);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   *
   * @param sourceFields The source fields.
   * @param sinkFields   The sink fields.
   * @param format       The format with which to parse (source) or format (sink) records.
   * @param charset      The character set with which to read and write CSV files.
   * @param strict       Indicates whether to parse records in strict parsing mode. When strict mode is disabled, single record
   *                     parse errors will be caught and logged.
   * @see com.datascience.cascading.scheme.CsvScheme
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, CSVFormat format, Charset charset, boolean strict) {
    super();
    setSourceFields(sourceFields);
    setSinkFields(sinkFields);
    this.format = format;
    this.charset = charset.name();
    this.strict = strict;
  }

  @Property(name = "delimiter", visibility = Visibility.PUBLIC)
  @PropertyDescription("The CSV field delimiter.")
  public char getDelimiter() {
    return format.getDelimiter();
  }

  @Property(name = "skipHeaderRecord", visibility = Visibility.PUBLIC)
  @PropertyDescription("Whether to skip the header record.")
  public boolean getSkipHeaderRecord() {
    return format.getSkipHeaderRecord();
  }

  @Property(name = "recordSeparator", visibility = Visibility.PUBLIC)
  @PropertyDescription("The record separator string.")
  public String getRecordSeparator() {
    return format.getRecordSeparator();
  }

  @Property(name = "quoteCharacter", visibility = Visibility.PUBLIC)
  @PropertyDescription("The quote character.")
  public Character getQuoteCharacter() {
    return format.getQuoteCharacter();
  }

  @Property(name = "quoteMode", visibility = Visibility.PUBLIC)
  @PropertyDescription("The quote mode.")
  public String getQuoteMode() {
    return format.getQuoteMode().name();
  }

  @Property(name = "escapeCharacter", visibility = Visibility.PUBLIC)
  @PropertyDescription("The escape character.")
  public Character getEscapeCharacter() {
    return format.getEscapeCharacter();
  }

  @Property(name = "ignoreSurroundingSpaces", visibility = Visibility.PUBLIC)
  @PropertyDescription("Whether to ignore spaces surrounding fields and records.")
  public boolean isIgnoreSurroundingSpaces() {
    return format.getIgnoreSurroundingSpaces();
  }

  @Property(name = "ignoreEmptyLines", visibility = Visibility.PUBLIC)
  @PropertyDescription("Whether to ignore empty lines.")
  public boolean isIgnoreEmptyLines() {
    return format.getIgnoreEmptyLines();
  }

  @Property(name = "nullString", visibility = Visibility.PUBLIC)
  @PropertyDescription("The string to convert to null.")
  public String getNullString() {
    return format.getNullString();
  }

  @Override
  public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    Fields fields = getSourceFields();

    // If fields is unknown and headers is null then detect the header from the file.
    // If fields is unknown and headers is null and skipHeaderRecord is true, this indicates that the field names need
    // to be detected. Assume the first record represents the headers and map the first record to the output fields.
    if (fields.isUnknown() && format.getHeader() == null && format.getSkipHeaderRecord()) {
      setSourceFields(detectHeader(flowProcess, tap, false));
    }
    // If fields is unknown and headers is null and skipHeaderRecord is false, this indicates that no headers are available
    // to be detected. Instead, we generate generic ordinal based field names for each column in the input file.
    else if (fields.isUnknown() && format.getHeader() == null) {
      setSourceFields(detectHeader(flowProcess, tap, true));
    }
    // If fields are provided but header is null and the first record is skipped, use the provided field names as the
    // CSV header names, assume the first record is a header and validate the provided field names against the header.
    // This will allow provided field names to be intersected with CSV columns based on names in the header record.
    else if (!fields.isUnknown() && format.getHeader() == null && format.getSkipHeaderRecord()) {
      if (!validateFields(flowProcess, tap, fields)) {
        throw new RuntimeException("Fields passed not present in header");
      }
    }
    // If fields are provided but header is null and the first record is not skipped, simply validate that the number
    // of provided fields matches the number of columns in the CSV file since we have no way of mapping field names
    // to column names.
    else if (!fields.isUnknown() && format.getHeader() == null) {
      if (!doFieldsMatchColumns(flowProcess, tap, fields)) {
        throw new RuntimeException("Fields count don't match header count");
      }
    }
    // If fields is unknown but the header is provided, validate that the number of columns in the header match the
    // number of columns in the CSV file and set the source fields as the provided header.
    else if (fields.isUnknown()) {
      if (validateHeaders(flowProcess, tap)) {
        setSourceFields(new Fields(format.getHeader()));
      } else {
        throw new RuntimeException("Headers count don't match column count in input file");
      }
    }
    // Finally, if both fields and headers are explicitly provided, validate that the provided headers match the number of
    // columns in the CSV file and validate that the provided fields are present in the headers. This will allow us to
    // intersect provided fields with real CSV columns.
    else {
      if (!(validateHeaders(flowProcess, tap) && areFieldsInFormatHeaders(fields))) {
        throw new RuntimeException("Headers or Fields are invalid");
      }
    }
    return getSourceFields();
  }

  /**
   * Detects the header fields.
   */
  @SuppressWarnings("unchecked")
  protected Fields detectHeader(FlowProcess<JobConf> flowProcess, Tap tap, boolean genericNames) {
    CSVRecord record = getHeaderRecord(flowProcess, tap);
    String[] fields = new String[record.size()];
    for (int i = 0; i < record.size(); i++) {
      if (genericNames) {
        fields[i] = String.format("col%d", i);
      } else {
        fields[i] = record.get(i);
      }
    }
    return new Fields(fields);
  }

  /**
   * Method to validate if input file has same number of columns as headers passed.
   */
  protected boolean validateHeaders(FlowProcess<JobConf> flowProcess, Tap tap) {
    CSVRecord headerRecord = getHeaderRecord(flowProcess, tap);
    int i = headerRecord.size();
    return headerRecord.size() == format.getHeader().length;
  }

  /**
   * Method to check if fields are present in format headers.
   */
  protected boolean areFieldsInFormatHeaders(Fields fields) {
    List<String> formatHeaders = new ArrayList<>();
    formatHeaders.addAll(Arrays.asList(this.format.getHeader()));
    for (int i = 0; i < fields.size(); i++) {
      if (!formatHeaders.contains(fields.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Method to validate Fields passed present in the headers.
   */
  protected boolean validateFields(FlowProcess<JobConf> flowProcess, Tap tap, Fields sourceFields) {

    CSVRecord headerRecord = getHeaderRecord(flowProcess, tap);

    if (sourceFields.size() > headerRecord.size()) {
      return false;
    }
    List<String> recordList = new ArrayList<String>();

    for (int i = 0; i < headerRecord.size(); i++) {
      recordList.add(headerRecord.get(i));
    }

    for (int i = 0; i < sourceFields.size(); i++) {
      if (!recordList.contains(sourceFields.get(i))) {
        return false;
      }
    }
    return true;

  }

  /**
   * Checks whether the given fields match the columns in the source header record.
   */
  protected boolean doFieldsMatchColumns(FlowProcess<JobConf> flowProcess, Tap tap, Fields sourceFields) {
    CSVRecord headerRecord = getHeaderRecord(flowProcess, tap);
    return sourceFields.size() == headerRecord.size();
  }

  /**
   * Reads the header record from the source file.
   */
  @SuppressWarnings("unchecked")
  private CSVRecord getHeaderRecord(FlowProcess<JobConf> flowProcess, Tap tap) {
    Tap textLine = new Hfs(new TextLine(new Fields("line")), tap.getFullIdentifier(flowProcess.getConfigCopy()));

    try (TupleEntryIterator iterator = textLine.openForRead(flowProcess)) {
      String line = iterator.next().getTuple().getString(0);
      boolean skipHeaderRecord = format.getSkipHeaderRecord();
      CSVRecord headerRecord = CSVParser.parse(line, format.withSkipHeaderRecord(false)).iterator().next();
      format.withSkipHeaderRecord(skipHeaderRecord);
      return headerRecord;
    } catch (IOException e) {
      throw new TapException(e);
    }
  }

  /**
   * Initializes header index info.
   */
  private void initIndices() {
    if (format.getHeader() != null && format.getHeader().length > 0) {
      indices = new HashMap<>();
      for (int i = 0; i < format.getHeader().length; i++) {
        indices.put(format.getHeader()[i], i);
      }
    }
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    conf.setInputFormat(CsvInputFormat.class);
    configureReaderFormat(format, conf);
  }

  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    initIndices();
    if (sourceCall.getContext() == null) {
      sourceCall.setContext(new Object[2]);
    }
    sourceCall.getContext()[0] = sourceCall.getInput().createKey();
    sourceCall.getContext()[1] = sourceCall.getInput().createValue();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    Object[] context = sourceCall.getContext();
    if (!sourceCall.getInput().next(context[0], context[1])) {
      return false;
    }

    TupleEntry entry = sourceCall.getIncomingEntry();

    ListWritable<Text> values = (ListWritable<Text>) context[1];

    Fields fields = getSourceFields();
    if (fields.size() != values.size()) {
      LongWritable pos = (LongWritable) context[0];
      Long position = pos.get();
      LOGGER.warn("failed to parse record, columns and values don't match at line: "   );
      if (strict) {
        throw new FlowException();
      } else {
        return true;
      }
    }
    for (int i = 0; i < fields.size(); i++) {
      int index = indices != null ? indices.get(fields.get(i).toString()) : i;
      Text value = values.get(index);
      if (value == null) {
        entry.setString(i, null);
      } else {
        entry.setString(i, value.toString());
      }
    }
    return true;
  }

  @Override
  public void presentSinkFields(FlowProcess<JobConf> flowProcess, Tap tap, Fields fields) {
    presentSinkFieldsInternal(fields);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(ListWritable.class);
    conf.setOutputFormat(CsvOutputFormat.class);
    configureWriterFormat(format, conf);
  }

  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    initIndices();
    sinkCall.setContext(new Object[2]);
    sinkCall.getContext()[0] = new LongWritable();
    sinkCall.getContext()[1] = new ListWritable<>(Text.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    ListWritable<Text> record = (ListWritable<Text>) sinkCall.getContext()[1];
    record.clear();

    TupleEntry entry = sinkCall.getOutgoingEntry();
    Tuple tuple = entry.getTuple();

    Fields fields = getSinkFields();
    for (int i = 0; i < fields.size(); i++) {
      int index = indices != null ? indices.get(fields.get(i).toString()) : i;
      if (record.size() < index) {
        for (int j = record.size(); j < index; j++) {
          record.add(null);
        }
      }

      Object value = tuple.getObject(i);
      if (value != null) {
        record.add(index, new Text(value.toString()));
      } else {
        record.add(index, null);
      }
    }

    sinkCall.getOutput().collect(null, record);
  }

  /**
   * Configures the Hadoop configuration for the given CSV format.
   */
  private void configureReaderFormat(CSVFormat format, Configuration conf) {
    conf.set(CsvOutputFormat.CHARSET, charset);

    // If the format header was explicitly provided by the user then forward it to the record reader. If skipHeaderRecord
    // is enabled then that indicates that field names were detected. We need to ensure that headers are defined in order
    // for the CSV reader to skip the header record.
    conf.setBoolean(CsvInputFormat.STRICT_MODE, strict);
    if (format.getHeader() != null) {
      conf.setStrings(CsvInputFormat.CSV_READER_COLUMNS, format.getHeader());
    } else if (format.getSkipHeaderRecord()) {
      Fields fields = getSourceFields();
      String[] columns = new String[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        columns[i] = fields.get(i).toString();
      }
      conf.setStrings(CsvInputFormat.CSV_READER_COLUMNS, columns);
    }

    conf.setBoolean(CsvInputFormat.CSV_READER_SKIP_HEADER, format.getSkipHeaderRecord());
    conf.set(CsvInputFormat.CSV_READER_DELIMITER, String.valueOf(format.getDelimiter()));

    if (format.getRecordSeparator() != null) {
      conf.set(CsvInputFormat.CSV_READER_RECORD_SEPARATOR, format.getRecordSeparator());
    }

    if (format.getQuoteCharacter() != null) {
      conf.set(CsvInputFormat.CSV_READER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));
    }

    if (format.getQuoteMode() != null) {
      conf.set(CsvInputFormat.CSV_READER_QUOTE_MODE, format.getQuoteMode().name());
    }

    if (format.getEscapeCharacter() != null) {
      conf.set(CsvInputFormat.CSV_READER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));
    }

    conf.setBoolean(CsvInputFormat.CSV_READER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvInputFormat.CSV_READER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());

    if (format.getNullString() != null) {
      conf.set(CsvInputFormat.CSV_READER_NULL_STRING, format.getNullString());
    }
  }

  /**
   * Configures the Hadoop configuration for the given CSV format.
   */
  private void configureWriterFormat(CSVFormat format, Configuration conf) {
    conf.set(CsvOutputFormat.CHARSET, charset);

    // Apache CSV doesn't really handle the skipHeaderRecord flag correctly when writing output. If the skip flag is set
    // and headers are configured, headers will always be written to the output. Since we always have headers and/or
    // fields configured, we need to use the skipHeaderRecord flag to determine whether headers should be written.
    if (!format.getSkipHeaderRecord()) {
      if (format.getHeader() != null && format.getHeader().length != 0) {
        conf.setStrings(CsvOutputFormat.CSV_WRITER_COLUMNS, format.getHeader());
      } else {
        Fields fields = getSinkFields();
        String[] columns = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          columns[i] = fields.get(i).toString();
        }
        conf.setStrings(CsvOutputFormat.CSV_WRITER_COLUMNS, columns);
      }
    }

    conf.setBoolean(CsvOutputFormat.CSV_WRITER_SKIP_HEADER, format.getSkipHeaderRecord());
    conf.set(CsvOutputFormat.CSV_WRITER_DELIMITER, String.valueOf(format.getDelimiter()));

    if (format.getRecordSeparator() != null) {
      conf.set(CsvOutputFormat.CSV_WRITER_RECORD_SEPARATOR, format.getRecordSeparator());
    }

    if (format.getQuoteCharacter() != null) {
      conf.set(CsvOutputFormat.CSV_WRITER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));
    }

    if (format.getQuoteMode() != null) {
      conf.set(CsvOutputFormat.CSV_WRITER_QUOTE_MODE, format.getQuoteMode().name());
    }

    if (format.getEscapeCharacter() != null) {
      conf.set(CsvOutputFormat.CSV_WRITER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));
    }

    conf.setBoolean(CsvOutputFormat.CSV_WRITER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvOutputFormat.CSV_WRITER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());

    if (format.getNullString() != null) {
      conf.set(CsvOutputFormat.CSV_WRITER_NULL_STRING, format.getNullString());
    }
  }

}
