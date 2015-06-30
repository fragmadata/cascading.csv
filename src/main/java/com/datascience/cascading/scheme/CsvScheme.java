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

import cascading.flow.FlowProcess;
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
 * provided to the constructor. In cases where fields or columns are not directly specified in the configuration, the
 * scheme may be able to detect the field names from the CSV header. See specific constructors for more information.
 * <p>
 * Internally, {@code CsvScheme} uses {@link com.datascience.hadoop.CsvInputFormat} and {@link com.datascience.hadoop.CsvOutputFormat}
 * for sourcing and sinking data respectively. These custom Hadoop input/output formats allow the scheme complete control
 * over the encoding, compression, parsing, and formatting of bytes beneath Cascading's abstractions.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  private final CSVFormat format;

  /**
   * Creates a new CSV scheme with {@link org.apache.commons.csv.CSVFormat#DEFAULT}.
   * <p>
   * Note that because the default {@link org.apache.commons.csv.CSVFormat} does not specify a header record or skip the
   * header record, this constructor will result in {@link cascading.tuple.Fields} being dynamically generated for sources.
   * Source fields will be generated with positional names, e.g. {@code col1}, {@code col2}, {@code col3}, etc.
   */
  public CsvScheme() {
    this(Fields.ALL, Fields.ALL, CSVFormat.DEFAULT);
  }

  /**
   * Creates a new CSV scheme with the given {@link org.apache.commons.csv.CSVFormat}.
   * <p>
   * When used as a source, if {@link org.apache.commons.csv.CSVFormat#getHeader()} is specified, the provided header
   * column names will be used in the output {@link cascading.tuple.Fields}. If no headers are specified and
   * {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()} is {@code true}, the scheme will attempt to automatically
   * detect the header record from the first record in the CSV input. If {@link org.apache.commons.csv.CSVFormat#getSkipHeaderRecord()}
   * is {@code false} and no header record is provided, positional {@link cascading.tuple.Fields} will be generated, e.g.
   * {@code col1}, {@code col2}, {@code col3}, etc.
   *
   * @param format The format with which to parse (source) or format (sink) records.
   */
  public CsvScheme(CSVFormat format) {
    this(Fields.ALL, Fields.ALL, format);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * The provided {@link cascading.tuple.Fields} will be used both in sourcing and sinking. For sources, this constructor
   * assumes that the provided number of fields match the number of columns in the source data. For sinks, only columns
   * with the provided field names will be written to the output target.
   *
   * @param fields The source and sink fields.
   */
  public CsvScheme(Fields fields) {
    this(fields, fields, CSVFormat.DEFAULT);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * The provided {@link cascading.tuple.Fields} will be used both in sourcing and sinking. For sources, this constructor
   * assumes that the provided number of fields match the number of columns in the source data. For sinks, only columns
   * with the provided field names will be written to the output target.
   * <p>
   * Note that regardless of whether the {@link org.apache.commons.csv.CSVFormat} provides a header record, the
   * source {@link cascading.tuple.Fields} take precedence, and the header record configured in the format
   * will be essentially ignored.
   *
   * @param fields The source and sink fields.
   * @param format The format with which to parse (source) or format (sink) records.
   */
  public CsvScheme(Fields fields, CSVFormat format) {
    this(fields, fields, format);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields}.
   * <p>
   * The provided {@link cascading.tuple.Fields} will be used both in sourcing and sinking. For sources, this constructor
   * assumes that the provided number of fields match the number of columns in the source data. For sinks, only columns
   * with the provided field names will be written to the output target.
   *
   * @param sourceFields The source fields.
   * @param sinkFields The sink fields.
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields) {
    this(sourceFields, sinkFields, CSVFormat.DEFAULT);
  }

  /**
   * Creates a new CSV scheme with the given source and sink {@link cascading.tuple.Fields} and a custom format with
   * which to read and write CSV data.
   * <p>
   * The provided {@link cascading.tuple.Fields} will be used both in sourcing and sinking. For sources, this constructor
   * assumes that the provided number of fields match the number of columns in the source data. For sinks, only columns
   * with the provided field names will be written to the output target.
   * <p>
   * Note that regardless of whether the {@link org.apache.commons.csv.CSVFormat} provides a header record,
   * the source {@link cascading.tuple.Fields} take precedence, and the header record configured in the format
   * will be essentially ignored.
   *
   * @param sourceFields The source fields.
   * @param sinkFields The sink fields.
   * @param format The format with which to parse (source) or format (sink) records.
   */
  public CsvScheme(Fields sourceFields, Fields sinkFields, CSVFormat format) {
    super();
    setSourceFields(sourceFields);
    setSinkFields(sinkFields);
    this.format = format;
  }

  @Override
  public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    if (!getSourceFields().isUnknown())
      return getSourceFields();

    if (format.getSkipHeaderRecord() && format.getHeader() == null) {
      setSourceFields(detectHeader(flowProcess, tap, false));
    } else if (format.getHeader() != null) {
      setSourceFields(new Fields(format.getHeader()));
    } else {
      setSourceFields(detectHeader(flowProcess, tap, true));
    }
    return getSourceFields();
  }

  /**
   * Detects the header fields.
   */
  @SuppressWarnings("unchecked")
  protected Fields detectHeader(FlowProcess<JobConf> flowProcess, Tap tap, boolean genericNames) {
    Tap textLine = new Hfs(new TextLine(new Fields("line")), tap.getFullIdentifier(flowProcess.getConfigCopy()));

    try (TupleEntryIterator iterator = textLine.openForRead(flowProcess)) {
      String line = iterator.next().getTuple().getString(0);
      CSVRecord record = CSVParser.parse(line, format).iterator().next();
      String[] fields = new String[record.size()];
      for (int i = 0; i < record.size(); i++) {
        if (genericNames) {
          fields[i] = String.format("col%d", i);
        } else {
          fields[i] = record.get(i);
        }
      }
      return new Fields(fields);
    } catch (IOException e) {
      throw new TapException(e);
    }
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    conf.setInputFormat(CsvInputFormat.class);
    configureReaderFormat(format, conf);
  }

  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    if (sourceCall.getContext() == null)
      sourceCall.setContext(new Object[2]);
    sourceCall.getContext()[0] = sourceCall.getInput().createKey();
    sourceCall.getContext()[1] = sourceCall.getInput().createValue();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    Object[] context = sourceCall.getContext();
    if (!sourceCall.getInput().next(context[0], context[1]))
      return false;

    TupleEntry entry = sourceCall.getIncomingEntry();
    ListWritable<Text> values = (ListWritable<Text>) context[1];
    for (int i = 0; i < values.size(); i++) {
      entry.setString(i, values.get(i).toString());
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
    for (Object value : tuple) {
      record.add(new Text(value.toString()));
    }

    sinkCall.getOutput().collect(null, record);
  }

  /**
   * Configures the Hadoop configuration for the given CSV format.
   */
  private void configureReaderFormat(CSVFormat format, Configuration conf) {
    // If the format header was explicitly provided by the user then forward it to the record reader. If skipHeaderRecord
    // is enabled then that indicates that field names were detected. We need to ensure that headers are defined in order
    // for the CSV reader to skip the header record.
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

    if (format.getRecordSeparator() != null)
      conf.set(CsvInputFormat.CSV_READER_RECORD_SEPARATOR, format.getRecordSeparator());

    if (format.getQuoteCharacter() != null)
      conf.set(CsvInputFormat.CSV_READER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));

    if (format.getQuoteMode() != null)
      conf.set(CsvInputFormat.CSV_READER_QUOTE_MODE, format.getQuoteMode().name());

    if (format.getEscapeCharacter() != null)
      conf.set(CsvInputFormat.CSV_READER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));

    conf.setBoolean(CsvInputFormat.CSV_READER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvInputFormat.CSV_READER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());

    if (format.getNullString() != null)
      conf.set(CsvInputFormat.CSV_READER_NULL_STRING, format.getNullString());
  }

  /**
   * Configures the Hadoop configuration for the given CSV format.
   */
  private void configureWriterFormat(CSVFormat format, Configuration conf) {
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

    if (format.getRecordSeparator() != null)
      conf.set(CsvOutputFormat.CSV_WRITER_RECORD_SEPARATOR, format.getRecordSeparator());

    if (format.getQuoteCharacter() != null)
      conf.set(CsvOutputFormat.CSV_WRITER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));

    if (format.getQuoteMode() != null)
      conf.set(CsvOutputFormat.CSV_WRITER_QUOTE_MODE, format.getQuoteMode().name());

    if (format.getEscapeCharacter() != null)
      conf.set(CsvOutputFormat.CSV_WRITER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));

    conf.setBoolean(CsvOutputFormat.CSV_WRITER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvOutputFormat.CSV_WRITER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());

    if (format.getNullString() != null)
      conf.set(CsvOutputFormat.CSV_WRITER_NULL_STRING, format.getNullString());
  }

}
