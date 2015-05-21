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
import com.datascience.hadoop.*;
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
 * CSV scheme.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  private final CSVFormat format;

  public CsvScheme() {
    this(Fields.ALL, Fields.ALL, CSVFormat.DEFAULT);
  }

  public CsvScheme(CSVFormat format) {
    this(Fields.ALL, Fields.ALL, format);
  }

  public CsvScheme(Fields fields) {
    this(fields, fields, CSVFormat.DEFAULT);
  }

  public CsvScheme(Fields fields, CSVFormat format) {
    this(fields, fields, format);
  }

  public CsvScheme(Fields sourceFields, Fields sinkFields) {
    this(sourceFields, sinkFields, CSVFormat.DEFAULT);
  }

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
      conf.setStrings(CsvRecordReader.CSV_READER_COLUMNS, format.getHeader());
    } else if (format.getSkipHeaderRecord()) {
      Fields fields = getSourceFields();
      String[] columns = new String[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        columns[i] = fields.get(i).toString();
      }
      conf.setStrings(CsvRecordReader.CSV_READER_COLUMNS, columns);
    }

    conf.setBoolean(CsvRecordReader.CSV_READER_SKIP_HEADER, format.getSkipHeaderRecord());
    conf.set(CsvRecordReader.CSV_READER_DELIMITER, String.valueOf(format.getDelimiter()));

    if (format.getRecordSeparator() != null)
      conf.set(CsvRecordReader.CSV_READER_RECORD_SEPARATOR, format.getRecordSeparator());

    if (format.getQuoteCharacter() != null)
      conf.set(CsvRecordReader.CSV_READER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));

    if (format.getQuoteMode() != null)
      conf.set(CsvRecordReader.CSV_READER_QUOTE_MODE, format.getQuoteMode().name());

    if (format.getEscapeCharacter() != null)
      conf.set(CsvRecordReader.CSV_READER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));

    conf.setBoolean(CsvRecordReader.CSV_READER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvRecordReader.CSV_READER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());

    if (format.getNullString() != null)
      conf.set(CsvRecordReader.CSV_READER_NULL_STRING, format.getNullString());
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
        conf.setStrings(CsvRecordWriter.CSV_WRITER_COLUMNS, format.getHeader());
      } else {
        Fields fields = getSinkFields();
        String[] columns = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          columns[i] = fields.get(i).toString();
        }
        conf.setStrings(CsvRecordWriter.CSV_WRITER_COLUMNS, columns);
      }
    }

    conf.setBoolean(CsvRecordWriter.CSV_WRITER_SKIP_HEADER, format.getSkipHeaderRecord());
    conf.set(CsvRecordWriter.CSV_WRITER_DELIMITER, String.valueOf(format.getDelimiter()));

    if (format.getRecordSeparator() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_RECORD_SEPARATOR, format.getRecordSeparator());

    if (format.getQuoteCharacter() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));

    if (format.getQuoteMode() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_QUOTE_MODE, format.getQuoteMode().name());

    if (format.getEscapeCharacter() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));

    conf.setBoolean(CsvRecordWriter.CSV_WRITER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvRecordWriter.CSV_WRITER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());

    if (format.getNullString() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_NULL_STRING, format.getNullString());
  }

}
