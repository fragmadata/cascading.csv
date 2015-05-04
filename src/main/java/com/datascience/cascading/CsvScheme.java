package com.datascience.cascading;

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
  private CSVFormat format;

  public CsvScheme() {
    super();
  }

  public CsvScheme(CSVFormat format) {
    super();
    this.format = format;
  }

  public CsvScheme(Fields sourceFields) {
    this(sourceFields, CSVFormat.DEFAULT);
  }

  public CsvScheme(Fields sourceFields, CSVFormat format) {
    super(sourceFields);
    this.format = format;
  }

  public CsvScheme(Fields sourceFields, Fields sinkFields) {
    this(sourceFields, sinkFields, CSVFormat.DEFAULT);
  }

  public CsvScheme(Fields sourceFields, Fields sinkFields, CSVFormat format) {
    super(sourceFields, sinkFields);
    this.format = format;
  }

  @Override
  public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    if (format.getSkipHeaderRecord() && getSourceFields().isUnknown())
      setSourceFields(detectHeader(flowProcess, tap));
    return getSourceFields();
  }

  /**
   * Detects the header fields.
   */
  @SuppressWarnings("unchecked")
  protected Fields detectHeader(FlowProcess<JobConf> flowProcess, Tap tap) {
    Tap textLine = new Hfs(new TextLine(new Fields("line")), tap.getFullIdentifier(flowProcess.getConfigCopy()));

    try (TupleEntryIterator iterator = textLine.openForRead(flowProcess)) {
      String line = iterator.next().getTuple().getString(0);
      CSVRecord record = CSVParser.parse(line, format).iterator().next();
      String[] fields = new String[record.size()];
      for (int i = 0; i < record.size(); i++) {
        fields[i] = record.get(i);
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
  public Fields retrieveSinkFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    Fields fields = getSinkFields();
    if (!format.getSkipHeaderRecord() && (format.getHeader() == null || format.getHeader().length == 0)) {
      String[] columns = new String[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        columns[i] = fields.get(i).toString();
      }
      format = format.withHeader(columns);
    }
    return fields;
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
  private static void configureReaderFormat(CSVFormat format, Configuration conf) {
    if (format.getHeader() != null)
      conf.setStrings(CsvRecordReader.CSV_READER_COLUMNS, format.getHeader());
    conf.setBoolean(CsvRecordReader.CSV_READER_SKIP_HEADER, format.getSkipHeaderRecord());
    conf.set(CsvRecordReader.CSV_READER_DELIMITER, String.valueOf(format.getDelimiter()));
    if (format.getRecordSeparator() != null)
      conf.set(CsvRecordReader.CSV_READER_RECORD_SEPARATOR, format.getRecordSeparator());
    conf.set(CsvRecordReader.CSV_READER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));
    if (format.getQuoteMode() != null)
      conf.set(CsvRecordReader.CSV_READER_QUOTE_MODE, format.getQuoteMode().name());
    conf.set(CsvRecordReader.CSV_READER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));
    conf.setBoolean(CsvRecordReader.CSV_READER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvRecordReader.CSV_READER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());
    if (format.getNullString() != null)
      conf.set(CsvRecordReader.CSV_READER_NULL_STRING, format.getNullString());
  }

  /**
   * Configures the Hadoop configuration for the given CSV format.
   */
  private static void configureWriterFormat(CSVFormat format, Configuration conf) {
    if (format.getHeader() != null)
      conf.setStrings(CsvRecordWriter.CSV_WRITER_COLUMNS, format.getHeader());
    conf.setBoolean(CsvRecordWriter.CSV_WRITER_SKIP_HEADER, format.getSkipHeaderRecord());
    conf.set(CsvRecordWriter.CSV_WRITER_DELIMITER, String.valueOf(format.getDelimiter()));
    if (format.getRecordSeparator() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_RECORD_SEPARATOR, format.getRecordSeparator());
    conf.set(CsvRecordWriter.CSV_WRITER_QUOTE_CHARACTER, String.valueOf(format.getQuoteCharacter()));
    if (format.getQuoteMode() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_QUOTE_MODE, format.getQuoteMode().name());
    conf.set(CsvRecordWriter.CSV_WRITER_ESCAPE_CHARACTER, String.valueOf(format.getEscapeCharacter()));
    conf.setBoolean(CsvRecordWriter.CSV_WRITER_IGNORE_EMPTY_LINES, format.getIgnoreEmptyLines());
    conf.setBoolean(CsvRecordWriter.CSV_WRITER_IGNORE_SURROUNDING_SPACES, format.getIgnoreSurroundingSpaces());
    if (format.getNullString() != null)
      conf.set(CsvRecordWriter.CSV_WRITER_NULL_STRING, format.getNullString());
  }

}
