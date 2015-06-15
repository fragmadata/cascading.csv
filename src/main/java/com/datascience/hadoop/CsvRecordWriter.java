package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * CSV record writer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvRecordWriter implements RecordWriter<LongWritable, ListWritable<Text>> {
  private final CSVPrinter out;

  public CsvRecordWriter(OutputStream os, CSVFormat format) throws IOException {
    out = new CSVPrinter(new OutputStreamWriter(os), format);
  }

  @Override
  public void write(LongWritable key, ListWritable<Text> value) throws IOException {
    out.printRecord(value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    out.close();
  }

}
