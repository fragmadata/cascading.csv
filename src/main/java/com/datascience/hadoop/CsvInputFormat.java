package com.datascience.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * CSV input format.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvInputFormat extends FileInputFormat<LongWritable, ListWritable<Text>> {

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return true;
  }

  @Override
  public RecordReader<LongWritable, ListWritable<Text>> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
    return new CsvRecordReader((FileSplit) inputSplit, conf);
  }

}
