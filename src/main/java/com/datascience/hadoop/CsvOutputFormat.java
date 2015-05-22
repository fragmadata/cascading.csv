package com.datascience.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * CSV output format.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvOutputFormat extends FileOutputFormat<LongWritable, ListWritable<Text>> {

  @Override
  public RecordWriter<LongWritable, ListWritable<Text>> getRecordWriter(FileSystem fileSystem, JobConf conf, String name, Progressable progress) throws IOException {
    Path path = getTaskOutputPath(conf, name);
    if (fileSystem == null) {
      fileSystem = path.getFileSystem(conf);
    }
    return new CsvRecordWriter(fileSystem, path, conf);
  }

}
