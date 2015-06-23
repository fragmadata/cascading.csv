package com.datascience.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Created by amareeshbasanapalli on 6/23/15.
 */
public abstract class CSVHelper {

  Configuration conf;
  JobConf config;
  FileSystem fs;

  public void setUp() throws IOException {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set(CsvInputFormat.CSV_READER_DELIMITER, ",");
    conf.set(CsvInputFormat.CSV_READER_SKIP_HEADER, "true");
    conf.set(CsvInputFormat.CSV_READER_RECORD_SEPARATOR, "\n");
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    conf.setStrings(CsvInputFormat.CSV_READER_COLUMNS, "id", "first name", "last name");
    conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.SnappyCodec,org.apache.hadoop.io.compress.Lz4Codec");

    config = new JobConf(conf);
    fs = FileSystem.get(conf);
  }


  public CsvInputFormat createCSVInputFormat() {

    return ReflectionUtils.newInstance(CsvInputFormat.class, conf);

  }

  public File getFile(String path) {
    URL url = this.getClass().getResource(path);
    return new File(url.getFile());

  }


  public RecordReader createRecordReader(InputFormat format, InputSplit split) throws IOException {

    Reporter reporter = Reporter.NULL;
    JobConf jobConf = new JobConf(config);
    return format.getRecordReader(split, jobConf, reporter);

  }

  public FileSplit createFileSplit(Path path, long start, long end) {

    return new FileSplit(path, start, end, new String[0]);

  }


}
