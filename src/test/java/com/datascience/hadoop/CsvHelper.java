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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * CSV test helper.
 *
 * @author amareeshbasanapalli
 */
public class CsvHelper {


  public Configuration getDefaultConf(){
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set(CsvInputFormat.CSV_READER_DELIMITER, ",");
    conf.set(CsvInputFormat.CSV_READER_SKIP_HEADER, "true");
    conf.set(CsvInputFormat.CSV_READER_RECORD_SEPARATOR, "\n");
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.SnappyCodec,org.apache.hadoop.io.compress.Lz4Codec");
    conf.set(CsvInputFormat.CSV_READER_QUOTE_CHARACTER,"\"");
    conf.setBoolean(CsvInputFormat.STRICT_MODE, false);
   return conf;

  }

  public Configuration buildConfiguration(String delimiter, String skipHeader, String recordSeparator, String[] columns){
     Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set(CsvInputFormat.CSV_READER_DELIMITER, delimiter);
    conf.set(CsvInputFormat.CSV_READER_SKIP_HEADER, skipHeader);
    conf.set(CsvInputFormat.CSV_READER_RECORD_SEPARATOR, recordSeparator);
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    conf.setStrings(CsvInputFormat.CSV_READER_COLUMNS, columns );
    conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.SnappyCodec,org.apache.hadoop.io.compress.Lz4Codec");
    return conf;
  }

  public CsvInputFormat createCSVInputFormat(Configuration conf) {
    return ReflectionUtils.newInstance(CsvInputFormat.class, conf);
  }

  public File getFile(String path) {
    URL url = this.getClass().getResource(path);
    return new File(url.getFile());
  }


  public RecordReader createRecordReader(InputFormat format, InputSplit split, JobConf jobConf) throws IOException {
    Reporter reporter = Reporter.NULL;
    return format.getRecordReader(split, jobConf, reporter);
  }

  public FileSplit createFileSplit(Path path, long start, long end) {

    return new FileSplit(path, start, end, new String[0]);

  }


}
