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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CSV input format tests.
 *
 * @author amareeshbasanapalli
 */
public class CsvInputFormatTest {

  CsvHelper helper;
  Configuration conf;
  JobConf jobConf;
  FileSystem fs;

  @Before
  public void initialize() throws IOException {
    helper = new CsvHelper() ;
    String [] columns = {"id","first name", "last name" };
    conf = helper.buildConfiguration(",", "true", "\n", columns) ;
    jobConf = new JobConf(conf);
    fs = FileSystem.get(conf);
  }

  /**
   * Tests if CSVInputFormat returns a valid Record Reader.
   */
  @Test
  public void formatShouldReturnValidRecordReader() throws IOException {
    JobConf jobConf = new JobConf(conf);
    CsvInputFormat format = helper.createCSVInputFormat(conf);
    File inputFile = helper.getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());
    FileSplit split = helper.createFileSplit(inputPath, 0, inputFile.length());
    assertTrue(helper.createRecordReader(format, split, jobConf) instanceof CsvRecordReader);
  }

  /**
   * Tests to see if compressed files support seek.
   */
  @Test(expected = IOException.class)
  public void nonSplittableCodecShouldNotSupportSeek() throws IOException {


    CsvInputFormat format = ReflectionUtils.newInstance(CsvInputFormat.class, conf);

    File inputFile = helper.getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());

    FileSplit split = helper.createFileSplit(inputPath, 10, 50);
    helper.createRecordReader(format, split, jobConf);
  }

  /**
   * Tests to see if compressed files support splits.
   */
  @Test
  public void compressedFilesShouldNeverSplit() throws IOException {
    CsvInputFormat format = ReflectionUtils.newInstance(CsvInputFormat.class, conf);
    format.configure(jobConf);
    File inputFile = helper.getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());

    assertFalse(format.isSplitable(fs, inputPath));
    fs.close();
  }

}
