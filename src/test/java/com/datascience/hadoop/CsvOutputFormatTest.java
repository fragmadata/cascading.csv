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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * CSV output format tests.
 *
 * @author amareeshbasanapalli
 */
public class CsvOutputFormatTest  {

  Configuration conf;
  CsvHelper helper;
  JobConf jobConf;
  FileSystem fs;


  @Before
  public void initialize() throws IOException {
    helper = new CsvHelper();
    String[] columns = {"id", "first name", "last name"};
   conf= helper.buildConfiguration(",", "true", "\n", columns);

  }

  /**
   * Test for OutputFormat creates a correct instance of RecordWriter.
   */
  @Test
  public void shouldBeAbleToWriteCompressedFormat() throws IOException {
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.outputdir", "src/test/resources/output");
    conf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_00005_0");
    jobConf = new JobConf(conf);
    fs = FileSystem.get(conf);


    CsvOutputFormat format = ReflectionUtils.newInstance(CsvOutputFormat.class, conf);
    assertTrue(format.getRecordWriter(fs, jobConf, "output", null) instanceof CsvRecordWriter);
  }

  /**
   * Test for OutputFormat creates a correct instance of RecordWriter when compressed file is passed.
   */
  @Test
  public void shouldBeAbleToWriteNonCompressedFormat() throws IOException {
    conf.set("mapreduce.output.fileoutputformat.compress", "false");
    conf.set("mapreduce.output.fileoutputformat.outputdir", "src/test/resources/output");
    conf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_00005_0");
    jobConf = new JobConf(conf);
    fs = FileSystem.get(conf);


    CsvOutputFormat format = ReflectionUtils.newInstance(CsvOutputFormat.class, conf);
    assertTrue(format.getRecordWriter(fs, jobConf, "output", null) instanceof CsvRecordWriter);
  }

  @After
  public void tearDown() throws IOException{
    FileUtils.deleteDirectory(new File("src/test/resources/output"));
  }
}
