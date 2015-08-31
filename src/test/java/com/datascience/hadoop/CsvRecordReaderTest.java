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

import cascading.tap.TapException;
import com.datascience.util.CsvParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CSV record reader tests.
 *
 * @author amareeshbasanapalli
 */
public class CsvRecordReaderTest  {

  Configuration conf;
  CsvHelper helper;
  JobConf jobConf;
  FileSystem fs;

  @Before
  public void initialize() throws IOException {
    helper = new CsvHelper();
    String[] columns = {"id", "first name", "last name"};
    conf= helper.buildConfiguration(",", "true", "\n", columns);
    jobConf = new JobConf(conf);
    fs = FileSystem.get(conf);
  }

  /**
   * Tests if RecordReader reads all records from regular input file.
   */
  @Test
  public void readerShouldReadAllRecords() throws IOException {
    testForReadAllRecords("/input/with-headers.txt", 3, 6);
  }

  /**
   * Tests if RecordReader reads all records from compressed input file.
   */
  @Test
  public void readerShouldReadAllRecordsFromCompressedFile() throws IOException {
    testForReadAllRecords("/input/with-headers.txt.gz", 3, 6);
  }

  @Test
  public void readingExtraColumnsWhenNotStrict() throws IOException{

    helper = new CsvHelper();
    String[] columns = {"id", "first name", "last name","city","zip"};
    conf= helper.buildConfiguration("\t", "true", "\n", columns);
    conf.setBoolean(CsvInputFormat.STRICT_MODE, false);
    jobConf = new JobConf(conf);
    fs = FileSystem.get(conf);
    testForReadAllRecords("/input/with-extra-columns.txt", 7,7);
  }

  @Test(expected= CsvParseException.class)
  public void readingExtraColumnsWhenStrict() throws IOException{

    helper = new CsvHelper();
    String[] columns = {"id", "first name", "last name","city","zip"};
    conf= helper.buildConfiguration("\t", "true", "\n", columns);
    conf.setBoolean(CsvInputFormat.STRICT_MODE, true);
    jobConf = new JobConf(conf);
    fs = FileSystem.get(conf);
    testForReadAllRecords("/input/with-extra-columns.txt", 5,5);
  }




  @Test(expected=RuntimeException.class)
  public void readerShouldNotParseErrorRecords()throws IOException{
    conf.set(CsvInputFormat.CSV_READER_QUOTE_CHARACTER,"\"");

    jobConf = new JobConf(conf);
    fs = FileSystem.get(conf);

    testForReadAllRecords("/input/skipped-lines.txt", 3, 4);
  }



  /**
   * Helper function that iterates through Record Reader and assert values.
   */
  public void testForReadAllRecords(String fileName, int expectedRowLength, int expectedRecordCount) throws IOException {
    CsvInputFormat inputFormat = helper.createCSVInputFormat(conf);
    File inputFile = helper.getFile(fileName);
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());
    FileSplit split = helper.createFileSplit(inputPath, 0, inputFile.length());

    RecordReader createdReader = helper.createRecordReader(inputFormat, split, jobConf);

    LongWritable key = new LongWritable();
    ListWritable<Text> value = new ListWritable<Text>(Text.class);

    int actualRecordCount = 0;

    long expectedKey = 0;

    while (createdReader.next(key, value)) {
      actualRecordCount++;

      expectedKey++;

      assertTrue((value.size() - expectedRowLength) < 1);
    }
    assertEquals(expectedRecordCount, actualRecordCount);
  }


}
