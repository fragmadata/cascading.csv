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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * CSV record reader tests.
 *
 * @author amareeshbasanapalli
 */
public class CsvRecordReaderTest extends CSVHelper {

  @Before
  public void initialize() throws IOException {
    setUp();
  }

  @Test
  public void readerShouldReadAllRecords() throws IOException {
    testForReadAllRecords("/input/with-headers.txt", 3, 6);
  }

  @Test
  public void readerShouldReadAllRecordsFromCompressedFile() throws IOException {
    testForReadAllRecords("/input/with-headers.txt.gz", 3, 6);
  }

  public void testForReadAllRecords(String fileName, int expectedRowLength, int expectedRecordCount) throws IOException {
    CsvInputFormat inputFormat = createCSVInputFormat();
    File inputFile = getFile(fileName);
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());
    FileSplit split = createFileSplit(inputPath, 0, inputFile.length());

    RecordReader createdReader = createRecordReader(inputFormat, split);

    LongWritable key = new LongWritable();
    ListWritable<Text> value = new ListWritable<Text>(Text.class);

    int actualRecordCount = 0;

    long expectedKey = 0;

    while (createdReader.next(key, value)) {
      actualRecordCount++;

      assertEquals(expectedKey, key.get());
      expectedKey++;

      assertEquals(expectedRowLength, value.size());
    }
    assertEquals(expectedRecordCount, actualRecordCount);
  }


}
