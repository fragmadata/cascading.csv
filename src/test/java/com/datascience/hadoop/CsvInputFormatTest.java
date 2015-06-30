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
import org.apache.hadoop.mapred.FileSplit;
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
public class CsvInputFormatTest extends CsvHelper {

  @Before
  public void initialize() throws IOException {
    String [] columns = {"id","first name", "last name" };
    setUp(",", "true", "\n", columns) ;
  }

  /**
   * Tests if CSVInputFormat returns a valid Record Reader.
   */
  @Test
  public void formatShouldReturnValidRecordReader() throws IOException {
    CsvInputFormat format = createCSVInputFormat();
    File inputFile = getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());
    FileSplit split = createFileSplit(inputPath, 0, inputFile.length());
    assertTrue(createRecordReader(format, split) instanceof CsvRecordReader);
  }

  /**
   * Tests to see if compressed files support seek.
   */
  @Test(expected = IOException.class)
  public void nonSplittableCodecShouldNotSupportSeek() throws IOException {
    CsvInputFormat format = ReflectionUtils.newInstance(CsvInputFormat.class, conf);

    File inputFile = getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());

    FileSplit split = createFileSplit(inputPath, 10, 50);
    createRecordReader(format, split);
  }

  /**
   * Tests to see if compressed files support splits.
   */
  @Test
  public void compressedFilesShouldNeverSplit() throws IOException {
    CsvInputFormat format = ReflectionUtils.newInstance(CsvInputFormat.class, conf);
    format.configure(config);
    File inputFile = getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());

    assertFalse(format.isSplitable(fs, inputPath));
    fs.close();
  }

}
