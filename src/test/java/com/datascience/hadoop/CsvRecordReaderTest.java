package com.datascience.hadoop;


import org.apache.commons.csv.CSVFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.ConsoleAppender;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by amareeshbasanapalli on 6/19/15.
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
