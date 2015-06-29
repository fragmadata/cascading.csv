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
public class CsvRecordReaderTest extends CsvHelper {

  @Before
  public void initialize() throws IOException {
    setUp();
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

  /**
   * Helper function that iterates through Record Reader and assert values.
   */
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
