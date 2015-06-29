package com.datascience.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import java.io.File;
import java.io.IOException;

/**
 * Created by amareeshbasanapalli on 6/22/15.
 */
public class CSVInputFormatTest extends CSVHelper {


  @Before
  public void initialize() throws IOException {
    String [] columns = {"id","first name", "last name" };
    setUp(",", "true", "\n", columns) ;
  }

  @Test
  public void formatShoudReturnValidRecordReader() throws IOException {

    CsvInputFormat format = createCSVInputFormat();
    File inputFile = getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());
    FileSplit split = createFileSplit(inputPath, 0, inputFile.length());
    assertTrue(createRecordReader(format, split) instanceof CsvRecordReader);

  }

  @Test(expected = IOException.class)
  public void nonSplittableCodecShouldNotSupportSeek() throws IOException {

    CsvInputFormat format = ReflectionUtils.newInstance(CsvInputFormat.class, conf);

    File inputFile = getFile("/input/with-headers.txt.gz");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());

    FileSplit split = createFileSplit(inputPath, 10, 50);
    createRecordReader(format, split);

  }


  @Test
  public void regularFilesShouldBeAbleToSplit() throws IOException {


    CsvInputFormat format = ReflectionUtils.newInstance(CsvInputFormat.class, conf);
    format.configure(config);
    File inputFile = getFile("/input/with-headers.txt");
    Path inputPath = new Path(inputFile.getAbsoluteFile().toURI().toString());

    assertTrue(format.isSplitable(fs, inputPath));

  }

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
