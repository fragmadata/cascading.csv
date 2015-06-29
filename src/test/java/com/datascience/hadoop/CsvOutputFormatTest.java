package com.datascience.hadoop;

import org.apache.commons.io.FileUtils;
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
public class CsvOutputFormatTest extends CsvHelper {

  @Before
  public void initialize() throws IOException {
    String[] columns = {"id", "first name", "last name"};
    setUp(",", "true", "\n", columns);
  }

  /**
   * Test for OutputFormat creates a correct instance of RecordWriter.
   */
  @Test
  public void shouldBeAbleToWriteCompressedFormat() throws IOException {
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.outputdir", "src/test/resources/output");
    conf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_00005_0");
    config = new JobConf(conf);

    CsvOutputFormat format = ReflectionUtils.newInstance(CsvOutputFormat.class, conf);
    assertTrue(format.getRecordWriter(fs, config, "output", null) instanceof CsvRecordWriter);
  }

  /**
   * Test for OutputFormat creates a correct instance of RecordWriter when compressed file is passed.
   */
  @Test
  public void shouldBeAbleToWriteNonCompressedFormat() throws IOException {
    conf.set("mapreduce.output.fileoutputformat.compress", "false");
    conf.set("mapreduce.output.fileoutputformat.outputdir", "src/test/resources/output");
    conf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_00005_0");
    config = new JobConf(conf);

    CsvOutputFormat format = ReflectionUtils.newInstance(CsvOutputFormat.class, conf);
    assertTrue(format.getRecordWriter(fs, config, "output", null) instanceof CsvRecordWriter);
  }

  @After
  public void tearDown() throws IOException{
    FileUtils.deleteDirectory(new File("src/test/resources/output"));
  }
}
