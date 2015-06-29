package com.datascience.hadoop;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileSystem;

/**
 * Created by amareeshbasanapalli on 6/23/15.
 */
public class CSVOutputFormatTest extends CSVHelper {

  @Before
  public void initialize() throws IOException {
    String [] columns = {"id","first name", "last name" };
    setUp(",", "true", "\n", columns) ;
  }

  @Test
  public void shouldBeAbleToWriteCompressedFormat() throws IOException {
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.outputdir", "file/output");
    conf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_00005_0");
    config = new JobConf(conf);

    CsvOutputFormat format = ReflectionUtils.newInstance(CsvOutputFormat.class, conf);
    assertTrue(format.getRecordWriter(fs, config, "output", null) instanceof CsvRecordWriter);

  }

  @Test
  public void shouldBeAbleToWriteNonCompressedFormat() throws IOException {
    conf.set("mapreduce.output.fileoutputformat.compress", "false");
    conf.set("mapreduce.output.fileoutputformat.outputdir", "file/output");
    conf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_00005_0");
    config = new JobConf(conf);

    CsvOutputFormat format = ReflectionUtils.newInstance(CsvOutputFormat.class, conf);
    assertTrue(format.getRecordWriter(fs, config, "output", null) instanceof CsvRecordWriter);

  }
}
