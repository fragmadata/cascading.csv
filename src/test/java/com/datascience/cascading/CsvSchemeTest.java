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
package com.datascience.cascading;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;
import com.datascience.cascading.scheme.CsvScheme;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * CSV scheme test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvSchemeTest {

  /**
   * Tests the CSV scheme source with headers.
   */
  @Test
  public void testUncompressedCsvSourceWithHeaders() throws Exception {
    testCsvSourceWithHeaders("src/test/resources/input/with-headers.txt");
  }

  /**
   * Tests the CSV scheme source with headers.
   */
  @Test
  public void testCompressedCsvSourceWithHeaders() throws Exception {
    testCsvSourceWithHeaders("src/test/resources/input/with-headers.txt.gz");
  }

  /**
   * Tests the CSV scheme source with headers.
   */
  private void testCsvSourceWithHeaders(String inputPath) throws Exception {
    String sinkPath = "src/test/resources/output/source-with-headers";
    String expectedPath = "src/test/resources/expected/with-headers.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat(',')
      .withQuote('"')
      .withHeader("id", "first name", "last name")
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withEscape('\\')
      .withRecordSeparator('\n');

    testScheme(inputPath, sourceFormat, sinkPath, sinkFormat, expectedPath, true);
  }

  /**
   * Tests the CSV scheme source with detected headers.
   */
  @Test
  public void testUncompressedCsvSourceDetectHeaders() throws Exception {
    testCsvSourceDetectHeaders("src/test/resources/input/with-headers.txt");
  }

  /**
   * Tests the CSV scheme source with detected headers.
   */
  @Test
  public void testCompressedCsvSourceDetectHeaders() throws Exception {
    testCsvSourceDetectHeaders("src/test/resources/input/with-headers.txt.gz");
  }

  /**
   * Tests the CSV scheme source with detected headers.
   */
  private void testCsvSourceDetectHeaders(String inputPath) throws Exception {
    String sinkPath = "src/test/resources/output/source-detect-headers";
    String expectedPath = "src/test/resources/expected/with-headers.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat(',')
      .withQuote('"')
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withEscape('\\')
      .withRecordSeparator('\n');

    testScheme(inputPath, sourceFormat, sinkPath, sinkFormat, expectedPath, true);
  }

  /**
   * Tests the CSV scheme source with generated headers.
   */
  @Test
  public void testCsvSourceGenerateHeaders() throws Exception {
    String sourcePath = "src/test/resources/input/without-headers.txt";
    String sinkPath = "src/test/resources/output/source-generate-headers";
    String expectedPath = "src/test/resources/expected/with-generated-headers.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat(',')
      .withQuote('"')
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withEscape('\\')
      .withRecordSeparator('\n');

    testScheme(sourcePath, sourceFormat, sinkPath, sinkFormat, expectedPath, true);
  }

  /**
   * Tests that strict parsing fails on a bad CSV source.
   */
  @Test(expected = RuntimeException.class)
  public void testBadCsvSourceStrict() throws Exception {
    String sourcePath = "src/test/resources/input/bad-without-headers.txt";
    String sinkPath = "src/test/resources/output/bad-fail-headers";
    String expectedPath = "src/test/resources/expected/bad-generated-headers.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat('\t')
      .withQuote('"')
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withEscape('\\')
      .withRecordSeparator('\n');

    testScheme(sourcePath, sourceFormat, sinkPath, sinkFormat, expectedPath, true);
  }

  /**
   * Tests that strict parsing fails on a bad CSV source.
   */
  @Test
  public void testBadCsvSourceNotStrict() throws Exception {
    String sourcePath = "src/test/resources/input/bad-without-headers.txt";
    String sinkPath = "src/test/resources/output/bad-generate-headers";
    String expectedPath = "src/test/resources/expected/bad-generated-headers.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat('\t')
      .withQuote('"')
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withEscape('\\')
      .withRecordSeparator('\n');

    testScheme(sourcePath, sourceFormat, sinkPath, sinkFormat, expectedPath, false);
  }

  /**
   * Tests the CSV scheme reading and writing nulls.
   */
  @Test
  public void testCsvNulls() throws Exception {
    String sourcePath = "src/test/resources/input/with-nulls.txt";
    String sinkPath = "src/test/resources/output/with-nulls";
    String expectedPath = "src/test/resources/expected/with-nulls.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat(',')
      .withQuote('"')
      .withHeader("id", "first name", "last name")
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n')
      .withNullString("\\N");

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withEscape('\\')
      .withRecordSeparator('\n')
      .withNullString("null");

    testScheme(sourcePath, sourceFormat, sinkPath, sinkFormat, expectedPath, true);
  }

  /**
   * Tests the CSV scheme sink without headers.
   */
  @Test
  public void testCsvSinkWithHeaders() throws Exception {
    String sourcePath = "src/test/resources/input/with-headers.txt";
    String sinkPath = "src/test/resources/output/sink-with-headers";
    String expectedPath = "src/test/resources/expected/with-headers.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat(',')
      .withQuote('"')
      .withHeader("id", "first name", "last name")
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withEscape('\\')
      .withRecordSeparator('\n');

    testScheme(sourcePath, sourceFormat, sinkPath, sinkFormat, expectedPath, true);
  }

  /**
   * Tests the CSV scheme sink without headers.
   */
  @Test
  public void testCsvSinkWithoutHeaders() throws Exception {
    String sourcePath = "src/test/resources/input/with-headers.txt";
    String sinkPath = "src/test/resources/output/sink-without-headers";
    String expectedPath = "src/test/resources/expected/without-headers.txt";

    CSVFormat sourceFormat = CSVFormat.newFormat(',')
      .withQuote('"')
      .withHeader("id", "first name", "last name")
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat sinkFormat = CSVFormat.newFormat('\t')
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n');

    testScheme(sourcePath, sourceFormat, sinkPath, sinkFormat, expectedPath, true);
  }

  /**
   * Tests a source and sink scheme together.
   */
  private void testScheme(String sourcePath, CSVFormat sourceFormat, String sinkPath, CSVFormat sinkFormat, String expectedPath, boolean strict) throws Exception {
    FlowConnector connector = new Hadoop2MR1FlowConnector();

    Tap source = new Hfs(new CsvScheme(sourceFormat, strict), sourcePath);
    Tap sink = new Hfs(new CsvScheme(sinkFormat), sinkPath);

    Pipe pipe = new Pipe("pipe");

    connector.connect(source, sink, pipe).complete();

    testPaths(sinkPath, expectedPath);
  }

  /**
   * Tests the content of an output path against the given expected path.
   */
  @SuppressWarnings("unchecked")
  private void testPaths(String actual, String expected) throws Exception {
    Tap outputTest = new Hfs(new TextLine(), actual);
    Tap expectedTest = new Hfs(new TextLine(), expected);

    FlowProcess outputProcess = new HadoopFlowProcess(new JobConf(new Configuration()));
    FlowProcess expectedProcess = new HadoopFlowProcess(new JobConf(new Configuration()));

    TupleEntryIterator outputIterator = outputTest.openForRead(outputProcess);
    TupleEntryIterator expectedIterator = expectedTest.openForRead(expectedProcess);

    List<String> outputList = new ArrayList<>();
    while (outputIterator.hasNext()) {
      outputList.add(outputIterator.next().getTuple().getString(1));
    }

    List<String> expectedList = new ArrayList<>();
    while (expectedIterator.hasNext()) {
      expectedList.add(expectedIterator.next().getTuple().getString(1));
    }

    assertTrue(outputList.equals(expectedList));
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File("src/test/resources/output"));
  }

}
