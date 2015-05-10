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
import junit.framework.TestCase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV scheme test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvSchemeTest extends TestCase {

  /**
   * Tests the CSV scheme.
   */
  public void testCsvScheme() throws Exception {
    String inputPath = "src/test/resources/input/test1.txt";
    String outputPath = "src/test/resources/output/test1";
    String expectedPath = "src/test/resources/expected/test1.txt";

    FlowConnector connector = new Hadoop2MR1FlowConnector();

    CSVFormat inputFormat = CSVFormat.newFormat(',')
      .withQuote('"')
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n');

    CSVFormat outputFormat = CSVFormat.newFormat('\t')
      .withSkipHeaderRecord()
      .withEscape('\\')
      .withRecordSeparator('\n');

    Tap source = new Hfs(new CsvScheme(inputFormat), inputPath);
    Tap sink = new Hfs(new CsvScheme(outputFormat), outputPath);

    Pipe pipe = new Pipe("pipe");

    connector.connect(source, sink, pipe).complete();

    testPaths(outputPath, expectedPath);
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

  @Override
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File("src/test/resources/output"));
  }

}
