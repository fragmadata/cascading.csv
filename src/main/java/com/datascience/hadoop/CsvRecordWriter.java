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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * CSV record writer.
 * <p>
 * The CSV record writer handles writing {@link com.datascience.hadoop.ListWritable} instances out to CSV files on behalf
 * of {@link com.datascience.hadoop.CsvOutputFormat}. Internally, it uses a {@link org.apache.commons.csv.CSVPrinter}
 * configured by its parent {@link org.apache.hadoop.mapred.OutputFormat} to write CSV records out to the provided
 * {@link java.io.OutputStream}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvRecordWriter implements RecordWriter<LongWritable, ListWritable<Text>> {
  private final CSVPrinter out;

  public CsvRecordWriter(OutputStream os, CSVFormat format) throws IOException {
    out = new CSVPrinter(new OutputStreamWriter(os), format);
  }

  @Override
  public void write(LongWritable key, ListWritable<Text> value) throws IOException {
    out.printRecord(value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    out.close();
  }

}
