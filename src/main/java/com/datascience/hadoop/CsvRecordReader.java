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
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;

/**
 * CSV record reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvRecordReader implements RecordReader<LongWritable, ListWritable<Text>> {
  private final Text[] cache = new Text[1024];
  private final CSVParser parser;
  private final Iterator<CSVRecord> iterator;
  private final float start;
  private long position;
  private long end;

  public CsvRecordReader(InputStream is, CSVFormat format, long start, long end) throws IOException {
    this.start = start;
    this.end = end;
    Reader isr = new InputStreamReader(is);
    parser = new CSVParser(isr, format, start, start);
    iterator = parser.iterator();
  }

  @Override
  public boolean next(LongWritable key, ListWritable<Text> value) throws IOException {
    value.clear();
    if (position < end && iterator.hasNext()) {
      CSVRecord record = iterator.next();
      key.set(record.getRecordNumber());
      for (int i = 0; i < record.size(); i++) {
        Text text = cache[i];
        if (text == null) {
          text = new Text();
          cache[i] = text;
        }
        text.set(record.get(i));
        value.add(text);
      }
      position = record.getCharacterPosition();
      return true;
    }
    return false;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public ListWritable<Text> createValue() {
    return new ListWritable<>(Text.class);
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public float getProgress() throws IOException {
    return Math.min(1.0f, (float) position - start / (end - start));
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
