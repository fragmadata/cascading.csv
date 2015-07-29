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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * CSV record reader.
 * <p>
 * The CSV record reader handles reading CSV records from an {@link java.io.InputStream} on behalf of a
 * {@link com.datascience.hadoop.CsvInputFormat}. Internally, the record reader uses a {@link org.apache.commons.csv.CSVParser}
 * to iterate through the provided stream and parse records.
 * <p>
 * This record reader is agnostic about the compression of the provided input stream. Additionally, note that this
 * reader does <em>not</em> currently support input splits. It instead assumes that all provided
 * {@link java.io.InputStream streams} are representative of a full data set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CsvRecordReader implements RecordReader<LongWritable, ListWritable<Text>> {
  private static final Logger LOGGER = Logger.getLogger(CsvRecordReader.class);
  private final Text[] cache = new Text[1024];
  private final CSVParser parser;
  private final Iterator<CSVRecord> iterator;
  private final long length;
  private final boolean strict;
  private long position;
  private int headerLength;

  public CsvRecordReader(Reader reader, CSVFormat format, long length, boolean strict) throws IOException {
    this.length = length;
    this.strict = strict;
    parser = new CSVParser(reader, format);
    iterator = parser.iterator();
    headerLength = format.getHeader().length;
  }

  @Override
  public boolean next(LongWritable key, ListWritable<Text> value) throws IOException {
    value.clear();
    try {
      if (iterator.hasNext()) {
        CSVRecord record = iterator.next();
        if (!record.isConsistent()) {
          LOGGER.warn("inconsistent record at position: " + position);
          return next(key, value);
        }

        key.set(record.getRecordNumber());

        for (int i = 0; i < record.size(); i++) {
          String item = record.get(i);
          if (item == null) {
            value.add(null);
          } else {
            Text text = cache[i];
            if (text == null) {
              text = new Text();
              cache[i] = text;
            }
            text.set(item);
            value.add(text);
          }
        }
        position = record.getCharacterPosition();
        return true;
      }
    } catch (Exception e) {
      LOGGER.warn("failed to parse record at position: " + position);
      if (strict) {
        throw e;
      } else {
        return next(key, value);
      }
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
    return Math.min(1.0f, (float) position / length);
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
