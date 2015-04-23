package com.datascience.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
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
  private final CSVFormat format;
  private final CSVParser parser;
  private final Iterator<CSVRecord> iterator;
  private final float start;
  private final float length;
  private long position;
  private long end;

  public CsvRecordReader(FileSplit split, Configuration conf) throws IOException {
    this.format = CsvConf.createFormat(conf);
    this.start = split.getStart();
    this.length = split.getLength();
    this.end = split.getStart() + split.getLength();
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream is = fs.open(path);
    Reader isr = new InputStreamReader(is);
    parser = new CSVParser(isr, format, split.getStart(), split.getStart());
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
    return Math.min(1.0f, (float) position - start / length);
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
