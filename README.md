# Cascading.CSV
This project provides a simple CSV scheme for [Cascading](http://cascading.org) that supports both source and sink taps.

## Justification

You may be thinking [Cascading already has a CSV scheme](https://github.com/tresata/cascading-opencsv/blob/master/src/main/java/com/tresata/cascading/opencsv/OpenCsvScheme.java)
or that [it even comes with one](http://docs.cascading.org/cascading/2.0/javadoc/cascading/scheme/hadoop/TextDelimited.html).
Cascading's core `TextDelimited` scheme works fine for many use cases, but when working with dirty data - CSV files
with rogue delimiters, line endings, quotes, and escape characters - we often require more powerful CSV parsers to
handle the wide variety of formats in which delimited data is written.

Tresata's [OpenCSV scheme](https://github.com/tresata/cascading-opencsv) was a valiant effort to remedy these precise
problems, but it still takes a naive approach to the problem. Specifically, the OpenCSV scheme extends Cascading's
`TextLine` scheme, thus inhereting issues such as when line endings occur in lines that coincide with Hadoop input splits.
In cases where a line ending appears in the same line as a Hadoop input split, existing CSV schemes will only receive
and split partial lines, losing one complete record and gaining two partial records (the one prior to the split and the
one after the split).

## Design

Cascading.CSV uses [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) for parsing CSV inputs and writing
CSV outputs. Commons CSV natively supports input partitioning files and therefore handles Hadoops input splits gracefully.
In order to take advantage of input splits, this project provides a custom Hadoop
[CsvInputFormat](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvInputFormat.java)
and [CsvOutputFormat](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvOutputFormat.java)
along with an associated [RecordReader](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvRecordReader.java)
and [RecordWriter](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvRecordWriter.java) respectively.
Finally, the [CsvScheme](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/cascading/CsvScheme.java)
provides the interface for operating on CSV sources and sinks within Cascading. Of course, `CsvInputFormat` and
`CsvOutputFormat` can certainly be used independently of Cascading as well.

## Usage

To use the scheme, first construct a [CSVFormat](https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html).

```java
CSVFormat format = CSVFormat.newFormat(',')
  .withQuote('"')
  .withSkipHeaderRecord()
  .withEscape('\\')
  .withRecordSeparator('\n');
```

The `CSVFormat` dictates to the [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) parser how to read
input records and write output records.

Given a `CSVFormat`, simply construct a `CsvScheme` instance.

```java
Tap tap = new Hfs(new CsvScheme(format), "/path/to/file.csv");
```

For source schemes, if `skipHeaderRecord` is `true` and no fields are provided to the `CsvScheme` constructor the headers
and output fields will be automatically detected from the source data set. Header detection is performed by parsing only
the first row of the source and using the first row's values as headers.

### License

Copyright 2015 [DataScience, inc](http://datascience.com)
