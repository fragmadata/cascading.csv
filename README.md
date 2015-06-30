# Cascading.CSV
This project provides a simple CSV scheme for [Cascading](http://cascading.org) that supports both source and sink taps.

#### [JavaDocs](http://datascienceinc.github.io/cascading.csv/java/)

## Rationale

You may be thinking [Cascading already has a CSV scheme](https://github.com/tresata/cascading-opencsv/blob/master/src/main/java/com/tresata/cascading/opencsv/OpenCsvScheme.java)
or that [it even comes with one](http://docs.cascading.org/cascading/2.0/javadoc/cascading/scheme/hadoop/TextDelimited.html).
Cascading's core `TextDelimited` scheme works fine for many use cases, but when working with dirty data - CSV files
with rogue delimiters, line endings, quotes, and escape characters - we often require more powerful CSV parsers to
handle the wide variety of formats in which delimited data is written.

Tresata's [OpenCSV scheme](https://github.com/tresata/cascading-opencsv) was a valiant effort to handle the types of edge
cases often seen in CSV formatted data, but it still takes a naive approach to the problem. Specifically, the OpenCSV scheme
extends Cascading's `TextLine` scheme, thus inhereting issues such as those resulting from the occurrence of line endings in
lines that coincide with Hadoop input splits. In cases where a line ending appears in the same line as a Hadoop input split,
existing CSV schemes will only receive and split partial lines, losing one complete record and gaining two partial records
(the one prior to the split and the one after the split).

## Design

Cascading.CSV uses [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) for parsing CSV inputs and writing
CSV outputs. Commons CSV natively supports partitioning input files and so handles Hadoop's input splits rather gracefully.
In order to properly support Hadoop input splits for CSV files, this project provides a custom Hadoop
[CsvInputFormat](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvInputFormat.java)
and [CsvOutputFormat](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvOutputFormat.java)
along with an associated [RecordReader](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvRecordReader.java)
and [RecordWriter](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/hadoop/CsvRecordWriter.java) respectively.
Finally, the [CsvScheme](https://github.com/datascienceinc/cascading.csv/blob/master/src/main/java/com/datascience/cascading/CsvScheme.java)
provides the interface for operating on CSV sources and sinks within Cascading. Of course, `CsvInputFormat` and
`CsvOutputFormat` can certainly be used independently of Cascading as well.

### Installation

To use the official release, add the [Conjars](http://conjars.org/) repository to your `pom.xml`:

```
<repository>
  <id>conjars.org</id>
  <url>http://conjars.org/repo</url>
</repository>
```

Then add the following snippet to the `<dependencies>` section of your `pom.xml` file:

```
<dependency>
  <groupId>com.datascience</groupId>
  <artifactId>cascading-csv</artifactId>
  <version>0.1</version>
</dependency>
```

## Usage

To use the scheme, first construct a [CSVFormat](https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html).

```java
CSVFormat format = CSVFormat.newFormat(',')
  .withQuote('"')
  .withSkipHeaderRecord()
  .withEscape('\\')
  .withRecordSeparator('\n');
```

The `CSVFormat` dictates to the CSV parser how to read input records and write output records. Refer to the
[Commons CSV](https://commons.apache.org/proper/commons-csv/) documentation for the specific `CSVFormat` options provided.

Given a `CSVFormat`, simply construct a `CsvScheme` instance.

```java
Tap tap = new Hfs(new CsvScheme(format), "/path/to/file.csv");
```

For source schemes, if `skipHeaderRecord` is `true` and no fields are provided to the `CsvScheme` constructor the headers
and output fields will be automatically detected from the source data set. Header detection is performed by parsing only
the first row of the source and using the first row's values as headers.

## Using with Maven

Compiled binaries are available from [Conjars.org](www.conjars.org). 

Be sure to add the repository to your pom.xml:
```
<repository>
    <id>conjars.org</id>
    <url>http://conjars.org/repo</url>
</repository>
```
As well as the dependency:
```
<dependency>
    <groupId>com.datascience</groupId>
    <artifactId>cascading-csv</artifactId>
    <version>${cascading.csv.version}</version>
</dependency>
```

Snapshot and release artifacts can be found on Conjars.org. 

### License
Licensed under the [Apache License version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Copyright 2015 [DataScience, Inc](http://datascience.com)
