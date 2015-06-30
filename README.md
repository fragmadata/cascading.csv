# Cascading.CSV
This project provides a simple CSV scheme for [Cascading](http://cascading.org) based on [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/)

#### [JavaDocs](http://datascienceinc.github.io/cascading.csv/java/1.0)

## Rationale

Cascading's core [TextDelimited](http://docs.cascading.org/cascading/2.0/javadoc/cascading/scheme/hadoop/TextDelimited.html)
scheme works well for many use cases, but when working with dirty data - CSV files with quoted and escaped delimiters and
line endings - we at [DataScience](http://datascience.com) require a more powerful scheme to handle the wide variety of
formats in which delimited data is written.

## Design

Cascading.CSV uses [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) for parsing CSV inputs and writing
CSV outputs. In order to properly handle encoding, compression, and Hadoop input splits, this project implements a custom
[CsvInputFormat](http://datascienceinc.github.io/cascading.csv/java/1.0/com/datascience/hadoop/CsvInputFormat.html)
and [CsvOutputFormat](http://datascienceinc.github.io/cascading.csv/java/1.0/com/datascience/hadoop/CsvOutputFormat.html)
along with an associated [RecordReader](http://datascienceinc.github.io/cascading.csv/java/1.0/com/datascience/hadoop/CsvRecordReader.html)
and [RecordWriter](http://datascienceinc.github.io/cascading.csv/java/1.0/com/datascience/hadoop/CsvRecordWriter.html) respectively.
Finally, the [CsvScheme](http://datascienceinc.github.io/cascading.csv/java/1.0/com/datascience/cascading/scheme/CsvScheme.html)
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
[Commons CSV documentation](https://commons.apache.org/proper/commons-csv/archives/1.1/apidocs/index.html) for the specific
`CSVFormat` options provided.

Given a `CSVFormat`, simply construct a `CsvScheme` instance.

```java
Tap tap = new Hfs(new CsvScheme(format), "/path/to/file.csv");
```

For source schemes, if `skipHeaderRecord` is `true` and no fields are provided to the `CsvScheme` constructor the headers
and output fields will be automatically detected from the source data set. Header detection is performed by parsing only
the first row of the source and using the first row's values as headers.

See the [API documentation](http://datascienceinc.github.io/cascading.csv/java/1.0) for more information on specific
`CsvScheme` constructors.

### License
Licensed under the [Apache License version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Copyright 2015 [DataScience, Inc](http://datascience.com)
