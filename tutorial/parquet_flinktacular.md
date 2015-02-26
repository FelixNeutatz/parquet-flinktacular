---
title: "Parquet at Flink"
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}

This document gives a deep-dive into the available transformations on DataSets. For a general introduction to the
Flink Java API, please refer to the [Programming Guide](programming_guide.html)

### Parquet

*"[Parquet](http://parquet.incubator.apache.org/) is a columnar storage format for Hadoop that supports complex nested data."*

Parquet is an open source project. Cloudera and Twitter are the major contributors. The idea for Parquet came from Google. They introduced their system called [Dremel](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf)
which brings the advantages of columnar storage and nested data together. Parquet is implementing this concept in Hadoop. Since Flink provides a seamless integration for Hadoop formats, we can leverage all the advantages of Parquet in Flink.

A [columnar storage format](http://en.wikipedia.org/wiki/Column-oriented_DBMS) brings a lot of advantages. The first one is [schema projection](#schema-projection). This makes it possible to read only those columns which are really needed in your application. Moreover column stores are highly compression friendly. This means compression algorithms work faster and can compress better, because information entropy per column is lower than per row. E.g if you imagine a column `phone number`, the values in this column are really similar. Maybe most of them have even the same area code. This high data value locality allows it to apply all kinds of compression. In Parquet supports GZIP, LZO and SNAPPY. Moreover it is possible to use serveral types of encoding: Bit Packing, Run Length encoding, Dictionary encoding, ...
Another nice feature of Parquet is the native implementation of [predicate pushdown](#native-predicate-pushdown). This makes it possible to filter records on the lowest level.

### Getting started

The idea of this tutorial is to get you started as quickly as possible. Therefore I setup a [Github repository](https://github.com/FelixNeutatz/parquet-flinktacular). There you can find sample [Maven](http://maven.apache.org/) projects which can serve you as templates for your own projects. 

At the moment I provide templates for the following use cases:

1. [Parquet at Flink - using Java and Protocol Buffers schema definition](https://github.com/FelixNeutatz/parquet-flinktacular/tree/master/java/protobuf)
2. [Parquet at Flink - using Java and Thrift schema definition](https://github.com/FelixNeutatz/parquet-flinktacular/tree/master/java/thrift)
3. [Parquet at Flink - using Java and Avro schema definition](https://github.com/FelixNeutatz/parquet-flinktacular/tree/master/java/avro)
4. [Parquet at Flink - using Scala and Protocol Buffers schema definition](https://github.com/FelixNeutatz/parquet-flinktacular/tree/master/scala/protobuf) 

Each project has two main folders: __commons__ and __flink__. 

In the __commons__ folder you put [your schema definition IDL file](#define-your-schema). The Maven `commons/pom.xml` is configured to build classes from the IDL file during compilation. This makes development more convenient, because you don't need to recompile the IDL file by hand whenever there is any minor change in your schema.

In the __flink__ folder there are your Flink jobs which read and write Parquet.

So choose your template project, download the corresponding folder and run: 

~~~bash
$ mvn clean compile package
~~~


### Define your schema

There are several ways to define the schema of your data. This tutorial covers three data serialization frameworks:  [Avro](http://avro.apache.org/), [Protocol Buffers (protobuf)](https://github.com/google/protobuf/) and [Thrift](https://thrift.apache.org/).

The example schema is the data of a person who has a name, id, email address and several phone numbers:

<div class="codetabs" markdown="1">
<div data-lang="protobuf" markdown="1">

~~~
option java_package = "flink.parquet.proto";
option java_outer_classname = "PersonProto";

message Person {
    required string name = 1;
    required int32 id = 2;
    optional string email = 3;

    enum PhoneType {
        MOBILE = 0;
        HOME = 1;
        WORK = 2;
    }

    message PhoneNumber {
        required string number = 1;
        optional PhoneType type = 2 [default = HOME];
    }

    repeated PhoneNumber phone = 4;
}
~~~

</div>
<div data-lang="thrift" markdown="1">

~~~
namespace java flink.parquet.thrift

enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
}

struct PhoneNumber {
    1: required string number,
    2: optional PhoneType type = PhoneType.HOME;
}

struct Person {
    1: required string name,
    2: required i32 id,
    3: optional string email,
    4: list<PhoneNumber> phone
}
~~~

</div>
<div data-lang="avro" markdown="1">

~~~
@namespace("flink.parquet.avro")
protocol FlinkParquetAvro {

    enum PhoneType {
        MOBILE,
        HOME, 
        WORK
    }
  
    record PhoneNumber {
        string number;
        union { PhoneType, null } type = "HOME";
    }

    record Person {
        string name;
        int id;
        union { string, null } email;
        array<PhoneNumber> phone;
    }
}
~~~

</div>
</div>


### Flink dependencies

The Flink dependencies will also imported by the [templates](#getting-started). The one important thing to note here is that Flink supports Parquet since version `0.8.1`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

~~~
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-clients</artifactId>
	<version>0.8.1</version>
</dependency>
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-java</artifactId>
	<version>0.8.1</version>
</dependency>
~~~

</div>
<div data-lang="scala" markdown="1">

~~~
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-clients</artifactId>
	<version>0.8.1</version>
</dependency>
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-scala</artifactId>
	<version>0.8.1</version>
</dependency>
~~~

</div>
</div>


### Parquet dependencies

The Parquet dependencies are also provided by the [templates](#getting-started). But if you want to build your own Maven configuration you will find this interesting:

<div class="codetabs" markdown="1">
<div data-lang="protobuf" markdown="1">

~~~
<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>parquet-hadoop</artifactId>
	<version>1.6.0rc4</version>
</dependency>
<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>parquet-protobuf</artifactId>
	<version>1.6.0rc4</version>
</dependency>
~~~

</div>
<div data-lang="thrift" markdown="1">

~~~
<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>parquet-hadoop</artifactId>
	<version>1.6.0rc4</version>
</dependency>
<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>parquet-thrift</artifactId>
	<version>1.6.0rc4</version>
</dependency>
~~~

</div>
<div data-lang="avro" markdown="1">

~~~
<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>parquet-hadoop</artifactId>
	<version>1.6.0rc4</version>
</dependency>
<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>parquet-avro</artifactId>
	<version>1.6.0rc4</version>
</dependency>
~~~

</div>
</div>


### Write Parquet

Some text

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

~~~java
Job job = Job.getInstance();

// Set up Hadoop Output Format
HadoopOutputFormat parquetFormat = new HadoopOutputFormat(new ProtoParquetOutputFormat(), job);

FileOutputFormat.setOutputPath(job, new Path(outputPath));

ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
ParquetOutputFormat.setEnableDictionary(job, true);

ProtoParquetOutputFormat.setProtobufClass(job, Person.class);

// Output & Execute
data.output(parquetFormat);
~~~

</div>
<div data-lang="scala" markdown="1">

~~~scala
val job = Job.getInstance

// Set up Hadoop Output Format
val parquetFormat = new HadoopOutputFormat[Void,Person](new ProtoParquetOutputFormat, job)

FileOutputFormat.setOutputPath(job, new Path(outputPath))

ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
ParquetOutputFormat.setEnableDictionary(job, true)

ProtoParquetOutputFormat.setProtobufClass(job, classOf[Person])

// Output & Execute
data.output(parquetFormat)
~~~

</div>
</div>

### Read Parquet

Some text

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

~~~java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
Job job = Job.getInstance();

HadoopInputFormat hadoopInputFormat = new HadoopInputFormat(
        new ProtoParquetInputFormat(), Void.class, Person.Builder.class, job);

FileInputFormat.addInputPath(job, new Path(inputPath));

//native predicate push down: read only records which satisfy a given constraint
ParquetInputFormat.setUnboundRecordFilter(job, PersonFilter.class);

DataSet<Tuple2<Void,Person.Builder>> data = env.createInput(hadoopInputFormat);
~~~

</div>
<div data-lang="scala" markdown="1">

~~~scala
val env = ExecutionEnvironment.getExecutionEnvironment
val job = Job.getInstance

val parquetFormat = new HadoopInputFormat[Void,Person.Builder](
        new ProtoParquetInputFormat, classOf[Void], classOf[Person.Builder], job)

FileInputFormat.addInputPath(job, new Path(inputPath))

//native predicate push down: read only records which satisfy a given constraint
ParquetInputFormat.setUnboundRecordFilter(job, classOf[PersonFilter])

val data = env.createInput(parquetFormat)
~~~

</div>
</div>

### Native predicate pushdown

Something

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

~~~java
import parquet.column.ColumnReader;
import parquet.filter.ColumnPredicates;
import parquet.filter.ColumnRecordFilter;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;

public class PersonFilter implements UnboundRecordFilter {
    public RecordFilter bind(Iterable<ColumnReader> readers){
        return ColumnRecordFilter.column(
                "name",
                ColumnPredicates.equalTo("Felix")
        ).bind(readers);
    }
}
~~~

</div>
<div data-lang="scala" markdown="1">

~~~scala
import parquet.column.ColumnReader
import parquet.filter.ColumnPredicates
import parquet.filter.ColumnRecordFilter
import parquet.filter.RecordFilter
import parquet.filter.UnboundRecordFilter

class PersonFilter extends UnboundRecordFilter {
    def bind(readers: java.lang.Iterable[ColumnReader]): RecordFilter = {
        return ColumnRecordFilter.column(
		"name", 
		ColumnPredicates.equalTo("Felix")
	).bind(readers)
    }
}
~~~

</div>
</div>

### Schema projection

Something

<div class="codetabs" markdown="1">
<div data-lang="protobuf" markdown="1">

~~~java
//schema projection: don't read phone type attribute
String projection = "message Person {\n" +
        "  required binary name (UTF8);\n" +
        "  required int32 id;\n" +
        "  optional binary email (UTF8);\n" +
        "  repeated group phone {\n" +
        "    required binary number (UTF8);\n" +
        "  }\n" +
        "}";
ProtoParquetInputFormat.setRequestedProjection(job, projection);
~~~

</div>
<div data-lang="thrift" markdown="1">

~~~java
// schema projection: don't read phone type attribute
job.getConfiguration().set("parquet.thrift.column.filter", "name;id;email;phone/number");
~~~

</div>
<div data-lang="avro" markdown="1">

~~~java
// schema projection: don't read phone type attribute
Schema phone = Schema.createRecord("PhoneNumber", null, null, false);
phone.setFields(Arrays.asList(
        new Schema.Field("number",Schema.create(Schema.Type.BYTES), null, null)));

Schema array = Schema.createArray(phone);
Schema union = Schema.createUnion(
	Lists.newArrayList(Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.NULL)));


Schema projection = Schema.createRecord("Person", null, null, false);
projection.setFields(
            Arrays.asList(
                new Schema.Field("name",Schema.create(Schema.Type.BYTES), null, null),
                new Schema.Field("id",Schema.create(Schema.Type.INT), null, null),
                new Schema.Field("email",union, null, null),
                new Schema.Field("phone",array, null, null)    
            )
        );
       
AvroParquetInputFormat.setRequestedProjection(job, projection);
~~~

</div>
</div>
