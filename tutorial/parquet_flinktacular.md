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


### Flink dependencies

Some text

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

~~~java
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

~~~scala
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

Something

<div class="codetabs" markdown="1">
<div data-lang="protobuf" markdown="1">

~~~java
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

~~~java
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

~~~java
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

ProtoParquetOutputFormat.setProtobufClass(job, AddressBookProtos.Person.class);

// Output & Execute
data.output(parquetFormat);
~~~

</div>
<div data-lang="scala" markdown="1">

~~~scala
val job = Job.getInstance

// Set up Hadoop Output Format
val parquetFormat = 
        new HadoopOutputFormat[Void,AddressBookProtos.Person](new ProtoParquetOutputFormat, job)

FileOutputFormat.setOutputPath(job, new Path(outputPath))

ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
ParquetOutputFormat.setEnableDictionary(job, true)

ProtoParquetOutputFormat.setProtobufClass(job, classOf[AddressBookProtos.Person])

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
        new ProtoParquetInputFormat(), Void.class, AddressBookProtos.Person.Builder.class, job);

FileInputFormat.addInputPath(job, new Path(inputPath));

//native predicate push down: read only records which satisfy a given constraint
ParquetInputFormat.setUnboundRecordFilter(job, PersonFilter.class);

DataSet<Tuple2<Void,AddressBookProtos.Person.Builder>> data = env.createInput(hadoopInputFormat);
~~~

</div>
<div data-lang="scala" markdown="1">

~~~scala
val env = ExecutionEnvironment.getExecutionEnvironment
val job = Job.getInstance

val parquetFormat = new HadoopInputFormat[Void,AddressBookProtos.Person.Builder](
    new ProtoParquetInputFormat, classOf[Void], classOf[AddressBookProtos.Person.Builder], job)

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
//schema projection: don't read type of phone type attribute
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
// schema projection: don't read type of phone type attribute
job.getConfiguration().set("parquet.thrift.column.filter", "name;id;email;phone/number");
~~~

</div>
<div data-lang="avro" markdown="1">

~~~java
// schema projection: don't read attributes id and email
Schema projection = Schema.createRecord("Person", null, null, false);
projection.setFields(
            Arrays.asList(
                new Schema.Field("name",Schema.create(Schema.Type.BYTES), null, null),
                new Schema.Field("id",Schema.create(Schema.Type.INT), null, null),
                new Schema.Field("phone",Schema.create(Schema.Type.BYTES), null, null)    
            )
        );
       
AvroParquetInputFormat.setRequestedProjection(job, projection);
~~~

</div>
</div>
