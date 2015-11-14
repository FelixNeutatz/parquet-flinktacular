# parquet-flinktacular - How to use Parquet in Flink - Guide

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
$ mvn clean install package
~~~

The more detailed tutorial can be found [here](https://github.com/FelixNeutatz/parquet-flinktacular/blob/master/tutorial/parquet_flinktacular.pdf) :)
