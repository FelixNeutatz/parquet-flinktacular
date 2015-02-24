/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.parquet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.proto.ProtoParquetInputFormat
import parquet.proto.ProtoParquetOutputFormat

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce._

import flink.parquet.proto.AddressBookProtos
import flink.parquet.filter.PersonFilter


object ParquetProtobufExample {

  def main(args: Array[String]) {

        val env = ExecutionEnvironment.getExecutionEnvironment

        //output
        val data = generateDataSet(env)

        writeProtobuf(env, data, "newpath")
        
        data.print()
        
        env.execute("Parquet Output")

        //input
        val env2 = ExecutionEnvironment.getExecutionEnvironment
        
        val input = readProtobuf(env2, "newpath")
    
        input.map { pair => pair._2.asInstanceOf[AddressBookProtos.Person.Builder].build }.print

        env2.execute("Parquet input")
    }


    def generateSampleObject(id:Integer, name:String, phone:String): Tuple2[Void, AddressBookProtos.Person] = {
        val person = AddressBookProtos.Person.newBuilder
        person.setId(id)
        person.setName(name)
        
        val phoneNumber =  AddressBookProtos.Person.PhoneNumber.newBuilder.setNumber(phone)
        phoneNumber.setType(AddressBookProtos.Person.PhoneType.WORK)
        person.addPhone(phoneNumber)
        
        return new Tuple2[Void, AddressBookProtos.Person](null, person.build)
    }

    def generateDataSet(env:ExecutionEnvironment): DataSet[Tuple2[Void,AddressBookProtos.Person]] = {
        val samples = List(generateSampleObject(42,"Felix","0123"), generateSampleObject(43,"Robert","4567"))      
      
        val data = env.fromCollection(samples)
        
        return data
    }

    def writeProtobuf(env:ExecutionEnvironment, data:DataSet[Tuple2[Void,AddressBookProtos.Person]], outputPath:String) {
        // Set up the Hadoop Input Format
        val job = Job.getInstance

        // Set up Hadoop Output Format
        val hadoopOutputFormat = new HadoopOutputFormat[Void,AddressBookProtos.Person](new ProtoParquetOutputFormat, job)

        FileOutputFormat.setOutputPath(job, new Path(outputPath))

        ProtoParquetOutputFormat.setProtobufClass(job, classOf[AddressBookProtos.Person])
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
        ParquetOutputFormat.setEnableDictionary(job, true)

        // Output & Execute
        data.output(hadoopOutputFormat.asInstanceOf[OutputFormat[Tuple2[Void,AddressBookProtos.Person]]])
    }

    def readProtobuf(env:ExecutionEnvironment, inputPath:String): DataSet[Tuple2[Void,AddressBookProtos.Person]] = {
        val job = Job.getInstance

        val hadoopInputFormat = new HadoopInputFormat[Void,AddressBookProtos.Person.Builder](new ProtoParquetInputFormat, classOf[Void], classOf[AddressBookProtos.Person.Builder], job)

        FileInputFormat.addInputPath(job, new Path(inputPath))

        //schema projection: don't read type of phone type attribute
        val projection = "message Person {\n" +
                "  required binary name (UTF8);\n" +
                "  required int32 id;\n" +
                "  optional binary email (UTF8);\n" +
                "  repeated group phone {\n" +
                "    required binary number (UTF8);\n" +
                "  }\n" +
                "}"
        ProtoParquetInputFormat.setRequestedProjection(job, projection)

        //native predicate push down: read only records which have name = "Felix"
        ParquetInputFormat.setUnboundRecordFilter(job, classOf[PersonFilter])

        val data = env.createInput(hadoopInputFormat)

        return data.asInstanceOf[DataSet[Tuple2[Void,AddressBookProtos.Person]]]
    }


}
