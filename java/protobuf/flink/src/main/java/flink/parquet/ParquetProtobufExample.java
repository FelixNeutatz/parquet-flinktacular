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

package flink.parquet;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.proto.ProtoParquetInputFormat;
import parquet.proto.ProtoParquetOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import flink.parquet.filter.PersonFilter;
import flink.parquet.proto.AddressBookProtos;

/**
 * Implements a word count which takes the input file and counts the number of
 * occurrences of each word in the file and writes the result back to disk.
 *
 * This example shows how to use Hadoop Input Formats, how to convert Hadoop Writables to 
 * common Java types for better usage in a Flink job and how to use Hadoop Output Formats.
 */
@SuppressWarnings("serial")
public class ParquetProtobufExample {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //output
        DataSet<Tuple2<Void,AddressBookProtos.Person>> data = generateDataSet(env);

        writeProtobuf(env, data, "newpath");
        
        data.print();
        
        env.execute("Parquet Output");

        //input
        final ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();      
        
        DataSet<Tuple2<Void,AddressBookProtos.Person.Builder>> input = readProtobuf(env2, "newpath");
        
        input.map(new TupleToProto()).print();       

        env2.execute("Parquet input");
    }


    public static Tuple2<Void, AddressBookProtos.Person> generateSampleObject(int id, String name, String phone) {
        AddressBookProtos.Person.Builder person = AddressBookProtos.Person.newBuilder();
        person.setId(id);
        person.setName(name);
        
        AddressBookProtos.Person.PhoneNumber.Builder phoneNumber =  AddressBookProtos.Person.PhoneNumber.newBuilder().setNumber(phone);
        phoneNumber.setType(AddressBookProtos.Person.PhoneType.WORK);
        person.addPhone(phoneNumber);
        
        return new Tuple2<Void, AddressBookProtos.Person>(null, person.build());
    }

    public static  List<Tuple2<Void, AddressBookProtos.Person>> generateSampleList() {
        List samples = new ArrayList<Tuple2<Void, AddressBookProtos.Person>>();
        samples.add(generateSampleObject(42,"Felix","0123"));
        samples.add(generateSampleObject(43,"Robert","4567"));

        return samples;
    }
        
    public static DataSet<Tuple2<Void,AddressBookProtos.Person>> generateDataSet(ExecutionEnvironment env) {
        List l = generateSampleList();
        TypeInformation t = new TupleTypeInfo<Tuple2<Void,AddressBookProtos.Person>>(TypeExtractor.getForClass(Void.class), TypeExtractor.getForClass(AddressBookProtos.Person.class));

        DataSet<Tuple2<Void,AddressBookProtos.Person>> data = env.fromCollection(l,t);
        
        return data;
    }

    public static void writeProtobuf(ExecutionEnvironment env, DataSet<Tuple2<Void,AddressBookProtos.Person>> data, String outputPath) throws IOException {
        // Set up the Hadoop Input Format
        Job job = Job.getInstance();

        // Set up Hadoop Output Format
        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ProtoParquetOutputFormat(), job);

        ProtoParquetOutputFormat.setOutputPath(job, new Path(outputPath));

        ProtoParquetOutputFormat.setProtobufClass(job, AddressBookProtos.Person.class);
        ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ProtoParquetOutputFormat.setEnableDictionary(job, true);


        // Output & Execute
        data.output(hadoopOutputFormat);
    }

    public static DataSet<Tuple2<Void,AddressBookProtos.Person.Builder>> readProtobuf(ExecutionEnvironment env, String inputPath) throws IOException {
        Job job = Job.getInstance();

        HadoopInputFormat hadoopInputFormat = new HadoopInputFormat(new ProtoParquetInputFormat(), Void.class, AddressBookProtos.Person.Builder.class, job);

        ProtoParquetInputFormat.addInputPath(job, new Path(inputPath));

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

        //native predicate push down: read only records which have name = "Felix"
        ProtoParquetInputFormat.setUnboundRecordFilter(job, PersonFilter.class);

        DataSet<Tuple2<Void, AddressBookProtos.Person.Builder>> data = env.createInput(hadoopInputFormat);

        return data;
    }

    public static final class TupleToProto implements MapFunction<Tuple2<Void, AddressBookProtos.Person.Builder>, AddressBookProtos.Person> {

        @Override
        public AddressBookProtos.Person map(Tuple2<Void, AddressBookProtos.Person.Builder> value) {
            return value.f1.build();

        }
    }


}
