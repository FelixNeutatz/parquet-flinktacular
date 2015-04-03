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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import parquet.filter2.predicate.FilterPredicate;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.thrift.ParquetThriftOutputFormat;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.io.api.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import flink.parquet.thrift.*;

import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.Operators.BinaryColumn;


public class ParquetThriftExample {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        //output
        Person person = generateSampleObject();        
        DataSet<Tuple2<Void,Person>> output = putObjectIntoDataSet(env, person);
        writeThrift(output, "newpath");            
        output.print();
        
        //input
        DataSet<Tuple2<Void,Person>> input = readThrift(env, "newpath");        
        input.print();        

        env.execute("Parquet Thrift Example"); 
    }

    
    public static Person generateSampleObject() {
        Person person = new Person();        
        person.id = 42;
        person.name = "Felix";
        
        List<PhoneNumber> phoneNumberList = new ArrayList<PhoneNumber>();
        PhoneNumber phoneNumber = new PhoneNumber();
        phoneNumber.setType(PhoneType.WORK);
        phoneNumber.setNumber("0123456");
        phoneNumberList.add(phoneNumber);        
        person.setPhone(phoneNumberList);

        return person;
    }
    
    public static DataSet<Tuple2<Void,Person>> putObjectIntoDataSet(ExecutionEnvironment env, Person person) {
        List l = Arrays.asList(new Tuple2<Void, Person>(null, person));
        TypeInformation t = new TupleTypeInfo<Tuple2<Void,Person>>(TypeExtractor.getForClass(Void.class), TypeExtractor.getForClass(Person.class));

        DataSet<Tuple2<Void,Person>> data = env.fromCollection(l,t);
        
        return data;
    }

    public static void writeThrift(DataSet<Tuple2<Void,Person>> data, String outputPath) throws IOException {
        // Set up the Hadoop Input Format
        Job job = Job.getInstance();

        // Set up Hadoop Output Format
        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ParquetThriftOutputFormat(), job);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setEnableDictionary(job, true);

        ParquetThriftOutputFormat.setThriftClass(job, Person.class);

        // Output & Execute
        data.output(hadoopOutputFormat);
    }
    
    public static DataSet<Tuple2<Void,Person>> readThrift(ExecutionEnvironment env, String inputPath) throws IOException {
        Job job = Job.getInstance();

        HadoopInputFormat hadoopInputFormat = new HadoopInputFormat(new ParquetThriftInputFormat(), Void.class, Person.class, job);

        // schema projection: don't read attributes id and email
        job.getConfiguration().set("parquet.thrift.column.filter", "name;id;email;phone/number");

        FileInputFormat.addInputPath(job, new Path(inputPath));

        // push down predicates: get all persons with name = "Felix"
		BinaryColumn name = binaryColumn("name");
		FilterPredicate namePred = eq(name, Binary.fromString("Felix"));
		ParquetInputFormat.setFilterPredicate(job.getConfiguration(), namePred);

        DataSet<Tuple2<Void, Person>> data = env.createInput(hadoopInputFormat);

        return data;
    }
}
