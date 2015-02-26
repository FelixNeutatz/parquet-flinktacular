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

import com.google.common.collect.Lists;
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
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroParquetInputFormat;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import flink.parquet.avro.*;
import flink.parquet.filter.PersonFilter;


public class ParquetAvroExample {

    public static void main(String[] args) throws Exception {

        //output
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        Person person = generateSampleObject();
        
        DataSet<Tuple2<Void,Person>> output = putObjectIntoDataSet(env, person);

        writeAvro(output, "newpath");    
        
        output.print();

        env.execute("Output");

        //input
        final ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
        
        DataSet<Tuple2<Void,Person>> input = readAvro(env2, "newpath");
        
        input.print();        

        env2.execute("Input"); 
    }

    
    public static Person generateSampleObject() {
        Person person = new Person();
        
        person.id = 42;
        person.name = "Felix";
        
        List<PhoneNumber> pList = new ArrayList<PhoneNumber>();
        pList.add(new PhoneNumber("123456", PhoneType.WORK));
        
        person.setPhone(pList);
        return person;
    }
    
    public static DataSet<Tuple2<Void,Person>> putObjectIntoDataSet(ExecutionEnvironment env, Person person) {
        List l = Arrays.asList(new Tuple2<Void, Person>(null, person));
        TypeInformation t = new TupleTypeInfo<Tuple2<Void,Person>>(TypeExtractor.getForClass(Void.class), TypeExtractor.getForClass(Person.class));

        DataSet<Tuple2<Void,Person>> data = env.fromCollection(l,t);
        
        return data;
    }

    public static void writeAvro(DataSet<Tuple2<Void,Person>> data, String outputPath) throws IOException {
        // Set up the Hadoop Input Format
        Job job = Job.getInstance();

        // Set up Hadoop Output Format
        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new AvroParquetOutputFormat(), job);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        AvroParquetOutputFormat.setSchema(job, Person.getClassSchema());
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setEnableDictionary(job, true);

        // Output & Execute
        data.output(hadoopOutputFormat);
    }
    
    public static DataSet<Tuple2<Void,Person>> readAvro(ExecutionEnvironment env, String inputPath) throws IOException {
        Job job = Job.getInstance();

        HadoopInputFormat hadoopInputFormat = new HadoopInputFormat(new AvroParquetInputFormat(), Void.class, Person.class, job);

        FileInputFormat.addInputPath(job, new Path(inputPath));

        // schema projection: don't read type of phonenumber     
        Schema phone = Schema.createRecord("PhoneNumber", null, null, false);
        phone.setFields(Arrays.asList(
                new Schema.Field("number",Schema.create(Schema.Type.BYTES), null, null)));
        
        Schema array = Schema.createArray(phone);
        Schema union = Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.NULL)));
        

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

        // push down predicates: get all persons with name = "Felix"
        ParquetInputFormat.setUnboundRecordFilter(job, PersonFilter.class);


        DataSet<Tuple2<Void, Person>> data = env.createInput(hadoopInputFormat);

       return data;
    }
    


}
