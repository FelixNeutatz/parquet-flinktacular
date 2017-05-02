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
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import flink.parquet.avro.*;
import org.apache.parquet.io.api.Binary;

import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.Operators.BinaryColumn;


public class ParquetAvroExample {

	public static void main(String[] args) throws Exception {

		//output
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Person person = generateSampleObject();
		DataSet<Tuple2<Void, Person>> output = putObjectIntoDataSet(env, person);
		writeAvro(output, "/tmp/newpath");
		output.print();

		//input
		DataSet<Tuple2<Void, Person>> input = readAvro(env, "/tmp/newpath");
		input.print();
	}


	public static Person generateSampleObject() {
		Person person = new Person();
		person.setId(42);
		person.setName("Felix");
		person.setEmail("test@test.com");

		List<PhoneNumber> pList = new ArrayList<PhoneNumber>();
		pList.add(new PhoneNumber("123456", PhoneType.WORK));
		person.setPhone(pList);
		return person;
	}

	public static DataSet<Tuple2<Void, Person>> putObjectIntoDataSet(ExecutionEnvironment env, Person person) {
		List<Tuple2<Void, Person>> l = Arrays.asList(new Tuple2<Void, Person>(null, person));
		TypeInformation<Tuple2<Void, Person>> t = new TupleTypeInfo<Tuple2<Void, Person>>(
				TypeExtractor.getForClass(Void.class), TypeExtractor.getForClass(Person.class));

		DataSet<Tuple2<Void, Person>> data = env.fromCollection(l, t);

		return data;
	}

	public static void writeAvro(DataSet<Tuple2<Void, Person>> data, String outputPath) throws IOException {
		// Set up the Hadoop Input Format
		Job job = Job.getInstance();

		// Set up Hadoop Output Format
		HadoopOutputFormat<Void, Person> hadoopOutputFormat = new HadoopOutputFormat<>(
				new AvroParquetOutputFormat<Person>(), job);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		AvroParquetOutputFormat.setSchema(job, Person.getClassSchema());
		ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		ParquetOutputFormat.setEnableDictionary(job, true);

		// Output & Execute
		data.output(hadoopOutputFormat);
	}

	public static DataSet<Tuple2<Void, Person>> readAvro(ExecutionEnvironment env, String inputPath) throws
		IOException {
		Job job = Job.getInstance();

		HadoopInputFormat<Void, Person> hadoopInputFormat = new HadoopInputFormat<>(
				new AvroParquetInputFormat<Person>(), Void.class, Person.class, job);

		FileInputFormat.addInputPath(job, new Path(inputPath));

		List<Field> personProjFields =  new ArrayList<>();
		personProjFields.add(getPersonField(Person.SCHEMA$.getField("id")));
		personProjFields.add(getPersonField(Person.SCHEMA$.getField("name")));
		personProjFields.add(getPersonField(Person.SCHEMA$.getField("email")));
		Schema projection = Schema.createRecord(Person.SCHEMA$.getName(), 
				Person.SCHEMA$.getDoc(),
				Person.SCHEMA$.getNamespace(), 
				false, 
				personProjFields);
		
		AvroParquetInputFormat.setRequestedProjection(job, projection);

		// push down predicates: get all persons with name = "Felix"
		BinaryColumn name = binaryColumn("name");
		FilterPredicate namePred = eq(name, Binary.fromString("Felix"));
		ParquetInputFormat.setFilterPredicate(job.getConfiguration(), namePred);

		DataSet<Tuple2<Void, Person>> data = env.createInput(hadoopInputFormat);

		return data;
	}


	private static Schema.Field getPersonField(Field field) {
		Schema.Field ret = new Schema.Field(
				field.name(), field.schema(), field.doc(),
				field.defaultVal(), field.order());
		return ret;
	}
}
