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

import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.eq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import flink.parquet.thrift.Person;
import flink.parquet.thrift.PhoneNumber;

import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators.BinaryColumn;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.thrift.ParquetThriftOutputFormat;
import parquet.io.api.Binary;

public class ParquetThriftExample {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if (env instanceof LocalEnvironment) {
			Configuration conf = new Configuration();
			// conf.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			// FLINK_TEST_TMP_DIR);
			// conf.setString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY,
			// FLINK_TEST_TMP_DIR);
			conf.setLong(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 4096 * 4);
			env = ExecutionEnvironment.createLocalEnvironment(conf);
			env.setParallelism(Runtime.getRuntime().availableProcessors());
		}

		// output
		int size = 1200;
		Person[] people = new Person[size];
		for (int j = 0; j < size; j++) {
			people[j] = generateSampleObject(j);
		}
		DataSet<Person> output = env.fromElements(people);

		writeThrift(output, "/tmp/thrift-test");
		output.print();

		// input
		DataSet<Tuple2<Void, Person>> input = readThrift(env, "/tmp/thrift-test");
		input.print();

		// env.execute("Parquet Thrift Example");
	}

	public static Person generateSampleObject(int i) {
		Person person = new Person();
		person.id = i;
		person.name = "Felix" + i;

		List<PhoneNumber> phoneNumberList = new ArrayList<PhoneNumber>();
		PhoneNumber phoneNumber = new PhoneNumber();
		phoneNumber.setNumber("0123456" + i);
		phoneNumberList.add(phoneNumber);
		person.setPhone(phoneNumberList);

		return person;
	}

	public static void writeThrift(DataSet<Person> data, String outputPath) throws IOException {
		// Set up the Hadoop Input Format
		Job job = Job.getInstance();

		// Set up Hadoop Output Format
		OutputFormat<Tuple2<Void, Person>> hadoopOutputFormat = new HadoopOutputFormat<>(
				new ParquetThriftOutputFormat<>(), job);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		ParquetOutputFormat.setEnableDictionary(job, true);

		ParquetThriftOutputFormat.setThriftClass(job, Person.class);

		final TypeInformation<Tuple2<Void, Person>> returnType = new TupleTypeInfo<>(
		        TypeExtractor.getForClass(Void.class), TypeExtractor.getForClass(Person.class));

		// Output & Execute
		data.map(x-> new Tuple2<Void, Person>(null, x)).returns(returnType).output(hadoopOutputFormat);
	}

	public static DataSet<Tuple2<Void, Person>> readThrift(ExecutionEnvironment env, String inputPath)
			throws IOException {
		Job job = Job.getInstance();

		HadoopInputFormat<Void, Person> hadoopInputFormat = new HadoopInputFormat<>(new ParquetThriftInputFormat<>(),
				Void.class, Person.class, job);

		// schema projection: don't read attributes id and email
		job.getConfiguration().set("parquet.thrift.column.filter", "name;id;email;phone/number");

		FileInputFormat.addInputPath(job, new Path(inputPath));

		// push down predicates: get all persons with name = "Felix"
		BinaryColumn name = binaryColumn("name");
		FilterPredicate namePred = eq(name, Binary.fromString("Felix100"));
		ParquetInputFormat.setFilterPredicate(job.getConfiguration(), namePred);

		DataSet<Tuple2<Void, Person>> data = env.createInput(hadoopInputFormat);

		return data;
	}
}
