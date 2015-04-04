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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import parquet.filter2.predicate.FilterPredicate;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.io.api.Binary;
import parquet.proto.ProtoParquetInputFormat;
import parquet.proto.ProtoParquetOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import flink.parquet.proto.PersonProto.Person;

import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.Operators.BinaryColumn;


@SuppressWarnings("serial")
public class ParquetProtobufExample {

	public static void main(String[] args) throws Exception {

		//output
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Void, Person>> data = generateDataSet(env);
		writeProtobuf(data, "newpath");
		data.print();
		env.execute("Parquet output");

		//input
		final ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Void, Person.Builder>> input = readProtobuf(env2, "newpath");
		input.map(new TupleToProto()).print();
		env2.execute("Parquet input");
	}


	public static Tuple2<Void, Person> generateSampleObject(int id, String name, String phone) {
		Person.Builder person = Person.newBuilder();
		person.setId(id);
		person.setName(name);

		Person.PhoneNumber.Builder phoneNumber = Person.PhoneNumber.newBuilder().setNumber(phone);
		phoneNumber.setType(Person.PhoneType.WORK);
		person.addPhone(phoneNumber);

		return new Tuple2<Void, Person>(null, person.build());
	}

	public static List<Tuple2<Void, Person>> generateSampleList() {
		List samples = new ArrayList<Tuple2<Void, Person>>();
		samples.add(generateSampleObject(42, "Felix", "0123"));
		samples.add(generateSampleObject(43, "Robert", "4567"));

		return samples;
	}

	public static DataSet<Tuple2<Void, Person>> generateDataSet(ExecutionEnvironment env) {
		List l = generateSampleList();
		TypeInformation t = new TupleTypeInfo<Tuple2<Void, Person>>(TypeExtractor.getForClass(Void.class), 
			TypeExtractor.getForClass(Person.class));

		DataSet<Tuple2<Void, Person>> data = env.fromCollection(l, t);

		return data;
	}

	public static void writeProtobuf(DataSet<Tuple2<Void, Person>> data, String outputPath) throws IOException {
		Job job = Job.getInstance();

		// Set up Hadoop Output Format
		HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ProtoParquetOutputFormat(), job);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		ProtoParquetOutputFormat.setProtobufClass(job, Person.class);
		ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		ParquetOutputFormat.setEnableDictionary(job, true);

		// Output & Execute
		data.output(hadoopOutputFormat);
	}

	public static DataSet<Tuple2<Void, Person.Builder>> readProtobuf(ExecutionEnvironment env, String inputPath) 
		throws IOException {
		Job job = Job.getInstance();

		HadoopInputFormat hadoopInputFormat = new HadoopInputFormat(new ProtoParquetInputFormat(), Void.class, Person
			.Builder.class, job);

		FileInputFormat.addInputPath(job, new Path(inputPath));

		//native predicate push down: read only records which satisfy a given constraint
		BinaryColumn name = binaryColumn("name");
		FilterPredicate namePred = eq(name, Binary.fromString("Felix"));
		ParquetInputFormat.setFilterPredicate(job.getConfiguration(), namePred);

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

		DataSet<Tuple2<Void, Person.Builder>> data = env.createInput(hadoopInputFormat);

		return data;
	}

	public static final class TupleToProto implements MapFunction<Tuple2<Void, Person.Builder>, Person> {
		@Override
		public Person map(Tuple2<Void, Person.Builder> value) {
			return value.f1.build();
		}
	}
}
