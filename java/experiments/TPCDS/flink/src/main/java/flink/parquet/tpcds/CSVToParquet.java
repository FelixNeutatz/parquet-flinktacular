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

package flink.parquet.tpcds;

import flink.utilities.Util;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import flink.parquet.thrift.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.thrift.ParquetThriftOutputFormat;

import java.io.IOException;


public class CSVToParquet {
	public static void main(String[] args) throws Exception {
		if (!parseParameters(args)) {
			return;
		}

		int splitNumber = 4;

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDefaultLocalParallelism(splitNumber);
		createStoreSales(env);
		env.execute("convert store sales");

		final ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
		env2.setDefaultLocalParallelism(splitNumber);
		createItem(env2);
		env2.execute("convert items");

		final ExecutionEnvironment env3 = ExecutionEnvironment.getExecutionEnvironment();
		env3.setDefaultLocalParallelism(splitNumber);
		createDateDim(env3);
		env3.execute("convert datedims");
	}

	private static void createDateDim(ExecutionEnvironment env) throws IOException {
		DataSet<Tuple2<Void, DateDimTable>> datedims = getDateDimDataSet(env).map(new DateDimToParquet());

		Job job = Job.getInstance();

		HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ParquetThriftOutputFormat(), job);

		ParquetThriftOutputFormat.setOutputPath(job, new Path(outputPath + "/datedim"));
		ParquetThriftOutputFormat.setThriftClass(job, DateDimTable.class);
		ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		ParquetThriftOutputFormat.setCompressOutput(job, true);
		ParquetThriftOutputFormat.setEnableDictionary(job, false);

		datedims.output(hadoopOutputFormat);
	}

	private static void createItem(ExecutionEnvironment env) throws IOException {
		DataSet<Tuple2<Void, ItemTable>> items = getItemDataSet(env).map(new ItemToParquet());

		Job job = Job.getInstance();

		HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ParquetThriftOutputFormat(), job);

		ParquetThriftOutputFormat.setOutputPath(job, new Path(outputPath + "/item"));
		ParquetThriftOutputFormat.setThriftClass(job, ItemTable.class);
		ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		ParquetThriftOutputFormat.setCompressOutput(job, true);
		ParquetThriftOutputFormat.setEnableDictionary(job, false);

		items.output(hadoopOutputFormat);
	}

	private static void createStoreSales(ExecutionEnvironment env) throws IOException {
		DataSet<Tuple2<Void, StoreSalesTable>> storeSales = getStoreSalesDataSet(env).map(new StoreSalesToParquet());

		Job job = Job.getInstance();

		HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ParquetThriftOutputFormat(), job);

		ParquetThriftOutputFormat.setOutputPath(job, new Path(outputPath + "/storesales"));
		ParquetThriftOutputFormat.setThriftClass(job, StoreSalesTable.class);
		ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		ParquetThriftOutputFormat.setCompressOutput(job, true);
		ParquetThriftOutputFormat.setEnableDictionary(job, false);

		storeSales.output(hadoopOutputFormat);
	}


	private static String dateDimPath;
	private static String storeSalesPath;
	private static String itemPath;
	private static String outputPath;

	private static boolean parseParameters(String[] programArguments) {

		if (programArguments.length > 0) {
			if (programArguments.length == 4) {
				dateDimPath = programArguments[0];
				storeSalesPath = programArguments[1];
				itemPath = programArguments[2];
				outputPath = programArguments[3];
			} else {
				System.err.println("Usage: CSVToParquet <date_dim-csv path> <store_sales-csv path> <item-csv path> " +
					"<result path>");
				return false;
			}
		} else {
			System.err.println("This program expects data from the TPC-DS benchmark as input data.\n" +
				"  Due to legal restrictions, we can not ship generated data.\n" +
				"  You can find the TPC-DS data generator at http://www.tpc.org/tpcds/.\n" +
				"  Usage: CSVToParquet <date_dim-csv path> <store_sales-csv path> <item-csv path> <result path>");
			return false;
		}
		return true;
	}


	public static class DateDimString {
		public Long f0;
		public String f1;
		public String f2;
		public String f3;
		public String f4;
		public String f5;
		public String f6;
		public String f7;
		public String f8;
		public String f9;
		public String f10;
		public String f11;
		public String f12;
		public String f13;
		public String f14;
		public String f15;
		public String f16;
		public String f17;
		public String f18;
		public String f19;
		public String f20;
		public String f21;
		public String f22;
		public String f23;
		public String f24;
		public String f25;
		public String f26;
		public String f27;
	}

	public static class ItemString extends Tuple22<Long, String, String, String, String, String, String, String, 
		String, String, String, String, String, String, String, String, String, String, String, String, String, 
		String> {
	}

	public static class StoreSalesString extends Tuple23<String, String, Long, String, String, String, String, String,
		String, Long, String, String, String, String, String, String, String, String, String, String, String, String, 
		String> {
	}


	public static class ItemToParquet implements MapFunction<ItemString, Tuple2<Void, ItemTable>> {
		public Tuple2<Void, ItemTable> map(ItemString csv) {
			ItemTable items = new ItemTable();
			items.setI_item_sk(csv.f0);
			items.setI_item_id(csv.f1);
			items.setI_rec_start_date(csv.f2);
			items.setI_rec_end_date(csv.f3);
			items.setI_item_desc(csv.f4);
			items.setI_current_price(Util.emptyStringToDoubleZero(csv.f5));
			items.setI_wholesale_cost(Util.emptyStringToDoubleZero(csv.f6));
			items.setI_brand_id(Util.emptyStringToLongZero(csv.f7));
			items.setI_brand(csv.f8);
			items.setI_class_id(Util.emptyStringToLongZero(csv.f9));
			items.setI_class(csv.f10);
			items.setI_category_id(Util.emptyStringToLongZero(csv.f11));
			items.setI_category(csv.f12);
			items.setI_manufact_id(Util.emptyStringToLongZero(csv.f13));
			items.setI_manufact(csv.f14);
			items.setI_size(csv.f15);
			items.setI_formulation(csv.f16);
			items.setI_color(csv.f17);
			items.setI_units(csv.f18);
			items.setI_container(csv.f19);
			items.setI_manager_id(Util.emptyStringToLongZero(csv.f20));
			items.setI_product_name(csv.f21);

			return new Tuple2<Void, ItemTable>(null, items);
		}
	}


	public static class StoreSalesToParquet implements MapFunction<StoreSalesString, Tuple2<Void, StoreSalesTable>> {
		public Tuple2<Void, StoreSalesTable> map(StoreSalesString csv) {
			StoreSalesTable storesales = new StoreSalesTable();
			storesales.setSs_sold_date_sk(Util.emptyStringToLongZero(csv.f0));
			storesales.setSs_sold_time_sk(Util.emptyStringToLongZero(csv.f1));
			storesales.setSs_item_sk(csv.f2);
			storesales.setSs_customer_sk(Util.emptyStringToLongZero(csv.f3));
			storesales.setSs_cdemo_sk(Util.emptyStringToLongZero(csv.f4));
			storesales.setSs_hdemo_sk(Util.emptyStringToLongZero(csv.f5));
			storesales.setSs_addr_sk(Util.emptyStringToLongZero(csv.f6));
			storesales.setSs_store_sk(Util.emptyStringToLongZero(csv.f7));
			storesales.setSs_promo_sk(Util.emptyStringToLongZero(csv.f8));
			storesales.setSs_ticket_number(csv.f9);
			storesales.setSs_quantity(Util.emptyStringToLongZero(csv.f10));
			storesales.setSs_wholesale_cost(Util.emptyStringToDoubleZero(csv.f11));
			storesales.setSs_list_price(Util.emptyStringToDoubleZero(csv.f12));
			storesales.setSs_sales_price(Util.emptyStringToDoubleZero(csv.f13));
			storesales.setSs_ext_discount_amt(Util.emptyStringToDoubleZero(csv.f14));
			storesales.setSs_ext_sales_price(Util.emptyStringToDoubleZero(csv.f15));
			storesales.setSs_ext_wholesale_cost(Util.emptyStringToDoubleZero(csv.f16));
			storesales.setSs_ext_list_price(Util.emptyStringToDoubleZero(csv.f17));
			storesales.setSs_ext_tax(Util.emptyStringToDoubleZero(csv.f18));
			storesales.setSs_coupon_amt(Util.emptyStringToDoubleZero(csv.f19));
			storesales.setSs_net_paid(Util.emptyStringToDoubleZero(csv.f20));
			storesales.setSs_net_paid_inc_tax(Util.emptyStringToDoubleZero(csv.f21));
			storesales.setSs_net_profit(Util.emptyStringToDoubleZero(csv.f22));

			return new Tuple2<Void, StoreSalesTable>(null, storesales);
		}

	}


	public static class DateDimToParquet implements MapFunction<DateDimString, Tuple2<Void, DateDimTable>> {
		public Tuple2<Void, DateDimTable> map(DateDimString csv) {
			DateDimTable datedims = new DateDimTable();
			datedims.setD_date_sk(csv.f0);
			datedims.setD_date_id(csv.f1);
			datedims.setD_date(csv.f2);
			datedims.setD_month_seq(Util.emptyStringToLongZero(csv.f3));
			datedims.setD_week_seq(Util.emptyStringToLongZero(csv.f4));
			datedims.setD_quarter_seq(Util.emptyStringToLongZero(csv.f5));
			datedims.setD_year(Util.emptyStringToLongZero(csv.f6));
			datedims.setD_dow(Util.emptyStringToLongZero(csv.f7));
			datedims.setD_moy(Util.emptyStringToLongZero(csv.f8));
			datedims.setD_dom(Util.emptyStringToLongZero(csv.f9));
			datedims.setD_qoy(Util.emptyStringToLongZero(csv.f10));
			datedims.setD_fy_year(Util.emptyStringToLongZero(csv.f11));
			datedims.setD_fy_quarter_seq(Util.emptyStringToLongZero(csv.f12));
			datedims.setD_fy_week_seq(Util.emptyStringToLongZero(csv.f13));
			datedims.setD_day_name(csv.f14);
			datedims.setD_quarter_name(csv.f15);
			datedims.setD_holiday(csv.f16);
			datedims.setD_weekend(csv.f17);
			datedims.setD_following_holiday(csv.f18);
			datedims.setD_first_dom(Util.emptyStringToLongZero(csv.f19));
			datedims.setD_last_dom(Util.emptyStringToLongZero(csv.f20));
			datedims.setD_same_day_ly(Util.emptyStringToLongZero(csv.f21));
			datedims.setD_same_day_lq(Util.emptyStringToLongZero(csv.f22));
			datedims.setD_current_day(csv.f23);
			datedims.setD_current_week(csv.f24);
			datedims.setD_current_month(csv.f25);
			datedims.setD_current_quarter(csv.f26);
			datedims.setD_current_year(csv.f27);

			return new Tuple2<Void, DateDimTable>(null, datedims);
		}
	}


	private static DataSet<DateDimString> getDateDimDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(dateDimPath)
			.fieldDelimiter('|')
			.pojoType(DateDimString.class, new String[]{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", 
				"f10", "f11", "f12", "f13", "f14", "f15", "f16", "f17", "f18", "f19", "f20", "f21", "f22", "f23", 
				"f24", "f25", "f26", "f27"});
	}

	private static DataSet<StoreSalesString> getStoreSalesDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(storeSalesPath)
			.fieldDelimiter('|')
			.tupleType(StoreSalesString.class);
	}

	private static DataSet<ItemString> getItemDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(itemPath)
			.fieldDelimiter('|').tupleType(ItemString.class);
	}
}
