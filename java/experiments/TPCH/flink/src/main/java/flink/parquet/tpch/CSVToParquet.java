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

package flink.parquet.tpch;

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
        if(!parseParameters(args)) {
            return;
        }
        
        int splitNumber = 4;
        
		
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setDefaultLocalParallelism(splitNumber);
        createCustomers(env);
        env.execute("convert customers");
		
        final ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
        env2.setDefaultLocalParallelism(splitNumber);
        createOrders(env2);
        env2.execute("convert orders");
		
		final ExecutionEnvironment env3 = ExecutionEnvironment.getExecutionEnvironment();
        env3.setDefaultLocalParallelism(splitNumber);
        createLineitems(env3);
        env3.execute("convert lineitems");        
    }
    
    private static void createLineitems(ExecutionEnvironment env) throws IOException {
        DataSet<Tuple2<Void,LineitemTable>> lineitems = getLineitemDataSet(env).map(new LineitemToParquet());

        Job job = Job.getInstance();
        
        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ParquetThriftOutputFormat(), job);

        ParquetThriftOutputFormat.setOutputPath(job, new Path(outputPath + "/lineitems"));
        ParquetThriftOutputFormat.setThriftClass(job, LineitemTable.class);
        ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetThriftOutputFormat.setCompressOutput(job, true);
        ParquetThriftOutputFormat.setEnableDictionary(job, true);

        lineitems.output(hadoopOutputFormat);
    }

    private static void createOrders(ExecutionEnvironment env) throws IOException {
        DataSet<Tuple2<Void,OrderTable>> orders = getOrdersDataSet(env).map(new OrdersToParquet());

        Job job = Job.getInstance();

        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ParquetThriftOutputFormat(), job);

        ParquetThriftOutputFormat.setOutputPath(job, new Path(outputPath + "/orders"));
        ParquetThriftOutputFormat.setThriftClass(job, OrderTable.class);
        ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetThriftOutputFormat.setCompressOutput(job, true);
        ParquetThriftOutputFormat.setEnableDictionary(job, true);

        orders.output(hadoopOutputFormat);
    }

    private static void createCustomers(ExecutionEnvironment env) throws IOException {
        DataSet<Tuple2<Void,CustomerTable>> customers = getCustomerDataSet(env).map(new CustomerToParquet());

        Job job = Job.getInstance();

        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new ParquetThriftOutputFormat(), job);

        ParquetThriftOutputFormat.setOutputPath(job, new Path(outputPath + "/cust"));
        ParquetThriftOutputFormat.setThriftClass(job, CustomerTable.class);
        ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetThriftOutputFormat.setCompressOutput(job, true);
        ParquetThriftOutputFormat.setEnableDictionary(job, true);

        customers.output(hadoopOutputFormat);
    }
    

    private static String lineitemPath;
    private static String customerPath;
    private static String ordersPath;
    private static String outputPath;

    private static boolean parseParameters(String[] programArguments) {

        if(programArguments.length > 0) {
            if(programArguments.length == 4) {
                lineitemPath = programArguments[0];
                customerPath = programArguments[1];
                ordersPath = programArguments[2];
                outputPath = programArguments[3];
            } else {
                System.err.println("Usage: CSVToParquet <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>");
                return false;
            }
        } else {
            System.err.println("This program expects data from the TPC-H benchmark as input data.\n" +
                    "  Due to legal restrictions, we can not ship generated data.\n" +
                    "  You can find the TPC-H data generator at http://www.tpc.org/tpch/.\n" +
                    "  Usage: CSVToParquet <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>");
            return false;
        }
        return true;
    }


    public static class Lineitem extends Tuple16<Long, Long, Long, Long,Double,Double,Double,Double,String,String,String,String,String,String,String,String> {
    }
    public static class Orders extends Tuple9<Long, Long, String, Double, String, String, String, Integer, String> {
    }
    public static class Customer extends Tuple8<Long, String, String, Long, String, Double, String, String> {
    }
        
    public static class OrdersToParquet implements MapFunction<Orders, Tuple2<Void,OrderTable>> {
        public Tuple2<Void, OrderTable> map(Orders csv) {
            OrderTable orders = new OrderTable();
            orders.setID(csv.f0);
            orders.setCUSTKEY(csv.f1);
            orders.setORDERSTATUS(csv.f2);
            orders.setTOTALPRICE(csv.f3);
            orders.setORDERDATE(csv.f4);
            orders.setORDER_PRIORITY(csv.f5);
            orders.setCLERK(csv.f6);
            orders.setSHIP_PRIORITY(csv.f7);
            orders.setCOMMENT(csv.f8);
            return new Tuple2<Void, OrderTable>(null,orders);
        }
    }

    public static class CustomerToParquet implements MapFunction<Customer, Tuple2<Void,CustomerTable>> {
        public Tuple2<Void,CustomerTable> map(Customer csv) {
            CustomerTable customer = new CustomerTable();
            customer.setID(csv.f0);
            customer.setNAME(csv.f1);
            customer.setADDRESS(csv.f2);
            customer.setNATIONKEY(csv.f3);
            customer.setPHONE(csv.f4);
            customer.setACCTBAL(csv.f5);
            customer.setMKTSEGMENT(csv.f6);
            customer.setCOMMENT(csv.f7);
            return new Tuple2<Void, CustomerTable>(null,customer);
        }
                
    }

    public static class LineitemToParquet implements MapFunction<Lineitem, Tuple2<Void,LineitemTable>> {
        public Tuple2<Void,LineitemTable> map(Lineitem csv) {
            LineitemTable lineitem = new LineitemTable();
            lineitem.setORDERKEY(csv.f0);
            lineitem.setPARTKEY(csv.f1);
            lineitem.setSUPPKEY(csv.f2);
            lineitem.setLINENUMBER(csv.f3);
            lineitem.setQUANTITY(csv.f4);
            lineitem.setEXTENDEDPRICE(csv.f5);
            lineitem.setDISCOUNT(csv.f6);
            lineitem.setTAX(csv.f7);
            lineitem.setRETURNFLAG(csv.f8);
            lineitem.setLINESTATUS(csv.f9);
            lineitem.setSHIPDATE(csv.f10);
            lineitem.setCOMMITDATE(csv.f11);
            lineitem.setRECEIPTDATE(csv.f12);
            lineitem.setSHIPINSTRUCT(csv.f13);
            lineitem.setSHIPMODE(csv.f14);
            lineitem.setCOMMENT(csv.f15);
            return new Tuple2<Void, LineitemTable>(null,lineitem);
        }
    }        

    private static DataSet<Lineitem> getLineitemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(lineitemPath)
                .fieldDelimiter('|')
                .tupleType(Lineitem.class);
    }

    private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customerPath)
                .fieldDelimiter('|')
                .tupleType(Customer.class);
    }

    private static DataSet<Orders> getOrdersDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(ordersPath)
                .fieldDelimiter('|').tupleType(Orders.class);
    }
}
