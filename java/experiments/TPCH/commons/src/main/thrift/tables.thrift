namespace java flink.parquet.thrift

struct NationTable {
	1: optional i64 ID,
	2: optional string NAME,
	3: optional i64 REGIONKEY,
	4: optional string COMMENT
}

struct SupplierTable {
	1: optional i64 ID,
	2: optional string NAME,
	3: optional string ADDRESS,
	4: optional i64 NATIONKEY,
	5: optional string PHONE,
	6: optional double ACCTBAL,
	7: optional string COMMENT
}

struct PartsuppTable {
	1: optional i64 PARTKEY,	
	2: optional i64 SUPPKEY,
	3: optional i64 AVAILQTY,
	4: optional double SUPPLYCOST,
	5: optional string COMMENT
}

struct CustomerTable {
	1: optional i64 ID,
	2: optional string NAME,
	3: optional string ADDRESS,
	4: optional i64 NATIONKEY,
	5: optional string PHONE,
	6: optional double ACCTBAL,
	7: optional string MKTSEGMENT,
	8: optional string COMMENT
}


struct OrderTable {
	1: optional i64 ID,
	2: optional i64 CUSTKEY,
	3: optional string ORDERSTATUS,
	4: optional double TOTALPRICE,
	5: optional string ORDERDATE,
	6: optional string ORDER_PRIORITY,
	7: optional string CLERK,
	8: optional i32 SHIP_PRIORITY,
	9: optional string COMMENT
}

struct LineitemTable {
	1: optional i64 ORDERKEY,
	2: optional i64 PARTKEY,	
	3: optional i64 SUPPKEY,
	4: optional i64 LINENUMBER,
	5: optional double QUANTITY,
	6: optional double EXTENDEDPRICE,
	7: optional double DISCOUNT,
	8: optional double TAX,
	9: optional string RETURNFLAG,
	10: optional string LINESTATUS,
	11: optional string SHIPDATE,
	12: optional string COMMITDATE,
	13: optional string RECEIPTDATE,
	14: optional string SHIPINSTRUCT,
	15: optional string SHIPMODE,
	16: optional string COMMENT
}
