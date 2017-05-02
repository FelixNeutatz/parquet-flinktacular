namespace java flink.parquet.thrift
/*
enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
}
*/
struct PhoneNumber {
	1: required string number,
	//2: optional PhoneType type = PhoneType.HOME;
}

struct Person {
	1: required string name,
	2: required i32 id,
	3: optional string email,
	4: list<PhoneNumber> phone
}


