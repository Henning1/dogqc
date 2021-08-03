#include <list>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <ctime>
#include <limits.h>
#include <float.h>
#include "../dogqc/include/csv.h"
#include "../dogqc/include/util.h"
#include "../dogqc/include/mappedmalloc.h"
int toDate ( const char* c ) {    int d=0;    d += (int)( c[0] - 48 ) * 10000000;    d += (int)( c[1] - 48 ) *  1000000;    d += (int)( c[2] - 48 ) *   100000;    d += (int)( c[3] - 48 ) *    10000;    d += (int)( c[5] - 48 ) *     1000;    d += (int)( c[6] - 48 ) *      100;    d += (int)( c[8] - 48 ) *       10;    d += (int)( c[9] - 48 ) *        1;    return d;}

int main() {
io::CSVReader <17, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader0("/home/jigao/Desktop/tpch-dbgen//lineitem.tbl");
int l_orderkey;
int* lineitem_l_orderkey;
lineitem_l_orderkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 6001215), "mmdb/lineitem_l_orderkey" );
int l_partkey;
int* lineitem_l_partkey;
lineitem_l_partkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 6001215), "mmdb/lineitem_l_partkey" );
int l_suppkey;
int* lineitem_l_suppkey;
lineitem_l_suppkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 6001215), "mmdb/lineitem_l_suppkey" );
int l_linenumber;
int* lineitem_l_linenumber;
lineitem_l_linenumber = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 6001215), "mmdb/lineitem_l_linenumber" );
int l_quantity;
int* lineitem_l_quantity;
lineitem_l_quantity = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 6001215), "mmdb/lineitem_l_quantity" );
float l_extendedprice;
float* lineitem_l_extendedprice;
lineitem_l_extendedprice = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 6001215), "mmdb/lineitem_l_extendedprice" );
float l_discount;
float* lineitem_l_discount;
lineitem_l_discount = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 6001215), "mmdb/lineitem_l_discount" );
float l_tax;
float* lineitem_l_tax;
lineitem_l_tax = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 6001215), "mmdb/lineitem_l_tax" );
char l_returnflag;
char* lineitem_l_returnflag;
lineitem_l_returnflag = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * 6001215), "mmdb/lineitem_l_returnflag" );
char l_linestatus;
char* lineitem_l_linestatus;
lineitem_l_linestatus = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * 6001215), "mmdb/lineitem_l_linestatus" );
std::string l_shipdate;
unsigned* lineitem_l_shipdate;
lineitem_l_shipdate = ( unsigned*) malloc_memory_mapped_file ( (sizeof ( unsigned) * 6001215), "mmdb/lineitem_l_shipdate" );
std::string l_commitdate;
unsigned* lineitem_l_commitdate;
lineitem_l_commitdate = ( unsigned*) malloc_memory_mapped_file ( (sizeof ( unsigned) * 6001215), "mmdb/lineitem_l_commitdate" );
std::string l_receiptdate;
unsigned* lineitem_l_receiptdate;
lineitem_l_receiptdate = ( unsigned*) malloc_memory_mapped_file ( (sizeof ( unsigned) * 6001215), "mmdb/lineitem_l_receiptdate" );
std::string l_shipinstruct;
size_t lineitem_l_shipinstruct_len = 0;
size_t* lineitem_l_shipinstruct_offset;
lineitem_l_shipinstruct_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 6001216), "mmdb/lineitem_l_shipinstruct_offset" );
lineitem_l_shipinstruct_offset[0] = 0;
std::string l_shipmode;
size_t lineitem_l_shipmode_len = 0;
size_t* lineitem_l_shipmode_offset;
lineitem_l_shipmode_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 6001216), "mmdb/lineitem_l_shipmode_offset" );
lineitem_l_shipmode_offset[0] = 0;
std::string l_comment;
size_t lineitem_l_comment_len = 0;
size_t* lineitem_l_comment_offset;
lineitem_l_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 6001216), "mmdb/lineitem_l_comment_offset" );
lineitem_l_comment_offset[0] = 0;
char* nothing_lineitem;
int i = 0;
while(reader0.read_row(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,nothing_lineitem)) {
if((i > 6001215)) {
std::cout << "error";
}
lineitem_l_orderkey[i] = l_orderkey;
lineitem_l_partkey[i] = l_partkey;
lineitem_l_suppkey[i] = l_suppkey;
lineitem_l_linenumber[i] = l_linenumber;
lineitem_l_quantity[i] = l_quantity;
lineitem_l_extendedprice[i] = l_extendedprice;
lineitem_l_discount[i] = l_discount;
lineitem_l_tax[i] = l_tax;
lineitem_l_returnflag[i] = l_returnflag;
lineitem_l_linestatus[i] = l_linestatus;
lineitem_l_shipdate[i] = toDate (l_shipdate.c_str());
lineitem_l_commitdate[i] = toDate (l_commitdate.c_str());
lineitem_l_receiptdate[i] = toDate (l_receiptdate.c_str());
lineitem_l_shipinstruct_len += l_shipinstruct.length();
lineitem_l_shipinstruct_offset[(i + 1)] = lineitem_l_shipinstruct_len;
lineitem_l_shipmode_len += l_shipmode.length();
lineitem_l_shipmode_offset[(i + 1)] = lineitem_l_shipmode_len;
lineitem_l_comment_len += l_comment.length();
lineitem_l_comment_offset[(i + 1)] = lineitem_l_comment_len;
i++;
}
char* lineitem_l_shipinstruct_char;
lineitem_l_shipinstruct_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * lineitem_l_shipinstruct_len), "mmdb/lineitem_l_shipinstruct_char" );
char* lineitem_l_shipmode_char;
lineitem_l_shipmode_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * lineitem_l_shipmode_len), "mmdb/lineitem_l_shipmode_char" );
char* lineitem_l_comment_char;
lineitem_l_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * lineitem_l_comment_len), "mmdb/lineitem_l_comment_char" );
io::CSVReader <17, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader1("/home/jigao/Desktop/tpch-dbgen//lineitem.tbl");
int j = 0;
while(reader1.read_row(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,nothing_lineitem)) {
strcpy ( &(lineitem_l_shipinstruct_char[lineitem_l_shipinstruct_offset[j]]), l_shipinstruct.c_str());
strcpy ( &(lineitem_l_shipmode_char[lineitem_l_shipmode_offset[j]]), l_shipmode.c_str());
strcpy ( &(lineitem_l_comment_char[lineitem_l_comment_offset[j]]), l_comment.c_str());
j++;
}

io::CSVReader <9, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader2("/home/jigao/Desktop/tpch-dbgen//customer.tbl");
int c_custkey;
int* customer_c_custkey;
customer_c_custkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 150000), "mmdb/customer_c_custkey" );
std::string c_name;
size_t customer_c_name_len = 0;
size_t* customer_c_name_offset;
customer_c_name_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 150001), "mmdb/customer_c_name_offset" );
customer_c_name_offset[0] = 0;
std::string c_address;
size_t customer_c_address_len = 0;
size_t* customer_c_address_offset;
customer_c_address_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 150001), "mmdb/customer_c_address_offset" );
customer_c_address_offset[0] = 0;
int c_nationkey;
int* customer_c_nationkey;
customer_c_nationkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 150000), "mmdb/customer_c_nationkey" );
std::string c_phone;
size_t customer_c_phone_len = 0;
size_t* customer_c_phone_offset;
customer_c_phone_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 150001), "mmdb/customer_c_phone_offset" );
customer_c_phone_offset[0] = 0;
float c_acctbal;
float* customer_c_acctbal;
customer_c_acctbal = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 150000), "mmdb/customer_c_acctbal" );
std::string c_mktsegment;
size_t customer_c_mktsegment_len = 0;
size_t* customer_c_mktsegment_offset;
customer_c_mktsegment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 150001), "mmdb/customer_c_mktsegment_offset" );
customer_c_mktsegment_offset[0] = 0;
std::string c_comment;
size_t customer_c_comment_len = 0;
size_t* customer_c_comment_offset;
customer_c_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 150001), "mmdb/customer_c_comment_offset" );
customer_c_comment_offset[0] = 0;
char* nothing_customer;
int k = 0;
while(reader2.read_row(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment,nothing_customer)) {
if((k > 150000)) {
std::cout << "error";
}
customer_c_custkey[k] = c_custkey;
customer_c_nationkey[k] = c_nationkey;
customer_c_acctbal[k] = c_acctbal;
customer_c_name_len += c_name.length();
customer_c_name_offset[(k + 1)] = customer_c_name_len;
customer_c_address_len += c_address.length();
customer_c_address_offset[(k + 1)] = customer_c_address_len;
customer_c_phone_len += c_phone.length();
customer_c_phone_offset[(k + 1)] = customer_c_phone_len;
customer_c_mktsegment_len += c_mktsegment.length();
customer_c_mktsegment_offset[(k + 1)] = customer_c_mktsegment_len;
customer_c_comment_len += c_comment.length();
customer_c_comment_offset[(k + 1)] = customer_c_comment_len;
k++;
}
char* customer_c_name_char;
customer_c_name_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * customer_c_name_len), "mmdb/customer_c_name_char" );
char* customer_c_address_char;
customer_c_address_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * customer_c_address_len), "mmdb/customer_c_address_char" );
char* customer_c_phone_char;
customer_c_phone_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * customer_c_phone_len), "mmdb/customer_c_phone_char" );
char* customer_c_mktsegment_char;
customer_c_mktsegment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * customer_c_mktsegment_len), "mmdb/customer_c_mktsegment_char" );
char* customer_c_comment_char;
customer_c_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * customer_c_comment_len), "mmdb/customer_c_comment_char" );
io::CSVReader <9, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader3("/home/jigao/Desktop/tpch-dbgen//customer.tbl");
int l = 0;
while(reader3.read_row(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment,nothing_customer)) {
strcpy ( &(customer_c_name_char[customer_c_name_offset[l]]), c_name.c_str());
strcpy ( &(customer_c_address_char[customer_c_address_offset[l]]), c_address.c_str());
strcpy ( &(customer_c_phone_char[customer_c_phone_offset[l]]), c_phone.c_str());
strcpy ( &(customer_c_mktsegment_char[customer_c_mktsegment_offset[l]]), c_mktsegment.c_str());
strcpy ( &(customer_c_comment_char[customer_c_comment_offset[l]]), c_comment.c_str());
l++;
}

io::CSVReader <10, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader4("/home/jigao/Desktop/tpch-dbgen//orders.tbl");
int o_orderkey;
int* orders_o_orderkey;
orders_o_orderkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 1500000), "mmdb/orders_o_orderkey" );
int o_custkey;
int* orders_o_custkey;
orders_o_custkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 1500000), "mmdb/orders_o_custkey" );
char o_orderstatus;
char* orders_o_orderstatus;
orders_o_orderstatus = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * 1500000), "mmdb/orders_o_orderstatus" );
float o_totalprice;
float* orders_o_totalprice;
orders_o_totalprice = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 1500000), "mmdb/orders_o_totalprice" );
std::string o_orderdate;
unsigned* orders_o_orderdate;
orders_o_orderdate = ( unsigned*) malloc_memory_mapped_file ( (sizeof ( unsigned) * 1500000), "mmdb/orders_o_orderdate" );
std::string o_orderpriority;
size_t orders_o_orderpriority_len = 0;
size_t* orders_o_orderpriority_offset;
orders_o_orderpriority_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 1500001), "mmdb/orders_o_orderpriority_offset" );
orders_o_orderpriority_offset[0] = 0;
std::string o_clerk;
size_t orders_o_clerk_len = 0;
size_t* orders_o_clerk_offset;
orders_o_clerk_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 1500001), "mmdb/orders_o_clerk_offset" );
orders_o_clerk_offset[0] = 0;
int o_shippriority;
int* orders_o_shippriority;
orders_o_shippriority = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 1500000), "mmdb/orders_o_shippriority" );
std::string o_comment;
size_t orders_o_comment_len = 0;
size_t* orders_o_comment_offset;
orders_o_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 1500001), "mmdb/orders_o_comment_offset" );
orders_o_comment_offset[0] = 0;
char* nothing_orders;
int m = 0;
while(reader4.read_row(o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment,nothing_orders)) {
if((m > 1500000)) {
std::cout << "error";
}
orders_o_orderkey[m] = o_orderkey;
orders_o_custkey[m] = o_custkey;
orders_o_orderstatus[m] = o_orderstatus;
orders_o_totalprice[m] = o_totalprice;
orders_o_orderdate[m] = toDate (o_orderdate.c_str());
orders_o_shippriority[m] = o_shippriority;
orders_o_orderpriority_len += o_orderpriority.length();
orders_o_orderpriority_offset[(m + 1)] = orders_o_orderpriority_len;
orders_o_clerk_len += o_clerk.length();
orders_o_clerk_offset[(m + 1)] = orders_o_clerk_len;
orders_o_comment_len += o_comment.length();
orders_o_comment_offset[(m + 1)] = orders_o_comment_len;
m++;
}
char* orders_o_orderpriority_char;
orders_o_orderpriority_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * orders_o_orderpriority_len), "mmdb/orders_o_orderpriority_char" );
char* orders_o_clerk_char;
orders_o_clerk_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * orders_o_clerk_len), "mmdb/orders_o_clerk_char" );
char* orders_o_comment_char;
orders_o_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * orders_o_comment_len), "mmdb/orders_o_comment_char" );
io::CSVReader <10, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader5("/home/jigao/Desktop/tpch-dbgen//orders.tbl");
int n = 0;
while(reader5.read_row(o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment,nothing_orders)) {
strcpy ( &(orders_o_orderpriority_char[orders_o_orderpriority_offset[n]]), o_orderpriority.c_str());
strcpy ( &(orders_o_clerk_char[orders_o_clerk_offset[n]]), o_clerk.c_str());
strcpy ( &(orders_o_comment_char[orders_o_comment_offset[n]]), o_comment.c_str());
n++;
}

io::CSVReader <6, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader6("/home/jigao/Desktop/tpch-dbgen//partsupp.tbl");
int ps_partkey;
int* partsupp_ps_partkey;
partsupp_ps_partkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 800000), "mmdb/partsupp_ps_partkey" );
int ps_suppkey;
int* partsupp_ps_suppkey;
partsupp_ps_suppkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 800000), "mmdb/partsupp_ps_suppkey" );
int ps_availqty;
int* partsupp_ps_availqty;
partsupp_ps_availqty = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 800000), "mmdb/partsupp_ps_availqty" );
float ps_supplycost;
float* partsupp_ps_supplycost;
partsupp_ps_supplycost = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 800000), "mmdb/partsupp_ps_supplycost" );
std::string ps_comment;
size_t partsupp_ps_comment_len = 0;
size_t* partsupp_ps_comment_offset;
partsupp_ps_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 800001), "mmdb/partsupp_ps_comment_offset" );
partsupp_ps_comment_offset[0] = 0;
char* nothing_partsupp;
int o = 0;
while(reader6.read_row(ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment,nothing_partsupp)) {
if((o > 800000)) {
std::cout << "error";
}
partsupp_ps_partkey[o] = ps_partkey;
partsupp_ps_suppkey[o] = ps_suppkey;
partsupp_ps_availqty[o] = ps_availqty;
partsupp_ps_supplycost[o] = ps_supplycost;
partsupp_ps_comment_len += ps_comment.length();
partsupp_ps_comment_offset[(o + 1)] = partsupp_ps_comment_len;
o++;
}
char* partsupp_ps_comment_char;
partsupp_ps_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * partsupp_ps_comment_len), "mmdb/partsupp_ps_comment_char" );
io::CSVReader <6, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader7("/home/jigao/Desktop/tpch-dbgen//partsupp.tbl");
int p = 0;
while(reader7.read_row(ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment,nothing_partsupp)) {
strcpy ( &(partsupp_ps_comment_char[partsupp_ps_comment_offset[p]]), ps_comment.c_str());
p++;
}

io::CSVReader <10, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader8("/home/jigao/Desktop/tpch-dbgen//part.tbl");
int p_partkey;
int* part_p_partkey;
part_p_partkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 200000), "mmdb/part_p_partkey" );
std::string p_name;
size_t part_p_name_len = 0;
size_t* part_p_name_offset;
part_p_name_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 200001), "mmdb/part_p_name_offset" );
part_p_name_offset[0] = 0;
std::string p_mfgr;
size_t part_p_mfgr_len = 0;
size_t* part_p_mfgr_offset;
part_p_mfgr_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 200001), "mmdb/part_p_mfgr_offset" );
part_p_mfgr_offset[0] = 0;
std::string p_brand;
size_t part_p_brand_len = 0;
size_t* part_p_brand_offset;
part_p_brand_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 200001), "mmdb/part_p_brand_offset" );
part_p_brand_offset[0] = 0;
std::string p_type;
size_t part_p_type_len = 0;
size_t* part_p_type_offset;
part_p_type_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 200001), "mmdb/part_p_type_offset" );
part_p_type_offset[0] = 0;
int p_size;
int* part_p_size;
part_p_size = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 200000), "mmdb/part_p_size" );
std::string p_container;
size_t part_p_container_len = 0;
size_t* part_p_container_offset;
part_p_container_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 200001), "mmdb/part_p_container_offset" );
part_p_container_offset[0] = 0;
float p_retailprice;
float* part_p_retailprice;
part_p_retailprice = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 200000), "mmdb/part_p_retailprice" );
std::string p_comment;
size_t part_p_comment_len = 0;
size_t* part_p_comment_offset;
part_p_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 200001), "mmdb/part_p_comment_offset" );
part_p_comment_offset[0] = 0;
char* nothing_part;
int q = 0;
while(reader8.read_row(p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment,nothing_part)) {
if((q > 200000)) {
std::cout << "error";
}
part_p_partkey[q] = p_partkey;
part_p_size[q] = p_size;
part_p_retailprice[q] = p_retailprice;
part_p_name_len += p_name.length();
part_p_name_offset[(q + 1)] = part_p_name_len;
part_p_mfgr_len += p_mfgr.length();
part_p_mfgr_offset[(q + 1)] = part_p_mfgr_len;
part_p_brand_len += p_brand.length();
part_p_brand_offset[(q + 1)] = part_p_brand_len;
part_p_type_len += p_type.length();
part_p_type_offset[(q + 1)] = part_p_type_len;
part_p_container_len += p_container.length();
part_p_container_offset[(q + 1)] = part_p_container_len;
part_p_comment_len += p_comment.length();
part_p_comment_offset[(q + 1)] = part_p_comment_len;
q++;
}
char* part_p_name_char;
part_p_name_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * part_p_name_len), "mmdb/part_p_name_char" );
char* part_p_mfgr_char;
part_p_mfgr_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * part_p_mfgr_len), "mmdb/part_p_mfgr_char" );
char* part_p_brand_char;
part_p_brand_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * part_p_brand_len), "mmdb/part_p_brand_char" );
char* part_p_type_char;
part_p_type_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * part_p_type_len), "mmdb/part_p_type_char" );
char* part_p_container_char;
part_p_container_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * part_p_container_len), "mmdb/part_p_container_char" );
char* part_p_comment_char;
part_p_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * part_p_comment_len), "mmdb/part_p_comment_char" );
io::CSVReader <10, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader9("/home/jigao/Desktop/tpch-dbgen//part.tbl");
int r = 0;
while(reader9.read_row(p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment,nothing_part)) {
strcpy ( &(part_p_name_char[part_p_name_offset[r]]), p_name.c_str());
strcpy ( &(part_p_mfgr_char[part_p_mfgr_offset[r]]), p_mfgr.c_str());
strcpy ( &(part_p_brand_char[part_p_brand_offset[r]]), p_brand.c_str());
strcpy ( &(part_p_type_char[part_p_type_offset[r]]), p_type.c_str());
strcpy ( &(part_p_container_char[part_p_container_offset[r]]), p_container.c_str());
strcpy ( &(part_p_comment_char[part_p_comment_offset[r]]), p_comment.c_str());
r++;
}

io::CSVReader <8, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader10("/home/jigao/Desktop/tpch-dbgen//supplier.tbl");
int s_suppkey;
int* supplier_s_suppkey;
supplier_s_suppkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 10000), "mmdb/supplier_s_suppkey" );
std::string s_name;
size_t supplier_s_name_len = 0;
size_t* supplier_s_name_offset;
supplier_s_name_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 10001), "mmdb/supplier_s_name_offset" );
supplier_s_name_offset[0] = 0;
std::string s_address;
size_t supplier_s_address_len = 0;
size_t* supplier_s_address_offset;
supplier_s_address_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 10001), "mmdb/supplier_s_address_offset" );
supplier_s_address_offset[0] = 0;
int s_nationkey;
int* supplier_s_nationkey;
supplier_s_nationkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 10000), "mmdb/supplier_s_nationkey" );
std::string s_phone;
size_t supplier_s_phone_len = 0;
size_t* supplier_s_phone_offset;
supplier_s_phone_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 10001), "mmdb/supplier_s_phone_offset" );
supplier_s_phone_offset[0] = 0;
float s_acctbal;
float* supplier_s_acctbal;
supplier_s_acctbal = ( float*) malloc_memory_mapped_file ( (sizeof ( float) * 10000), "mmdb/supplier_s_acctbal" );
std::string s_comment;
size_t supplier_s_comment_len = 0;
size_t* supplier_s_comment_offset;
supplier_s_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 10001), "mmdb/supplier_s_comment_offset" );
supplier_s_comment_offset[0] = 0;
char* nothing_supplier;
int s = 0;
while(reader10.read_row(s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment,nothing_supplier)) {
if((s > 10000)) {
std::cout << "error";
}
supplier_s_suppkey[s] = s_suppkey;
supplier_s_nationkey[s] = s_nationkey;
supplier_s_acctbal[s] = s_acctbal;
supplier_s_name_len += s_name.length();
supplier_s_name_offset[(s + 1)] = supplier_s_name_len;
supplier_s_address_len += s_address.length();
supplier_s_address_offset[(s + 1)] = supplier_s_address_len;
supplier_s_phone_len += s_phone.length();
supplier_s_phone_offset[(s + 1)] = supplier_s_phone_len;
supplier_s_comment_len += s_comment.length();
supplier_s_comment_offset[(s + 1)] = supplier_s_comment_len;
s++;
}
char* supplier_s_name_char;
supplier_s_name_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * supplier_s_name_len), "mmdb/supplier_s_name_char" );
char* supplier_s_address_char;
supplier_s_address_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * supplier_s_address_len), "mmdb/supplier_s_address_char" );
char* supplier_s_phone_char;
supplier_s_phone_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * supplier_s_phone_len), "mmdb/supplier_s_phone_char" );
char* supplier_s_comment_char;
supplier_s_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * supplier_s_comment_len), "mmdb/supplier_s_comment_char" );
io::CSVReader <8, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader11("/home/jigao/Desktop/tpch-dbgen//supplier.tbl");
int t = 0;
while(reader11.read_row(s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment,nothing_supplier)) {
strcpy ( &(supplier_s_name_char[supplier_s_name_offset[t]]), s_name.c_str());
strcpy ( &(supplier_s_address_char[supplier_s_address_offset[t]]), s_address.c_str());
strcpy ( &(supplier_s_phone_char[supplier_s_phone_offset[t]]), s_phone.c_str());
strcpy ( &(supplier_s_comment_char[supplier_s_comment_offset[t]]), s_comment.c_str());
t++;
}

io::CSVReader <5, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader12("/home/jigao/Desktop/tpch-dbgen//nation.tbl");
int n_nationkey;
int* nation_n_nationkey;
nation_n_nationkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 25), "mmdb/nation_n_nationkey" );
std::string n_name;
size_t nation_n_name_len = 0;
size_t* nation_n_name_offset;
nation_n_name_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 26), "mmdb/nation_n_name_offset" );
nation_n_name_offset[0] = 0;
int n_regionkey;
int* nation_n_regionkey;
nation_n_regionkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 25), "mmdb/nation_n_regionkey" );
std::string n_comment;
size_t nation_n_comment_len = 0;
size_t* nation_n_comment_offset;
nation_n_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 26), "mmdb/nation_n_comment_offset" );
nation_n_comment_offset[0] = 0;
char* nothing_nation;
int u = 0;
while(reader12.read_row(n_nationkey,n_name,n_regionkey,n_comment,nothing_nation)) {
if((u > 25)) {
std::cout << "error";
}
nation_n_nationkey[u] = n_nationkey;
nation_n_regionkey[u] = n_regionkey;
nation_n_name_len += n_name.length();
nation_n_name_offset[(u + 1)] = nation_n_name_len;
nation_n_comment_len += n_comment.length();
nation_n_comment_offset[(u + 1)] = nation_n_comment_len;
u++;
}
char* nation_n_name_char;
nation_n_name_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * nation_n_name_len), "mmdb/nation_n_name_char" );
char* nation_n_comment_char;
nation_n_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * nation_n_comment_len), "mmdb/nation_n_comment_char" );
io::CSVReader <5, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader13("/home/jigao/Desktop/tpch-dbgen//nation.tbl");
int v = 0;
while(reader13.read_row(n_nationkey,n_name,n_regionkey,n_comment,nothing_nation)) {
strcpy ( &(nation_n_name_char[nation_n_name_offset[v]]), n_name.c_str());
strcpy ( &(nation_n_comment_char[nation_n_comment_offset[v]]), n_comment.c_str());
v++;
}

io::CSVReader <4, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader14("/home/jigao/Desktop/tpch-dbgen//region.tbl");
int r_regionkey;
int* region_r_regionkey;
region_r_regionkey = ( int*) malloc_memory_mapped_file ( (sizeof ( int) * 5), "mmdb/region_r_regionkey" );
std::string r_name;
size_t region_r_name_len = 0;
size_t* region_r_name_offset;
region_r_name_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 6), "mmdb/region_r_name_offset" );
region_r_name_offset[0] = 0;
std::string r_comment;
size_t region_r_comment_len = 0;
size_t* region_r_comment_offset;
region_r_comment_offset = ( size_t*) malloc_memory_mapped_file ( (sizeof ( size_t) * 6), "mmdb/region_r_comment_offset" );
region_r_comment_offset[0] = 0;
char* nothing_region;
int w = 0;
while(reader14.read_row(r_regionkey,r_name,r_comment,nothing_region)) {
if((w > 5)) {
std::cout << "error";
}
region_r_regionkey[w] = r_regionkey;
region_r_name_len += r_name.length();
region_r_name_offset[(w + 1)] = region_r_name_len;
region_r_comment_len += r_comment.length();
region_r_comment_offset[(w + 1)] = region_r_comment_len;
w++;
}
char* region_r_name_char;
region_r_name_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * region_r_name_len), "mmdb/region_r_name_char" );
char* region_r_comment_char;
region_r_comment_char = ( char*) malloc_memory_mapped_file ( (sizeof ( char) * region_r_comment_len), "mmdb/region_r_comment_char" );
io::CSVReader <4, io::trim_chars<' '>, io::no_quote_escape<'|'> > reader15("/home/jigao/Desktop/tpch-dbgen//region.tbl");
int x = 0;
while(reader15.read_row(r_regionkey,r_name,r_comment,nothing_region)) {
strcpy ( &(region_r_name_char[region_r_name_offset[x]]), r_name.c_str());
strcpy ( &(region_r_comment_char[region_r_comment_offset[x]]), r_comment.c_str());
x++;
}



std::clock_t start_totalKernelTime0 = std::clock();
std::clock_t stop_totalKernelTime0 = std::clock();
printf("<timing>\n");
printf("</timing>\n");
}
