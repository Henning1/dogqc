import sys
sys.path.insert(0,'..')

from collections import OrderedDict
from dogqc.types import Type 

tpchSchema = {}

tpchSchema["dateformat"] = ("int toDate ( const char* c ) {"
                            "    int d=0;"
                            "    d += (int)( c[0] - 48 ) * 10000000;"
                            "    d += (int)( c[1] - 48 ) *  1000000;"
                            "    d += (int)( c[2] - 48 ) *   100000;"
                            "    d += (int)( c[3] - 48 ) *    10000;"
                            "    d += (int)( c[5] - 48 ) *     1000;"
                            "    d += (int)( c[6] - 48 ) *      100;"
                            "    d += (int)( c[8] - 48 ) *       10;"
                            "    d += (int)( c[9] - 48 ) *        1;"
                            "    return d;"
                            "}")


tpchSchema["lineitem"] = { "name":"lineitem", "size":0, "numColumns":16, "attributes": 
                             OrderedDict([
                                 ("l_orderkey",Type.INT),
                                 ("l_partkey",Type.INT),
                                 ("l_suppkey",Type.INT),
                                 ("l_linenumber",Type.INT),
                                 ("l_quantity",Type.INT),
                                 ("l_extendedprice",Type.FLOAT),
                                 ("l_discount",Type.FLOAT),
                                 ("l_tax",Type.FLOAT),
                                 ("l_returnflag",Type.CHAR),
                                 ("l_linestatus",Type.CHAR),
                                 ("l_shipdate",Type.DATE),
                                 ("l_commitdate",Type.DATE),
                                 ("l_receiptdate",Type.DATE),
                                 ("l_shipinstruct",Type.STRING),
                                 ("l_shipmode",Type.STRING),
                                 ("l_comment",Type.STRING)
                             ])
                         }

tpchSchema["customer"] = { "name":"customer", "size":0, "numColumns":8, "attributes": 
                             OrderedDict([
                                 ("c_custkey",Type.INT),
                                 ("c_name",Type.STRING),
                                 ("c_address",Type.STRING),
                                 ("c_nationkey",Type.INT),
                                 ("c_phone",Type.STRING),
                                 ("c_acctbal",Type.FLOAT),
                                 ("c_mktsegment",Type.STRING),
                                 ("c_comment",Type.STRING)
                             ])
                         }

tpchSchema["orders"] = { "name":"orders", "size":0, "numColumns":9, "attributes": 
                           OrderedDict([
                               ("o_orderkey",Type.INT),
                               ("o_custkey",Type.INT),
                               ("o_orderstatus",Type.CHAR),
                               ("o_totalprice",Type.FLOAT),
                               ("o_orderdate",Type.DATE),
                               ("o_orderpriority",Type.STRING),
                               ("o_clerk",Type.STRING),
                               ("o_shippriority",Type.INT),
                               ("o_comment",Type.STRING)
                           ])
                       }

tpchSchema["partsupp"] = { "name":"partsupp", "size":0, "numColumns":5, "attributes": 
                             OrderedDict([
                                 ("ps_partkey",Type.INT),
                                 ("ps_suppkey",Type.INT),
                                 ("ps_availqty",Type.INT),
                                 ("ps_supplycost",Type.FLOAT),
                                 ("ps_comment",Type.STRING)
                             ])
                         }

tpchSchema["part"] = { "name":"part", "size":0, "numColumns":9, "attributes": 
                         OrderedDict([
                             ("p_partkey",Type.INT),
                             ("p_name",Type.STRING),
                             ("p_mfgr",Type.STRING),
                             ("p_brand",Type.STRING),
                             ("p_type",Type.STRING),
                             ("p_size",Type.INT),
                             ("p_container",Type.STRING),
                             ("p_retailprice",Type.FLOAT),
                             ("p_comment",Type.STRING)
                         ])
                     }

tpchSchema["supplier"] = { "name":"supplier", "size":0, "numColumns":7, "attributes": 
                             OrderedDict([
                                 ("s_suppkey",Type.INT),
                                 ("s_name",Type.STRING),
                                 ("s_address",Type.STRING),
                                 ("s_nationkey",Type.INT),
                                 ("s_phone",Type.STRING),
                                 ("s_acctbal",Type.FLOAT),
                                 ("s_comment",Type.STRING)
                             ])
                         }

tpchSchema["nation"] = { "name":"nation", "size":0, "numColumns":4, "attributes": 
                           OrderedDict([
                               ("n_nationkey",Type.INT),
                               ("n_name",Type.STRING),
                               ("n_regionkey",Type.INT),
                               ("n_comment",Type.STRING)
                           ])
                       }

tpchSchema["region"] = { "name":"region", "size":0, "numColumns":3, "attributes": 
                           OrderedDict([
                               ("r_regionkey",Type.INT),
                               ("r_name",Type.STRING),
                               ("r_comment",Type.STRING)
                           ])
                       }
