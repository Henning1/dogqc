import sys
sys.path.insert(0,'..')

from collections import OrderedDict
from dogqc.types import Type 


ssbSchema = {}

ssbSchema["dateformat"] = ("int toDate ( const char* c ) {"
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


ssbSchema["lineorder"] = { "name":"lineorder", "size":0, "numColumns":17, "attributes": 
                             OrderedDict([
                                 ("lo_orderkey",Type.INT),
                                 ("lo_linenumber",Type.INT),
                                 ("lo_custkey",Type.INT),
                                 ("lo_partkey",Type.INT),
                                 ("lo_suppkey",Type.INT),
                                 ("lo_orderdate",Type.INT),
                                 ("lo_ordpriority",Type.STRING),
                                 ("lo_shippriority",Type.CHAR),
                                 ("lo_quantity",Type.INT),
                                 ("lo_extendedprice",Type.INT),
                                 ("lo_ordtotalprice",Type.INT),
                                 ("lo_discount",Type.INT),
                                 ("lo_revenue",Type.INT),
                                 ("lo_supplycost",Type.INT),
                                 ("lo_tax",Type.INT),
                                 ("lo_commitdate",Type.INT),
                                 ("lo_shipmode",Type.STRING)
                             ])
                         }

ssbSchema["date"] = { "name":"date", "size":0, "numColumns":17, "attributes": 
                        OrderedDict([
                            ("d_datekey",Type.INT),
                            ("d_date",Type.STRING),
                            ("d_dayofweek",Type.STRING),
                            ("d_month",Type.STRING),
                            ("d_year",Type.INT),
                            ("d_yearmonthnum",Type.INT),
                            ("d_yearmonth",Type.STRING),
                            ("d_daynuminweek",Type.INT),
                            ("d_daynuminmonth",Type.INT),
                            ("d_daynuminyear",Type.INT),
                            ("d_monthnuminyear",Type.INT),
                            ("d_weeknuminyear",Type.INT),
                            ("d_sellingseason",Type.STRING),
                            ("d_lastdayinweekfl",Type.INT),
                            ("d_lastdayinmonthfl",Type.INT),
                            ("d_holidayfl",Type.INT),
                            ("d_weekdayfl",Type.INT)
                        ])
                    }

ssbSchema["customer"] = { "name":"customer", "size":0, "numColumns":8, "attributes": 
                            OrderedDict([
                                ("c_custkey",Type.INT),
                                ("c_name",Type.STRING),
                                ("c_address",Type.STRING),
                                ("c_city",Type.STRING),
                                ("c_nation",Type.STRING),
                                ("c_region",Type.STRING),
                                ("c_phone",Type.STRING),
                                ("c_mktsegment",Type.STRING)
                            ])
                        }


ssbSchema["part"] = { "name":"part", "size":0, "numColumns":9, "attributes": 
                        OrderedDict([
                            ("p_partkey",Type.INT),
                            ("p_name",Type.STRING),
                            ("p_mfgr",Type.STRING),
                            ("p_category",Type.STRING),
                            ("p_brand1",Type.STRING),
                            ("p_color",Type.STRING),
                            ("p_type",Type.STRING),
                            ("p_size",Type.INT),
                            ("p_container",Type.STRING)
                        ])
                    }

ssbSchema["supplier"] = { "name":"supplier", "size":0, "numColumns":7, "attributes": 
                            OrderedDict([
                                    ("s_suppkey",Type.INT),
                                    ("s_name",Type.STRING),
                                    ("s_address",Type.STRING),
                                    ("s_city",Type.STRING),
                                    ("s_nation",Type.STRING),
                                    ("s_region",Type.STRING),
                                    ("s_phone",Type.STRING)
                            ])
                        }
