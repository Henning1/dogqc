from dogqc.code import Code

# includes
def getIncludes ():
    code = Code()
    code.add("#include <list>")
    code.add("#include <unordered_map>")
    code.add("#include <vector>")
    code.add("#include <iostream>")
    code.add("#include <ctime>")
    code.add("#include <limits.h>")
    code.add("#include <float.h>")
    code.add("#include \"../dogqc/include/csv.h\"")
    code.add("#include \"../dogqc/include/util.h\"")
    code.add("#include \"../dogqc/include/mappedmalloc.h\"")
    return code

def getCudaIncludes ():
    code = Code()
    code.add("#include \"../dogqc/include/util.cuh\"")
    code.add("#include \"../dogqc/include/hashing.cuh\"")
    return code


class Type ( object ):
    MULTI_HT = "multi_ht"
    UNIQUE_HT = "unique_ht"
    AGG_HT = "agg_ht"

class Const ( object ):
    ALL_LANES = "ALL_LANES"


class Krnl ( object ):
    INIT_AGG_HT = "initAggHT"
    INIT_ARRAY = "initArray"
    INIT_UNIQUE_HT = "initUniqueHT"
    INIT_MULTI_HT = "initMultiHT"

# functions
class Fct ( object ):
    HASH_BUILD_UNIQUE = "hashBuildUnique"
    HASH_PROBE_UNIQUE = "hashProbeUnique"
    HASH_COUNT_MULTI = "hashCountMulti"
    HASH_INSERT_MULTI = "hashInsertMulti"
    HASH_PROBE_MULTI = "hashProbeMulti"
    HASH = "hash"
    HASH_AGG_BUCKET = "hashAggregateGetBucket"
    HASH_AGG_CHECK = "hashAggregateFindBucket"




