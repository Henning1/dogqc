
def cleanAttName ( name ):
    res = name.replace('_','')
    return res[0:8]

def attVarName ( a ):
    return "att" + str(a.id) + "_" + cleanAttName ( a.name )

def iatt ( a ):
    return "i" + attVarName ( a ) 

def oatt ( a ):
    return "o" + attVarName ( a ) 

def itm_att ( a ):
    return "itm_" + attVarName ( a ) 

def ratt ( a ):
    return "r" + attVarName ( a ) 

def att ( a ):
    return attVarName ( a ) 

def scanKernel ( algExpr ):
    kernelName = "krnl_" + str ( algExpr.table["name"] ) 
    if algExpr.scanTableId > 1:
        kernelName += str ( algExpr.scanTableId )
    return kernelName


def numOutVar( table ):
    return "numOutput_" + table["name"]

def size( relationName ):
    return relationName + "_size"

def tid( table ):
    return table["name"] + "_tid"

def column( attribute ):
    return attribute.table["name"] + "_" + attribute.name

def charLenVar ( attribute ):
    return attribute.table["name"] + "_" + attribute.name + "_len"

def nothingVar ( tableName ):
    return "nothing_" + tableName 

def numProbesVar ( tableName ):
    return "numProbes_" + tableName 

def resultColumn( attribute ):
    return "result_" + column( attribute )

def ht( table ):
    return "hashtable_" + table["name"]

def probeHash ( buildtable ):
    return "probeHash_" + buildtable["name"] 

def buildHash ( buildtable ):
    return "buildHash_" + buildtable["name"] 

def htPayload( table ):
    return "payload_" + table["name"]

def payloadVar( table ):
    return "payl_" + table["name"]

def htRangeOffset( attribute ):
    return "hashtable_" + column(attribute) + "_rangeoffset"

def divergenceBuffer( var ):
    return var.get() + "_dvgnce_buf"

def materializationBuffer( var, id ):
    return var.get() + "_mbuf" + str(id)

def materializationBufferCount( id ):
    return "count_mbuf" + str(id)

def registerBuffer( var ):
    return var.get() + "_reg_buf"

def tableKernelName( table ):
    return table["name"] + "Krnl"

def registerShuffleBuffer( var ):
    return var.get() + "_reg_shuf"

def gpuColumn( attribute ):
    if(attribute.table["name"] == "ITMDTE"):
        return attribute.name
    else:
        return "d_" + column( attribute )   

def gpuResultColumn( attribute ):
    if(attribute.table["name"] == "ITMDTE"):
        return attribute.name
    else:
        return "d_" + resultColumn( attribute )   

def gpuHt( attribute ):
    return "d_" + ht( attribute )


