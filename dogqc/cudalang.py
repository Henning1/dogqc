
class CType( object ):
    INT = "int"
    UINT = "unsigned"
    UINT64 = "uint64_t"
    FP32 = "float"
    FP64 = "double"
    CHAR = "char"
    SIZE = "size_t"
    ULL = "unsigned long long"
    FILE = "FILE"
    STR_OFFS = "str_offs"
    STR_TYPE = "str_t"
    
    zeroValue = {
        INT:  "0",
        UINT: "0",
        ULL:  "0",
        FP32: "0.0f",
        FP64: "0.0",
        CHAR: "0"
    }
    
    maxValue = {
        INT:  "INT_MAX",
        UINT: "UINT_MAX",
        ULL:  "ULONG_MAX",
        FP32: "FLT_MAX",
        FP64: "DBL_MAX",
        CHAR: "CHAR_MAX"
    }
    
    minValue = {
        INT:  "INT_MIN",
        UINT: "0",
        ULL:  "0",
        FP32: "FLT_MIN",
        FP64: "DBL_MIN",
        CHAR: "CHAR_MIN"
    }

    printFormat = {
        INT:  "%8i",
        ULL:  "%10llu",
        UINT: "%10i",
        FP32: "%15.2f",
        FP64: "%15.2f",
        CHAR: "%c"
    }
    
    size = {
        INT: 4,
        UINT: 4,
        ULL: 8,
        FP32: 4,
        FP64: 8,
        CHAR: 1
    }

class CountingWhileLoop ( object ):

    indices = ["i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","ii","jj","kk","ll","mm","oo","pp"]
    i = 0
 
    def __init__( self, condition, code ): 
        self.code = code
        self.countVar = CountingWhileLoop.indices[CountingWhileLoop.i % 25]
        code.add ( "int " + self.countVar + " = 0;" )
        code.add ( "while(" + condition + ") {")
        CountingWhileLoop.i += 1

    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.code.add ( self.countVar + "++;" )
        self.code.add ( "}" )

class Scope ( object ):

    def __init__( self, code ): 
        self.code = code
        code.add ("{")

    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.close()
    
    def close ( self ):
        self.code.add ( "}" )

class StructClause ( object ):

    def __init__( self, name, code ): 
        self.code = code
        code.add ("struct " + name + " {")

    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.close()
    
    def close ( self ):
        self.code.add ( "};" )

class WhileLoop ( object ):

    def __init__( self, condition, code ): 
        self.code = code
        code.add ( "while(" + str ( condition ) + ") {")

    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.close()
    
    def break_ ( self ):
        self.code.add ( "break;" )
    
    def close ( self ):
        self.code.add ( "}" )


class ForLoop ( object ):

    def __init__( self, init, condition, increment, code ): 
        self.code = code
        code.add ("for ( " + str(init) + "; " + str(condition) + "; " + str(increment) + ") {")

    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.code.add ( "}" )
        
    def break_ ( self ):
        self.code.add ( "break;" )
    
    def close ( self ):
        self.code.add ( "}" )


class UnrolledForLoop ( object ):

    def __init__( self, depth, code ): 
        self.code = code
        self.depth = depth
        if depth > 1:
            code.add ("#pragma unroll")
            code.add ("for ( int u = 0; u < " + str(depth) + "; u++) {")

    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        if self.depth > 1:
            self.code.add ( "}" )

 
class IfClause ( object ):
 
    def __init__( self, condition, code ): 
        self.code = code
        code.add ( "if(" + str(condition) + ") {")
    
    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.close()

    def close ( self ):
        self.code.add ( "}" )

class ElseIfClause ( object ):
 
    def __init__( self, code ): 
        self.code = code
        code.add ( "else if {")
    
    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.close()

    def close ( self ):
        self.code.add ( "}" )


class ElseClause ( object ):
 
    def __init__( self, code ): 
        self.code = code
        code.add ( "else {")
    
    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.close()

    def close ( self ):
        self.code.add ( "}" )

def ptr ( type ):
    return type + "*"

def cast ( typename, expr ):
    return "((" + str(typename) + ")" + str(expr) + ")"

def declare ( variable ):
    return variable.dataType + " " + variable.name

def declareSharedArray ( variable, length ):
    return "__shared__ " + variable.dataType + " " + variable.name + "[" + length + "]"

def declareShared ( variable ):
    return "__shared__ " + variable.dataType + " " + variable.name

def declarePointer ( variable ):
    return ptr ( variable.dataType ) + " " + variable.name

def pointerMember ( pointerExpr, memberExpr ):
    return str(pointerExpr) + "->" + str(memberExpr)

def member ( expr, memberExpr ):
    return str(expr) + "." + str(memberExpr)

def deref ( expr ):
    return "*" + "(" + str(expr) + ")"

def intConst ( i ):
    return str( i )

def strConst ( s ):
    return "\"" + s + "\""

def charConst ( c ):
    return "'" + c + "'"

def bitmask32f ( ):
    return "0xffffffff"

def bitmask64f ( ):
    return "0xffffffffffffffff"

def assign ( left, right ):
    return str(left) + " = " + str(right)

def assignAnd ( left, right ):
    return str(left) + " &= (" + str(right) + ")"

def assignAndLogic ( left, right ):
    return str(left) + " &&= (" + str(right) + ")"

def assignAdd ( left, right ):
    return str(left) + " += " + str(right)

def assignMul ( left, right ):
    return str(left) + " *= " + str(right)

def assignXor ( left, right ):
    return str(left) + " ^= " + str(right)

def assignMod ( left, right ):
    return str(left) + " %= " + str(right)

def inlineIf ( condition, ifcase, elcase):
    return "(" + str(condition) + ") ? (" + str(ifcase) + ") : (" + str(elcase) + ")"

def assignSub ( left, right ):
    return str(left) + " -= " + str(right)

def minMath ( left, right ):
    return "min(" + str(left) + ", " + str(right) + ")"  

def maxMath ( left, right ):
    return "max(" + str(left) + ", " + str(right) + ")"  

def sub ( left, right ):
    return "(" + str(left) + " - " + str(right) + ")"

def add ( left, right ):
    return "(" + str(left) + " + " + str(right) + ")"

def mul ( left, right ):
    return "(" + str(left) + " * " + str(right) + ")"

def div ( left, right ):
    return "(" + str(left) + " / " + str(right) + ")"

def abs ( child ):
    return "fabs( (float)" + str(child) + ")"

def increment ( expr ):
    return "(" + str( expr ) + "++)"

def decrement ( expr ):
    return "(" + str( expr ) + "--)"

def shiftLeft ( left, right ):
    return "(" + str(left) + " << " + str(right) + ")"

def shiftRight ( left, right ):
    return "(" + str(left) + " >> " + str(right) + ")"

def larger ( left, right ):
    return "(" + str(left) + " > " + str(right) + ")"

def largerEqual ( left, right ):
    return "(" + str(left) + " >= " + str(right) + ")"

def smaller ( left, right ):
    return "(" + str(left) + " < " + str(right) + ")"

def smallerEqual ( left, right ):
    return "(" + str(left) + " <= " + str(right) + ")"

def andBitwise ( left, right ):
    return "(" + str(left) + " & " + str(right) + ")"

def orBitwise ( left, right ):
    return "(" + str(left) + " | " + str(right) + ")"

def andLogic ( left, right ):
    return "(" + str(left) + " && " + str(right) + ")"

def orLogic ( left, right ):
    return "(" + str(left) + " || " + str(right) + ")"

def notLogic ( expr ):
    return "!(" + str(expr) + ")"

def inverse ( expr ):
    return "~(" + str(expr) + ")"

def modulo ( left, right ):
    return "(" + str(left) + " % " + str(right) + ")"

def equals ( left, right ):
    return "(" + str(left) + " == " + str(right) + ")"

def notEquals ( left, right ):
    return "(" + str(left) + " != " + str(right) + ")"

def emit ( line, code ):
    code.add ( line + ";" )

def strcpy ( dest, source ):
    return "strcpy ( " + dest + ", " + source + ")"

def sizeof ( expr ):
    return "sizeof ( " + str(expr) + ")"

def addressof ( expr ):
    return "&(" + str(expr) + ")"

def printError( code ):
    code.add ( "std::cout << \"error\";" )

def mmapMalloc( typename, num, filename):
    return "( " + str(typename) + "*) malloc_memory_mapped_file ( " + mul ( sizeof ( typename ), num) + ", " + "\"" + str(filename) + "\"" + " )"

def mmapFile ( typename, filename ):
    return "( " + str(typename) + "*) map_memory_file ( " + "\"" + str(filename) + "\"" + " )"  

def activemask ( ):
    return "__activemask()"

def ballotIntr ( mask, expr ):
    return "__ballot_sync(" + str(mask) + "," + str(expr) + ")"

def shuffleIntr ( mask, var, srcLane ):
    return "__shfl_sync(" + str(mask) + "," + str(var) + "," + str(srcLane) + ")"

def anyIntr ( mask, expr ):
    return "__any_sync(" + str(mask) + "," + str(expr) + ")"

def clzIntr ( expr ):
    return "__clz(" + str(expr) + ")"

def ffsIntr ( expr ):
    return "__ffs(" + str(expr) + ")"

def printf ( string, params=[] ):
    if params != []:
        params = map(str, params)
        return "printf(" + "\"" + string + "\"" + ", " + ",".join(params) + ")"
    else:
        return "printf(" + "\"" + string + "\")"

def fopen ( filename, mode ):
    return "fopen(" + "\"" + filename + "\"" + ", " + "\"" +  mode + "\"" + ")"
    
def fprintf ( outFile, string, params=[] ):
    if params != []:
        params = map(str, params)
        return "fprintf(" + str(outFile) + ", " + "\"" + string + "\"" + ", " + ",".join(params) + ")"
    else:
        return "fprintf(" + str(outFile) + ", " + "\"" + string + "\"" + ")"

def popcount ( expr ):
    return "__popc(" + str(expr) + ")"

def threadIdx_x ( ):
    return "threadIdx.x"

def blockDim_x ( ):
    return "blockDim.x"

def blockIdx_x ( ):
    return "blockIdx.x"

def gridDim_x ( ):
    return "gridDim.x"

def syncthreads ( ):
    return "__syncthreads()"

def syncwarp ( ):
    return "__syncwarp()"

def atomicAdd ( address, value ):
    return "atomicAdd(" + str(address) + ", " + str(value) + ")"

def atomicMin ( address, value ):
    return "atomicMin(" + str(address) + ", " + str(value) + ")"

def atomicMax ( address, value ):
    return "atomicMax(" + str(address) + ", " + str(value) + ")"

def cout ( ):
    return "std::cout"

def breakLoop ( ):
    return "break"

def comment ( comment, code ):
    return code.add("// " + comment )

def deviceSynchronize ( ):
    return "cudaDeviceSynchronize()"

def cudaErrorCheck ( msg, code ):
    with Scope ( code ):
        emit ( "cudaError err = cudaGetLastError()", code )
        with IfClause ( "err != cudaSuccess", code ):
            code.add("std::cerr << \"Cuda Error in " + msg + "! \" << cudaGetErrorString( err ) << std::endl;")
            ERROR ( msg, code )
 

def getCudaMalloc ( dualVariable, num ):
    return "cudaMalloc((void**) &" + dualVariable.getGPU() + ", " + str(num) + "* sizeof(" + dualVariable.dataType + ") );"

def getCudaMallocLit ( dualVariable, num ):
    return "cudaMalloc((void**) &" + dualVariable.get() + ", " + str(num) + "* sizeof(" + dualVariable.dataType + ") );"

def getCudaMemcpyIn ( dualVariable, num ):
    return "cudaMemcpy( " + dualVariable.getGPU() + ", " + dualVariable.getPointer() + ", " + str(num) + " * sizeof(" + dualVariable.dataType + ") , cudaMemcpyHostToDevice);"

def getCudaMemcpyInLit ( gpuVar, hostVar, num ):
    return "cudaMemcpy( " + gpuVar.get() + ", " + hostVar.get() + ", " + str(num) + " * sizeof(" + gpuVar.dataType + ") , cudaMemcpyHostToDevice);"

def getCudaMemcpyOut ( dualVariable, num ):
    return "cudaMemcpy( " + dualVariable.getPointer() + ", " + dualVariable.getGPU() + ", " + str(num) + " * sizeof(" + dualVariable.dataType + ") , cudaMemcpyDeviceToHost);"

def getCudaFree ( dualVariable ):
    return "cudaFree( " + dualVariable.getGPU() + ");" 

def getCudaFreeLit ( dualVariable ):
    return "cudaFree( " + dualVariable.get() + ");" 
            
def getCudaMemset ( dualVariable, num ):
    return "cudaMemset( " + dualVariable.getGPU() + ", 0, sizeof (" + dualVariable.dataType + ") );"

def ERROR ( msg, code ):
    return code.add("ERROR(\"" + str(msg) + "\")")

def commentOperator ( comment, opId, code ):
    return code.add("// -------- " + comment + " (opId: " + str(opId) + ") --------" )
                
def stringEquals ( a, b ):
    return call ( "stringEquals", [ a, b ] )

def stringLike ( s, like ):
    return call ( "stringLikeCheck", [ s, like ] )

def stringSubstring ( str, frm, fr ):
    return call ( "stringSubstring", [ str, frm, fr ] )

def call ( name, params):
    comma = False
    callStr = name + " ( "
    for a in params:
        if not comma:
            comma = True
        else:
            callStr += ", " 
        callStr += str(a)
    callStr = callStr + ")"
    return callStr

def printMemoryFootprint ( code ):
    memprofile = """
    // show memory usage of GPU
    {size_t free_byte ;
    size_t total_byte ;
    cudaError_t cuda_status = cudaMemGetInfo( &free_byte, &total_byte ) ;
    if ( cudaSuccess != cuda_status ){
        printf("Error: cudaMemGetInfo fails, %s \\n", cudaGetErrorString(cuda_status) );
        exit(1);
    }
    double free_db = (double)free_byte ;
    double total_db = (double)total_byte ;
    double used_db = total_db - free_db ;
    fprintf(stderr, "Memory %.1f / %.1f GB\\n",
        used_db/(1024*1024*1024), total_db/(1024*1024*1024) );
    fflush(stdout);}
    """
    code.add ( memprofile )

