import subprocess
import os
import sys
import copy
import time

import dogqc.identifier as ident
import dogqc.querylib as qlib
from dogqc.cudalang import *
from dogqc.variable import Variable
from dogqc.code import Code
from dogqc.code import Timestamp
from dogqc.gpuio import GpuIO
from dogqc.kernel import Kernel, KernelCall
from dogqc.types import Type
from dogqc.cudalang import CType


class CodeGenerator ( object ):

    def __init__( self, decimalRepresentation ):
        self.read = Code()
        self.types = Code()
        self.kernels = []
        self.currentKernel = None
        self.kernelCalls = []

        self.declare = Code()
        self.finish = Code()
        self.mirrorKernel = None

        self.gpumem = GpuIO ( )
        self.constCounter = 0

        self.decimalType = decimalRepresentation
   
    def langType ( self, relDataType ):
        internalTypeMap = {}
        internalTypeMap [ Type.INT ] = CType.INT
        internalTypeMap [ Type.DATE ] = CType.UINT
        internalTypeMap [ Type.CHAR ] = CType.CHAR
        internalTypeMap [ Type.FLOAT ] = self.decimalType
        internalTypeMap [ Type.DOUBLE ] = self.decimalType
        internalTypeMap [ Type.STRING ] = CType.STR_TYPE
        return internalTypeMap [ relDataType ]

    def stringConstant ( self, token ):
        self.constCounter += 1
        c = Variable.val ( CType.STR_TYPE, "c" + str ( self.constCounter ) ) 
        emit ( assign ( declare ( c ), call ( "stringConstant", [ "\"" + token + "\"", len(token) ] ) ), self.init() )
        return c

    def openKernel ( self, kernel ):
        self.kernels.append ( kernel )
        self.currentKernel = kernel
        self.kernelCalls.append ( KernelCall.generated ( kernel ) )
        return kernel
    
    # used for multiple passes e.g. (multi) hash build
    def openMirrorKernel ( self, suffix ):
        kernel = copy.deepcopy ( self.currentKernel )
        kernel.kernelName = self.currentKernel.kernelName + suffix
        self.kernels.append ( kernel )
        self.mirrorKernel = kernel
        self.kernelCalls.append ( KernelCall.generated ( kernel ) )
        return kernel
    
    def closeKernel ( self ):
        self.currentKernel = None

        if self.mirrorKernel:
            self.mirrorKernel = None

    def add ( self, string ):
        self.currentKernel.add ( string )
        if self.mirrorKernel:
            self.mirrorKernel.add ( string )
 
    def init ( self ):
        return self.currentKernel.init
        
    def warplane( self ):
        try:
            return self.currentKernel.warplane
        except AttributeError:
            self.currentKernel.warplane = Variable.val ( CType.UINT, "warplane" )
            emit ( assign ( declare ( self.currentKernel.warplane ), modulo ( threadIdx_x(), intConst(32) ) ), self.init() )
            return self.currentKernel.warplane

    def warpid( self ):
        try:
            return self.currentKernel.warpid
        except AttributeError:
            self.currentKernel.warpid = Variable.val ( CType.UINT, "warpid" )
            emit ( assign ( declare ( self.currentKernel.warpid ), div ( threadIdx_x(), intConst(32) ) ), self.init() )
            return self.currentKernel.warpid

    def newStatisticsCounter ( self, varname, text ):
        counter = Variable.val ( CType.UINT, varname ) 
        counter.declareAssign ( intConst(0), self.declare )
        self.gpumem.mapForWrite ( counter ) 
        self.gpumem.initVar ( counter, "0u" ) 
        self.currentKernel.addVar ( counter )
        emit ( printf ( "\\n"+text+": %i\\n", [ counter ]), self.finish )  
        return counter

    def prefixlanes( self ):
        try:
            return self.currentKernel.prefixlanes
        except AttributeError:
            self.currentKernel.prefixlanes = Variable.val ( CType.UINT, "prefixlanes" )
            emit ( assign ( declare ( self.currentKernel.prefixlanes ), 
                shiftRight ( bitmask32f(), sub ( intConst(32), self.warplane() ) ) ), self.init() )    
            return self.currentKernel.prefixlanes
    
    def addDatabaseAccess ( self, context, accessor ):
        self.read.add( accessor.getCodeAccessDatabase ( context.inputAttributes ) )
        self.accessor = accessor

    # build complete code file from generated pieces and add time measurements
    def composeCode( self, useCuda=True ):
        code = Code()
        code.add ( qlib.getIncludes () )
        if useCuda:
            code.add ( qlib.getCudaIncludes () )
        code.addFragment ( self.types )
        for k in self.kernels: 
            code.add(k.getKernelCode())
        code.add( "int main() {" )
        code.addTimedFragment ( self.read, "import" )
        code.addTimedFragment ( self.declare, "declare" )
        if self.gpumem.cudaMalloc.hasCode:
            wakeup = Code()
            comment ( "wake up gpu", wakeup ) 
            code.addCudaFragment ( wakeup, "wake up gpu" )
        code.addCudaFragment ( self.gpumem.cudaMalloc, "cuda malloc" )
        if useCuda:
            printMemoryFootprint ( code )
        code.addCudaFragment ( self.gpumem.cudaMallocHT, "cuda mallocHT" )
        if useCuda:
            printMemoryFootprint ( code )
        code.addCudaFragment ( self.gpumem.cudaMemcpyIn, "cuda memcpy in" )
        tsKernels = Timestamp ( "totalKernelTime", code )
        for call in self.kernelCalls: 
            msg = call.kernelName + " " + str(call.gridSize) + " " + str(call.blockSize)
            code.addCudaFragment ( call.get(), msg )
        tsKernels.stop()
        code.addCudaFragment ( self.gpumem.cudaMemcpyOut, "cuda memcpy out" )
        code.addCudaFragment ( self.gpumem.cudaFree, "cuda free" )
        code.addTimedFragment ( self.finish, "finish" )
        if useCuda:
            code.timestamps.append ( tsKernels )
        for ts in code.timestamps: 
            ts.printTime() 
        code.add("}")
        return code.content

    def writeCodeFile ( self, code, filename ):
        with open(filename, 'w') as f:
            f.write( code )
        
        # format sourcecode
        cmd = "astyle --indent-col1-comments " + filename
        subprocess.run(cmd, stdout=subprocess.DEVNULL, shell=True)
   

    def compile_( self, filename, arch="sm_52", debug=False ):
        print("compilation...")
        sys.stdout.flush()
        self.filename = filename
        cuFilename = filename + ".cu"

        self.writeCodeFile ( self.composeCode(), cuFilename )

        # compile
        nvccFlags = "-std=c++11 -arch=" + arch + " "
        hostFlags = "-pthread "
        if debug:
            nvccFlags += "-g -G "
            hostFlags += "-rdynamic "
        cmd = "nvcc " + cuFilename + " -o " + filename + " " + nvccFlags + " -Xcompiler=\"" + hostFlags + "\" "
        print(cmd)
        start = time.time()
        if debug:
            subprocess.run(cmd, shell=True)
        else:
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
        end = time.time()
        print ( "compilation time: %.1f ms" % ((end-start)*1000) )
        

    def compileCpu ( self, filename, debug=False ):
        self.filename = filename
        cppFilename = filename + ".cpp"

        self.writeCodeFile ( self.composeCode(False), cppFilename )

        # compile
        flags = "-std=c++11  -pthread "
        if debug:
            flags += " -g"
        cmd = "g++ " + cppFilename + " -o " + filename + " " + flags
        print(cmd)
        output = subprocess.check_output(cmd, shell=True)

    def execute( self ):  
        print("\nexecution...")
        sys.stdout.flush()
        cmd = "./" + self.filename
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        print(output)
        sys.stdout.flush()
        return (output)


