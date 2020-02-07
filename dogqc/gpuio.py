from dogqc.cudalang import *
from dogqc.code import Code
from dogqc.kernel import KernelCall

class GpuIO ( object ):

    def __init__( self ):
        self.cudaMalloc = Code ()
        self.cudaMallocHT = Code ()
        self.cudaMemcpyIn = Code ()
        self.cudaMemcpyOut = Code ()
        self.cudaFree = Code ()

    def local ( self, var, init=None ):
        self.declareAllocateHT ( var )
        if init is not None:
            self.initVar ( var, init )
    
    def mapForRead ( self, var, blocked=False ):
        self.declareAllocate ( var )
        self.cudaMemcpyIn.add ( getCudaMemcpyIn ( var, var.numElements ) )
    
    def mapForReadLit ( self, deviceVar, hostVar, blocked=False ):
        self.declareAllocateLit ( deviceVar )
        self.cudaMemcpyIn.add ( getCudaMemcpyInLit ( deviceVar, hostVar, hostVar.numElements ) )
    
    def mapForWrite ( self, var, sizevar=None ):
        self.declareAllocate ( var )
        if sizevar == None:
            self.cudaMemcpyOut.add ( getCudaMemcpyOut ( var, var.numElements ) )
        else:
            self.cudaMemcpyOut.add ( getCudaMemcpyOut ( var, sizevar ) )

    def copyOut ( self, var ):
        self.declareAllocateDevice ( var )
        self.cudaMemcpyIn.add ( getCudaMemcpyIn ( var, var.numElements ) )
    
    def initVar ( self, var, init ):
        call = KernelCall.library ( "initArray", [var.getGPU(), str(init), var.numElements], var.dataType )
        self.cudaMallocHT.add ( call.get() )   
    
    def declare ( self, var, blocked=False ):
        self.cudaMalloc.add ( var.declareGPU() )

    def declareAllocate ( self, var, blocked=False ):
        self.cudaMalloc.add ( var.declareGPU() )
        self.cudaMalloc.add ( getCudaMalloc ( var, var.numElements ) )
        self.cudaFree.add ( getCudaFree ( var ) )
    
    def declareAllocateHT ( self, var ):
        self.cudaMallocHT.add ( var.declareGPU() )
        self.cudaMallocHT.add ( getCudaMalloc ( var, var.numElements ) )
        self.cudaFree.add ( getCudaFree ( var ) )
    
    def declareAllocateLit ( self, var, blocked=False ):
        var.declarePointer ( self.cudaMalloc )
        self.cudaMalloc.add ( getCudaMallocLit ( var, var.numElements ) )
        self.cudaFree.add ( getCudaFreeLit ( var ) )
