from dogqc.translatorBase import UnaryTranslator
from dogqc.codegen import CodeGenerator
from dogqc.cudalang import *
from dogqc.variable import Variable
from dogqc.kernel import Kernel, KernelCall
import dogqc.identifier as ident
import dogqc.querylib as qlib
from enum import Enum


class LaneRefill ( UnaryTranslator ):
 
    def __init__( self, parentAlgExpr, child ):
        UnaryTranslator.__init__ ( self, parentAlgExpr, child )
        self.threshold = 0.9
 
    def produce( self, ctxt ):
        self.child.produce( ctxt )

    def consume ( self, ctxt ):
        if ctxt.innerLoopCount > 0:
            commentOperator ("skipped divergence buffer in inner loop (depth " + str(ctxt.innerLoopCount) + ")", 1000000, ctxt.codegen)
            self.parent.consume ( ctxt )
            return
 
        ctxt.codegen.currentKernel.annotate ( "LR" + str(self.algExpr.opId) )
        vars = []
        for att in self.algExpr.outRelation:
            vars.append ( ctxt.attFile.regFile [ att ] )

        with BufferedLoop ( ctxt, vars, self.opId(), self.threshold ):
            self.parent.consume ( ctxt )
            emit ( assign ( ctxt.vars.activeVar, intConst(0) ), ctxt.codegen )
    
    def opId ( self ):
        return ( self.algExpr.opId + 100000 )
    
    def toDOT ( self, graph ):
        self.child.toDOT ( graph )
        graph.node ( str ( self.opId() ), "Lane Refill LR" + str(self.algExpr.opId), style='filled', color='#FFA494' )
        graph.edge ( str ( self.child.opId() ), str ( self.opId() ), self.algExpr.edgeDOTstr() )


class BufferType(Enum):
    SMEM = 1
    REG = 2


class BufferHelperSharedMemory ( object ):

    def __init__ ( self, ctxt, opId, buftype=BufferType.SMEM ):
        self.ctxt = ctxt
        self.buftype = buftype
        self.opId = opId
        

    def consumeDeclareBuffer ( self, bufferVars ):
        ctxt = self.ctxt
        codegen = ctxt.codegen

        self.bufferbase = Variable.val ( CType.INT, "bufferBase" + str(self.opId) + "_" )
        emit ( assign ( declare ( self.bufferbase ), mul ( codegen.warpid(), intConst(32) ) ), ctxt.codegen.init() )
        self.scan = Variable.val ( CType.INT, "scan" + str(self.opId) + "_" )
        self.scan.declare ( codegen )
        self.numRemaining = Variable.val ( CType.INT, "remaining" + str(self.opId) + "_" )
        self.numRemaining.declare ( ctxt.codegen )
       
        # remember variables that need to be buffered at this pipeline stage
        self.bufferVars = bufferVars.copy()

        if self.buftype is BufferType.SMEM:
            self.bufDeclareSmem ( bufferVars )
        if self.buftype is BufferType.REG:
            self.bufDeclareReg ( bufferVars )

    def bufDeclareSmem ( self, bufferVars ):
        ctxt = self.ctxt
        codegen = ctxt.codegen
        
        self.buf_ix = Variable.val ( CType.INT, "bufIdx" + str(self.opId) + "_" )
        self.buf_ix.declare ( ctxt.codegen )
        
        comment ( "shared memory variables for divergence buffers", codegen.init() ) 
        # initialize shared memory buffers and store in dict by variable name
        self.buffers = dict()
        for v in self.bufferVars:
            buf = Variable.val ( v.dataType, ident.divergenceBuffer ( v ) + "_" + str(self.opId) + "_" )
            buf.declareSharedArray ( intConst ( KernelCall.defaultBlockSize ), ctxt.codegen.init() ) 
            self.buffers[v.get()] = buf
    

    def bufDeclareReg ( self, bufferVars ):
        ctxt = self.ctxt
        
        comment ( "register variables for divergence buffers", ctxt.codegen ) 
        self.shuffleSourceLane = Variable.val ( CType.INT, "shuffleSourceLane" )     
        self.shuffleSourceLane.declareSharedArray ( intConst ( KernelCall.defaultBlockSize ), ctxt.codegen.init() ) 
        self.sourceLane = Variable.val ( CType.INT, "sourceLane" + str(self.opId) + "_" )     
        self.sourceLane.declare ( ctxt.codegen.init() ) 
        self.activeDest = Variable.val ( CType.INT, "activeDest" + str(self.opId) + "_" )     
        self.activeDest.declare ( ctxt.codegen.init() )
        self.activeSource = Variable.val ( CType.INT, "activeSource" + str(self.opId) + "_" )     
        self.activeSource.declare ( ctxt.codegen.init() )
        
        self.buffers = dict()
        self.shuffleBuffers = dict()
        for v in self.bufferVars:
            buf = Variable.val ( v.dataType, ident.registerBuffer ( v ) )
            sbuf = Variable.val ( v.dataType, ident.registerShuffleBuffer ( v ) )
            buf.declare ( ctxt.codegen.init() ) 
            sbuf.declare ( ctxt.codegen.init() ) 
            self.buffers [ v.get() ] = buf
            self.shuffleBuffers [ v.get() ] = sbuf

    
    def consumeFlushToBuffer ( self, numactive, mask ):
        ctxt = self.ctxt
        codegen = ctxt.codegen

        comment ("flush to divergence buffer", ctxt.codegen)
        with IfClause ( larger ( numactive, intConst(0) ), ctxt.codegen ):

            comment ("warp prefix scan of remaining active lanes", ctxt.codegen)
            emit ( assign ( self.scan, add ( popcount ( andBitwise ( mask, codegen.prefixlanes() ) ), ctxt.vars.buffercount ) ), ctxt.codegen )
            
            comment ("write to buffer", ctxt.codegen)
            if self.buftype is BufferType.SMEM:
                self.bufWriteSmem ( numactive, mask )
            if self.buftype is BufferType.REG:
                self.bufWriteReg ( numactive, mask )

            emit ( assignAdd ( ctxt.vars.buffercount, numactive ), ctxt.codegen )
            emit ( assign ( ctxt.vars.activeVar, intConst(0) ), ctxt.codegen )
   

    def bufWriteSmem ( self, numactive, mask ):
        ctxt = self.ctxt

        emit ( assign ( self.buf_ix, add ( self.bufferbase, self.scan ) ), ctxt.codegen )
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
            for v in self.bufferVars:
                emit ( assign ( self.buffers[v.get()].arrayAccess ( self.buf_ix ), v ), ctxt.codegen )
        emit ( syncwarp(), ctxt.codegen )
    
    def bufWriteReg ( self, numactive, mask ):
        ctxt = self.ctxt

        comment("scatter source lane", ctxt.codegen)
        emit ( assign ( self.activeDest, andLogic ( largerEqual ( codegen.warplane(), ctxt.vars.buffercount ), smaller ( codegen.warplane(), add ( ctxt.vars.buffercount, numactive ) ) ) ), ctxt.codegen )
        emit ( assign ( self.sourceLane, codegen.warplane() ), ctxt.codegen )
        with IfClause ( self.ctxt.vars.activeVar, ctxt.codegen ):
            emit ( assign ( self.shuffleSourceLane.arrayAccess ( add ( self.bufferbase, self.scan ) ), ctxt.codegen.warplane() ), ctxt.codegen )

        emit ( syncwarp(), ctxt.codegen )
        with IfClause ( self.activeDest, ctxt.codegen ):
            emit ( assign ( self.sourceLane, self.shuffleSourceLane.arrayAccess ( add ( self.bufferbase, codegen.warplane() ) ) ), ctxt.codegen )

        comment("gather tuples to buffer", ctxt.codegen)
        with IfClause ( orLogic ( ctxt.vars.activeVar, self.activeDest ), ctxt.codegen ):
            for v in self.bufferVars:
                emit ( assign ( v, shuffleIntr ( activemask(), v, self.sourceLane ) ), ctxt.codegen )
        with IfClause ( self.activeDest, ctxt.codegen ):
            for v in self.bufferVars:
                emit ( assign ( self.buffers [ v.get() ], v ), ctxt.codegen )


    def consumeRefillFromBuffer ( self, numactive, mask, threshold ):
        ctxt = self.ctxt
        codegen = self.ctxt.codegen
        
        comment ("refill inactive lanes from shared memory buffer", codegen)
        with IfClause ( andLogic ( smaller ( numactive, intConst(threshold) ), ctxt.vars.buffercount), codegen ):
            emit ( assign ( self.numRemaining, maxMath ( sub ( add ( ctxt.vars.buffercount, numactive ), intConst(32) ), intConst(0) ) ), codegen )
            comment ("prefix scan of inactive lanes", codegen)            
            emit ( assign ( self.scan, popcount ( andBitwise ( inverse ( mask ), codegen.prefixlanes() ) ) ), codegen )

            comment ("gather buffered data (tids, datastructure state, computed values)", codegen)
            if self.buftype is BufferType.SMEM:
                self.bufReadSmem ( numactive, mask )
            if self.buftype is BufferType.REG:
                self.bufReadReg ( numactive, mask )

            comment ("decrement buffer count", codegen)
            emit ( assign ( ctxt.vars.buffercount, self.numRemaining ), codegen )

    def bufReadSmem ( self, numactive, mask ):
        ctxt = self.ctxt
        codegen = self.ctxt.codegen
        with IfClause ( andLogic ( notLogic ( ctxt.vars.activeVar ), smaller ( self.scan, ctxt.vars.buffercount ) ), codegen ):
           emit ( assign ( self.buf_ix, ( add ( self.numRemaining, add ( self.scan, self.bufferbase ) ) ) ), codegen )
           for v in self.bufferVars:
               emit (assign ( v, self.buffers[ v.get() ].arrayAccess ( self.buf_ix ) ), codegen ) 
           emit (assign ( ctxt.vars.activeVar, intConst(1) ), codegen )

    def bufReadReg ( self, numactive, mask ):
        ctxt = self.ctxt
        codegen = self.ctxt.codegen

        emit ( assign ( self.activeDest, andLogic ( notLogic ( ctxt.vars.activeVar ), smaller ( self.scan, ctxt.vars.buffercount ) ) ), codegen )
        emit ( assign ( self.activeSource, andLogic ( largerEqual ( codegen.warplane(), self.numRemaining ), smaller ( codegen.warplane(), ctxt.vars.buffercount ) ) ), codegen ) 
        emit ( assign ( self.sourceLane, self.scan ), codegen )

        with IfClause ( orLogic ( self.activeSource, self.activeDest ), codegen):
            for v in self.bufferVars:
                emit ( assign ( self.shuffleBuffers [ v.get() ], shuffleIntr ( activemask(), self.buffers [ v.get() ], self.sourceLane ) ), codegen )
        with IfClause ( self.activeDest, codegen):
            for v in self.bufferVars:
                emit ( assign ( v, self.shuffleBuffers [ v.get() ] ), codegen )
            emit (assign ( ctxt.vars.activeVar, intConst(1) ), codegen )


class BufferedLoop ( object ):

    def __init__ ( self, ctxt, bufferVars, opId, threshold=0.8 ):
        self.ctxt = ctxt
        self.threshold = int ( 32 * threshold )
        self.hasBalancingCode = False
        self.bufferVars = bufferVars
        self.opId = opId

        buffercount = Variable.val ( CType.INT, "buffercount" + str(opId) + "_" )
        buffercount.declareAssign ( intConst(0), ctxt.codegen.init() )
        ctxt.vars.buffercount = buffercount

        self.bufferHelper = BufferHelperSharedMemory ( ctxt, opId, BufferType.SMEM )

        self.open ()

    def __enter__ ( self ):
        pass

    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        self.close()

    def addBalancingCode ( self, code ):
        self.balancingCode = code
        self.hasBalancingCode = True


    def open ( self ):
        ctxt = self.ctxt
        
        commentOperator ("divergence buffer", self.opId, ctxt.codegen)
        comment ("ensures that the thread activity in each warp (32 threads) lies above a given threshold", ctxt.codegen)
        comment ("depending on the buffer count inactive lanes are either refilled or flushed to the buffer", ctxt.codegen)

        self.mask = Variable.val ( CType.INT, "activemask" + str(self.opId) + "_" )
        self.numactive = Variable.val ( CType.INT, "numactive" + str(self.opId) + "_" )
        self.bailout = Variable.val ( CType.INT, "minTuplesInFlight" + str(self.opId) + "_" )

        self.mask.declareAssign ( ballotIntr ( qlib.Const.ALL_LANES, ctxt.vars.activeVar ), ctxt.codegen )
        self.numactive.declareAssign ( popcount ( self.mask ), ctxt.codegen )
        
        # declare buffer variables ( shuffle registers or shared memory )
        self.bufferHelper.consumeDeclareBuffer ( self.bufferVars )
        
        emit ( assign ( declare ( self.bailout ), inlineIf( ctxt.vars.flushVar, intConst(0), intConst(self.threshold))), ctxt.codegen )    

        # starts main buffer loop
        self.whileLoop = WhileLoop ( larger ( add ( ctxt.vars.buffercount, self.numactive ), self.bailout ), ctxt.codegen )
      
        # refill active lanes if activity below threshold 
        self.bufferHelper.consumeRefillFromBuffer ( self.numactive, self.mask, self.threshold )
        
        return self


    def close ( self ):
        ctxt = self.ctxt
        
        endVar = Variable.val ( CType.INT, "matchEnd" )
        offsetVar = Variable.val ( CType.INT, "matchOffset" )
        matchStepVar = Variable.val ( CType.INT, "matchStep" )

        emit ( assign ( self.mask, ballotIntr( qlib.Const.ALL_LANES, intConst( ctxt.vars.activeVar ) ) ), ctxt.codegen ) 
        emit ( assign ( self.numactive, popcount ( self.mask ) ), ctxt.codegen )
       
        # closes main buffer loop
        self.whileLoop.close()

        # write remaining active tuples to buffer
        self.bufferHelper.consumeFlushToBuffer ( self.numactive, self.mask ) 


