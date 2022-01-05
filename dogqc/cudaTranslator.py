from enum import Enum
import dogqc.identifier as ident
from dogqc.cudalang import *
from dogqc.variable import Variable
from dogqc.code import Code
from dogqc.gpuio import GpuIO
from dogqc.kernel import Kernel, KernelCall
from dogqc.relationalAlgebra import Reduction
from dogqc.relationalAlgebra import Join
from dogqc.util import listWrap
from dogqc.relationalAlgebra import MaterializationType
from dogqc.cudaDivergenceBuffer import LaneRefill
from dogqc.cudaLaneActivityProfile import LaneActivityProfiler
from dogqc.translatorBase import LiteralTranslator, UnaryTranslator, BinaryTranslator
from dogqc.hashTableUtil import Payload, HashTableMemory, Hash
from dogqc.hashJoins import EquiJoinTranslator
import dogqc.querylib as qlib
import dogqc.cudalang
import subprocess
import os
import copy
from dogqc.codegen import CodeGenerator
from dogqc.types import Type
import sys
            

class Vars ( object ):
    pass


class CudaCompiler ( object ):
 
    def __init__ ( self, algebraContext, smArchitecture = "sm_52", decimalRepr = CType.FP32, debug = False ):
        self.decimalRepr = decimalRepr
        self.debug = debug
        self.smArchitecture = smArchitecture
        self.lang = dogqc.cudalang
        self.algebraContext = algebraContext
        self.bufferPositions = []
        self.profilerPositions = []
        # innerLoopCount:
        # increment in consume implementations, which leave a loop open when calling next consume (e.g. multimatch join loop)
        # decrement when loop is closed
        self.innerLoopCount = 0

    def gencode ( self, translationPlan ):
        self.codegen = CodeGenerator ( self.decimalRepr )
        self.vars = Vars ()
        self.attFile = PipelineAttributesFile ( self.algebraContext, self.vars, self.codegen )
        for node in translationPlan:
            node.produce ( self )

    def compile ( self, filename ):
        return self.codegen.compile_ ( filename, self.smArchitecture, self.debug )
  
    def execute ( self ):
        self.codegen.execute () 
    
    def setBuffers ( self, positions ):
        self.bufferPositions = positions
    
    def setProfilers ( self, positions ):
        self.profilerPositions = positions

    def translate ( self, algebraExpr, child1=None, child2=None ): 
        op = None
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.Scan ):
            op = ScanTranslator ( algebraExpr )
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.Selection ):
            op = SelectionTranslator ( algebraExpr, child1 )
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.Map ):
            op = MapTranslator ( algebraExpr, child1 )
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.EquiJoin ):
            op = EquiJoinTranslator ( algebraExpr, child1, child2 )
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.CrossJoin ):
            op = NestedJoinTranslator ( algebraExpr, child1, child2 )
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.Aggregation ):
            op = AggregationTranslator ( algebraExpr, child1 )
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.Projection ):
            op = ProjectionTranslator ( algebraExpr, child1 )
        if isinstance ( algebraExpr, dogqc.relationalAlgebra.Materialize ):
            op = MaterializeTranslator ( algebraExpr, child1 )

        # add additional operators if specified for the current position
        if algebraExpr.opId in self.bufferPositions:
            op = LaneRefill ( algebraExpr, op )
        if algebraExpr.opId in self.profilerPositions:
            op = LaneActivityProfiler ( algebraExpr, op )
        return op

    # visualize plan
    def showGraph ( self, plan ):
        from graphviz import Digraph
        plan = listWrap ( plan )
        graph = Digraph ()
        graph.graph_attr['rankdir'] = 'BT'
        for node in plan:
            node.toDOT ( graph )
        file = open("query.dot","w") 
        file.write ( graph.source )
        graph.render("qplan")
         
        

class AttributeLocation ( Enum ):
    TABLE = 1
    REGISTER = 2
    TEMPTABLE = 3


class PipelineAttributesFile ( object ):

    def __init__ ( self, algebraContext, vars, codegen ):
        self.codegen = codegen
        self.algCtxt = algebraContext
        self.vars = vars

        # attribute variable in register
        self.incolFile = dict()
        # attribute variables in input columns
        self.regFile = dict()
        # intermediate materialization of attributes
        self.itmFile = dict()
        # attribute output columns variables
        self.ocolFile = dict()
        # attribute location information
        self.locFile = dict()
        # remember value null condition of attributes
        self.isNullFile = dict()
        # remember base table columns
        self.baseColumns = dict()

        self.itmSourceAttributes = dict()
    
    def file ( self, var ):
        return 'mmdb/' + var.get()
   
    def mapInputAttribute ( self, attr, table ):
        dbIdent = table ["name"] + "_" + attr.name
        attIdent = ident.iatt ( attr )
        size = table [ "size" ]
        charSize = 0
        if attr.dataType == Type.STRING:
            charSize = table [ "charSizes" ] [ attr.name ]
        dbCol = self.inputColumns ( attr, dbIdent, size, charSize )
        attCol = self.inputColumns ( attr, attIdent, size, charSize )
        for a, db in zip ( listWrap ( attCol ), listWrap ( dbCol ) ):
            # base table attributes are currently always stored as FP32
            if a.dataType == CType.FP64:
                a.dataType = CType.FP32
            baseColFileSys = self.file ( db ) 
            # add new gpu input column
            if baseColFileSys not in self.baseColumns:
                a.declarePointer ( self.codegen.read )
                emit ( assign ( a, mmapFile ( a.dataType, baseColFileSys ) ), self.codegen.read )
                self.codegen.gpumem.mapForRead ( a )
                self.baseColumns [ baseColFileSys ] = a.getGPU() 
            # gpu input column that was already used
            else:
                a.declarePointer ( self.codegen.read )
                emit ( assign ( a, mmapFile ( a.dataType, baseColFileSys ) ), self.codegen.read )
                self.codegen.gpumem.declare ( a )
                emit ( assign ( a.getGPU(), self.baseColumns [ baseColFileSys ] ), self.codegen.gpumem.cudaMalloc )
            self.codegen.currentKernel.addVar ( a )
        self.locFile [ attr.id ] = AttributeLocation.TABLE
        self.incolFile [ attr.id ] = attCol
    
    def mapTemptableInputAttribute ( self, a, table ):
        if a.id not in self.itmFile:
            self.itmFile [ a.id ] = self.itmFile [ a.sourceId ] 
        itmCol = self.itmFile [ a.id ]
        self.codegen.currentKernel.addVar ( itmCol )
        self.locFile [ a.id ] = AttributeLocation.TEMPTABLE
    
    def mapTemptableOutputAttribute ( self, a, table, sizeEstimate ):
        tempIdent = "itm_" + table["name"] + "_" + a.name
        itmCol = self.columns ( a, tempIdent, sizeEstimate )  
        if a.dataType == Type.STRING:
            itmCol = Variable ( CType.STR_TYPE, tempIdent, sizeEstimate );
        self.codegen.gpumem.declareAllocate ( itmCol )           
        self.codegen.currentKernel.addVar ( itmCol )
        self.itmFile [ a.id ] = itmCol
 
    def mapOutputAttribute ( self, a, expectedSize, outputSize ):
        outIdent = ident.oatt ( a )
        oc = self.columns ( a, outIdent, expectedSize )  
        if a.dataType == Type.STRING:
            oc = oc[0]
        oc.declareVector ( self.codegen.declare )
        oc.ishostvector = True
        self.codegen.gpumem.mapForWrite ( oc, outputSize )
        self.codegen.currentKernel.addVar ( oc )
        self.ocolFile [ a.id ] = oc

    def declareRegister ( self, a ):
        regIdent = ident.att ( a )
        regVar = self.variable ( a, regIdent )  
        regVar.declare ( self.codegen.init() )
        self.regFile [ a.id ] = regVar
        self.locFile [ a.id ] = AttributeLocation.REGISTER
        return regVar

    def setSourceString ( self, a, aStr ):
        self.incolFile [ a.id ] = self.incolFile [ aStr.id ]

    def dematerializeAttributeFromSource ( self, a, source ):
        regVar = self.declareRegister ( a )
        emit ( assign ( regVar, source ), self.codegen )
    
    def dematerializeAttribute ( self, a, tid ):
        if self.locFile [ a.id ] == AttributeLocation.TABLE:
            access = self.accessTable ( a, tid )
        if self.locFile [ a.id ] == AttributeLocation.TEMPTABLE:
            access = self.accessTemptable ( a, tid )
        regVar = self.declareRegister ( a )
        emit ( assign ( regVar, access ), self.codegen )

    def materializeAttribute ( self, a, wpVar, matType ):
        acc = self.access ( a )
        if matType == MaterializationType.RESULT:
            oc = self.ocolFile [ a.id ]
            if a.dataType == Type.STRING:
                chrCol = self.incolFile [ a.id ][1]
                acc = call ( "toStringOffset", [ chrCol, acc ] )   
                self.codegen.currentKernel.addVar ( chrCol )
        elif matType == MaterializationType.TEMPTABLE:
            oc = self.itmFile [ a.id ]
        emit ( assign ( oc.arrayAccess ( wpVar ), acc ), self.codegen ) 
       
    def access ( self, a ):
        if self.locFile [ a.id ] == AttributeLocation.REGISTER:
            return self.accessRegister ( a )
        else:
            raise ValueError('Attribute resides in Table and needs to be dematerialized before access.')

    def accessTable ( self, a, tid ): 
        if a.dataType == Type.STRING:
            cols = self.incolFile [ a.id ]
            return call ( "stringScan", [ cols[0], cols[1], tid ] )   
        else:
            var = self.incolFile [ a.id ]
            return var.arrayAccess ( tid )
    
    def accessTemptable ( self, a, tid ): 
        var = self.itmFile [ a.id ]
        return var.arrayAccess ( tid )
    
    def accessRegister ( self, a ):
        var = self.regFile [ a.id ]
        return var.get()
            
    def variable ( self, a, identifier, size=0 ):
        return Variable ( self.codegen.langType ( a.dataType ), identifier, size )
    
    def columns ( self, a, identifier, size, charSize=0 ):
        if ( a.dataType == Type.STRING ):
            var1 = Variable ( CType.STR_OFFS, identifier + "_offset", size );
            var2 = Variable ( CType.CHAR, identifier + "_char", charSize );
            return [var1, var2] 
        else:
            return Variable ( self.codegen.langType ( a.dataType ), identifier, size )
    
    def inputColumns ( self, a, identifier, size, charSize=0 ):
        if ( a.dataType == Type.STRING ):
            var1 = Variable ( CType.SIZE, identifier + "_offset", add ( size, intConst(1) ) );
            var2 = Variable ( CType.CHAR, identifier + "_char", charSize );
            return [var1, var2] 
        else:
            return Variable ( self.codegen.langType ( a.dataType ), identifier, size )


class ScanType ( Enum ):
    KERNEL = 1
    INNER = 2

class ScanLoop ( object ):
        
    def __init__ ( self, tid, scanType, isTempScan, table, scanRelation, algExpr, ctxt ):
        vars = ctxt.vars
        self.scanType = scanType
        codegen = ctxt.codegen
        self.ctxt = ctxt
        
        if scanType == scanType.KERNEL:
           scanKernel = ctxt.codegen.openKernel ( Kernel ( ident.scanKernel ( algExpr ) + str(algExpr.opId) ) )
           tid.declareAssign ( intConst(0), ctxt.codegen )
           vars.loopVar = Variable.val ( CType.UINT, "loopVar" )
           vars.loopVar.declareAssign ( add ( mul ( blockIdx_x(), blockDim_x() ), threadIdx_x() ), codegen )
           vars.stepVar = Variable.val ( CType.UINT, "step" )
           vars.stepVar.declareAssign ( mul ( blockDim_x(), gridDim_x() ), codegen )
           vars.flushVar = Variable.val ( CType.UINT, "flushPipeline", codegen, intConst(0) )
           vars.activeVar = Variable.val ( CType.INT, "active", codegen, intConst(0) )
           self.kernelLoop = WhileLoop ( notLogic ( vars.flushVar ), codegen )
           emit ( assign ( vars.scanTid, vars.loopVar ), codegen )
           emit ( assign ( vars.activeVar, smaller ( vars.loopVar, table["size"] ) ), codegen ) 
           comment ( "flush pipeline if no new elements", codegen ) 
           emit ( assign ( vars.flushVar, notLogic ( ballotIntr( qlib.Const.ALL_LANES, vars.activeVar ) ) ), codegen )                

        # inner scan loop
        elif scanType == scanType.INNER:
            self.outerActive = Variable.val ( CType.INT, "outerActive" + str ( algExpr.opId ) )
            self.outerActive.declareAssign ( ctxt.vars.activeVar, ctxt.codegen )
            self.innerLoop = ForLoop (  
                assign ( declare ( tid ), intConst(0) ), 
                smaller ( tid, table["size"] ), 
                increment ( tid ), ctxt.codegen )
            emit ( assign ( ctxt.vars.activeVar, self.outerActive ), ctxt.codegen )

        # map data columns 
        for (id, a) in scanRelation.items():
            if not isTempScan:
                ctxt.attFile.mapInputAttribute ( a, table )
            else:
                ctxt.attFile.mapTemptableInputAttribute ( a, table )

        # dematerialize
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
            for id, a in scanRelation.items():
                ctxt.attFile.dematerializeAttribute ( a, tid )

    def __enter__ ( self ): 
        return self
  
    def __exit__ ( self, exc_type, exc_val, exc_tb ):
        if self.scanType == ScanType.KERNEL:
            emit ( assignAdd ( self.ctxt.vars.loopVar, self.ctxt.vars.stepVar ), self.ctxt.codegen )
            self.kernelLoop.close()
            self.ctxt.codegen.closeKernel()
        elif self.scanType == ScanType.INNER:
            self.innerLoop.close()


class ScanTranslator ( LiteralTranslator ): 
    
    def consume ( self, ctxt ):
        pass

    def produce ( self, ctxt ):
        algExpr = self.algExpr       
        #commentOperator ( "scan", ctxt.codegen )

        ctxt.vars.scanTid = Variable.tidLit ( algExpr.table, algExpr.scanTableId )
    
        with ScanLoop ( ctxt.vars.scanTid, ScanType.KERNEL, algExpr.isTempScan, self.algExpr.table, algExpr.outRelation, algExpr, ctxt ):
            if self.algExpr.isTempScan:
                numOutVar = Variable.val ( CType.INT, "nout_" + algExpr.table["name"] ) 
                ctxt.codegen.currentKernel.addVar ( numOutVar )

            # call parent operator
            self.parent.consume ( ctxt )


class SelectionTranslator ( UnaryTranslator ): 
    
    def produce ( self, ctxt ):
        self.child.produce ( ctxt ) 

    def consume ( self, ctxt ):
        commentOperator ( "selection", self.algExpr.opId, ctxt.codegen)

        algExpr = self.algExpr
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
            emit ( assign ( ctxt.vars.activeVar, algExpr.condition.translate ( ctxt ) ), ctxt.codegen )

        if self.parent:
            self.parent.consume( ctxt )


class NestedJoinTranslator ( BinaryTranslator ):
    
    def __init__ ( self, algExpr, leftChild, rightChild ):
        super().__init__( algExpr, leftChild, rightChild )
        self.consumeCall = 0
        
    def produce ( self, ctxt ):
        self.leftChild.produce ( ctxt )
        self.rightChild.produce ( ctxt )

    def consume ( self, ctxt ):
        self.consumeCall += 1
        if ( self.consumeCall % 2 == 1):
            commentOperator ("nested join: materialize inner ", self.algExpr.opId, ctxt.codegen)
            self.tableName = "inner" + str ( self.algExpr.opId )
            self.denseWrite = DenseWrite ( self.algExpr.leftChild.outRelation, self.tableName, MaterializationType.TEMPTABLE, self.algExpr.leftChild.tupleNum, ctxt )
            
        elif ( self.consumeCall % 2 == 0):
            commentOperator ("nested join: loop inner ", self.algExpr.opId, ctxt.codegen)
            ctxt.codegen.currentKernel.addVar ( self.denseWrite.numOut )
            self.innerTid = Variable.tidLit ( self.denseWrite.getTable(), 0 )
   
            ctxt.innerLoopCount += 1 
            with ScanLoop ( self.innerTid, ScanType.INNER, True, self.denseWrite.getTable(), self.algExpr.leftChild.outRelation, self.algExpr, ctxt ):
                with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                    emit ( assign ( ctxt.vars.activeVar, self.algExpr.condition.translate ( ctxt ) ), ctxt.codegen )
                # call parent operator
                self.parent.consume ( ctxt )
            ctxt.innerLoopCount -= 1 




class MapTranslator ( UnaryTranslator ):
 
    def __init__( self, algExpr, child ):
        UnaryTranslator.__init__ ( self, algExpr, child )
 
    def produce( self, ctxt ):
        self.child.produce( ctxt )

    def consume ( self, ctxt ):
        commentOperator ("map", self.algExpr.opId, ctxt.codegen)

        # for map strings we set a reference to the char heap for the new attribute
        if len ( self.algExpr.mapStringAttributes ) == 1:
            ctxt.attFile.setSourceString ( self.algExpr.mapAttr, self.algExpr.mapStringAttributes[0] )

        ctxt.attFile.declareRegister ( self.algExpr.mapAttr )
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):  
            emit ( assign ( ctxt.attFile.access ( self.algExpr.mapAttr ), 
                self.algExpr.expression.translate ( ctxt ) ), ctxt.codegen )
        
        self.parent.consume( ctxt )


class AggregationTranslator ( UnaryTranslator ):

    def produce( self, ctxt ):
        self.child.produce ( ctxt )
        self.consumeHashTable ( ctxt )

    def consume ( self, ctxt ):
        commentOperator ("aggregation", self.algExpr.opId, ctxt.codegen)
        
        #ctxt.codegen.laneActivityProfile ( ctxt )

        # create aggregation hash table with grouping payload 
        if self.algExpr.doGroup:
            self.payload = Payload ( "apayl" + str ( self.algExpr.opId ), self.algExpr.groupAttributes, ctxt )
            htmem = HashTableMemory.createAgg ( "aht" + str ( self.algExpr.opId ), self.algExpr.tupleNum * 2.0, self.payload, ctxt.codegen )
        else:    
            htmem = HashTableMemory ( 1, ctxt.codegen )

        # create and initialize aggregation buckets
        htmem.addAggregationAttributes ( self.algExpr.aggregateAttributes, self.algExpr.aggregateTuples, ctxt )
        htmem.addToKernel ( ctxt.codegen.currentKernel )

        # find bucket
        bucketVar = Variable.val ( CType.INT, "bucket", ctxt.codegen, intConst(0) )
        if self.algExpr.doGroup:
            with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                #payload
                hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
                Hash.attributes ( self.algExpr.groupAttributes, hashVar, ctxt )
                payl = self.payload.materialize ( "payl", ctxt.codegen, ctxt )
                bucketFound = Variable.val ( CType.INT, "bucketFound", ctxt.codegen, intConst(0) )
                numLookups = Variable.val ( CType.INT, "numLookups", ctxt.codegen, intConst(0) )
                
                with WhileLoop ( notLogic ( bucketFound ), ctxt.codegen ) as loop:
                    # allocate empty bucket or get tid from bucket
                    emit ( assign ( bucketVar, call ( qlib.Fct.HASH_AGG_BUCKET, 
                        [ htmem.ht, htmem.numEntries, hashVar, numLookups, addressof ( payl ) ] ) ), ctxt.codegen )
                    # verify grouping attributes from bucket
                    probepayl = Variable.val ( self.payload.getType(), "probepayl", ctxt.codegen, member ( htmem.ht.arrayAccess ( bucketVar ), "payload" ) )
                    self.payload.checkEquality ( bucketFound, payl, probepayl, ctxt )

        # atomic summation of aggregates
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
            for id, (inId, reduction) in self.algExpr.aggregateTuples.items():
                typ = ctxt.codegen.langType ( self.algExpr.aggregateAttributes[id].dataType )
                agg = addressof ( htmem.accessAggregationAttribute ( id, bucketVar ) )
                # count
                if reduction == Reduction.COUNT:
                    sys.stdout.flush()
                    atomAdd = atomicAdd ( agg, cast ( typ, intConst(1) ) )
                    if inId in ctxt.attFile.isNullFile:
                        with IfClause ( notLogic ( ctxt.attFile.isNullFile [ inId ] ), ctxt.codegen ):
                            emit ( atomAdd, ctxt.codegen ) 
                    else:
                        emit ( atomAdd, ctxt.codegen ) 
                    continue
                val = cast ( typ, ctxt.attFile.access ( self.algExpr.aggregateInAttributes[inId] ) )
                # min
                if reduction == Reduction.MIN:
                    emit ( atomicMin ( agg, val ), ctxt.codegen ) 
                # max
                elif reduction == Reduction.MAX:
                    emit ( atomicMax ( agg, val ), ctxt.codegen ) 
                # sum
                elif reduction == Reduction.SUM:
                    emit ( atomicAdd ( agg, val ), ctxt.codegen ) 
                # avg
                elif reduction == Reduction.AVG:
                    emit ( atomicAdd ( agg, val ), ctxt.codegen ) 

        self.htmem = htmem
        
    def consumeHashTable ( self, ctxt ):
        htmem = self.htmem
        
        ctxt.vars.scanTid = Variable.tidLit ( htmem.getTable ( self.algExpr.opId ), self.algExpr.opId )

        self.algExpr.table = htmem.getTable ( self.algExpr.opId )
        self.algExpr.scanTableId = 1
                
        with ScanLoop ( ctxt.vars.scanTid, ScanType.KERNEL, False, htmem.getTable ( self.algExpr.opId ), dict(), self.algExpr, ctxt ):
            commentOperator ("scan aggregation ht", self.algExpr.opId, ctxt.codegen)
            htmem.addToKernel ( ctxt.codegen.currentKernel )
            if self.algExpr.doGroup:
                with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                    emit ( assignAnd ( ctxt.vars.activeVar, equals ( member ( htmem.ht.arrayAccess ( 
                        ctxt.vars.scanTid ), "lock.lock" ), "OnceLock::LOCK_DONE" ) ), ctxt.codegen )
                with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                    payl = Variable.val ( self.payload.getType(), "payl" )
                    payl.declareAssign ( member ( htmem.ht.arrayAccess ( ctxt.vars.scanTid ), "payload" ), ctxt.codegen )
                    self.payload.dematerialize ( payl, ctxt )
            with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                htmem.dematerializeAggregationAttributes ( ctxt.vars.scanTid, ctxt )
                for (id, att) in self.algExpr.avgAggregates.items():
                    count = self.algExpr.countAttr
                    type = ctxt.codegen.langType ( att.dataType )
                    emit ( assign ( ctxt.attFile.access ( att ), div ( ctxt.attFile.access ( att ), 
                        cast ( type, ctxt.attFile.access ( count ) ) ) ), ctxt.codegen )

            # call parent operator
            self.parent.consume ( ctxt )


class ProjectionTranslator ( UnaryTranslator ):
    
    def produce( self, ctxt ):
        self.child.produce ( ctxt )

    def consume( self, ctxt ):
        commentOperator ("projection (no code)", self.algExpr.opId, ctxt.codegen)
        self.parent.consume ( ctxt )


class DenseWrite ( object ):

    def __init__ ( self, relation, tableName, matType, sizeEstimate, ctxt ):
        vars = ctxt.vars
        codegen = ctxt.codegen  
        self.relation = relation
        self.tableName = tableName

        self.numOut = Variable.val ( CType.INT, "nout_" + tableName, codegen.declare ) 
        codegen.gpumem.mapForWrite ( self.numOut )
        codegen.gpumem.initVar ( self.numOut, intConst ( 0 ) ) 
        codegen.currentKernel.addVar ( self.numOut )

        wp = Variable.val ( CType.INT, "wp", codegen)
        self.useWarpScan = True
        if not self.useWarpScan:
            with IfClause ( ctxt.activeVar, codegen ):
                emit ( assign ( wp, atomicAdd ( self.numOut, intConst(1) ) ), codegen )
            codegen.add ( codewrite )
        else:
            mask = Variable.val (CType.INT, "writeMask", codegen )
            numactive = Variable.val (CType.INT, "numProj", codegen )
            emit ( assign ( mask, ballotIntr( qlib.Const.ALL_LANES, intConst( vars.activeVar ) ) ), codegen )
            emit ( assign ( numactive, popcount ( mask ) ), codegen )
            with IfClause ( equals ( codegen.warplane(), intConst(0) ), codegen): 
                emit ( assign ( wp, atomicAdd ( self.numOut, numactive ) ), codegen )
            emit ( assign ( wp, shuffleIntr ( qlib.Const.ALL_LANES, wp, intConst(0) ) ), codegen )      
            emit ( assign ( wp, add ( wp, popcount ( andBitwise ( mask, codegen.prefixlanes() ) ) ) ), codegen )

        with IfClause ( vars.activeVar, codegen ):
            for id, att in relation.items():
                if matType == MaterializationType.RESULT:
                    ctxt.attFile.mapOutputAttribute ( att, sizeEstimate, self.numOut.name )
                elif matType == MaterializationType.TEMPTABLE:
                    ctxt.attFile.mapTemptableOutputAttribute ( att, self.getTable(), sizeEstimate )
                ctxt.attFile.materializeAttribute ( att, wp, matType )

    def getTable ( self ):
        table = dict()
        table [ "name" ] = self.tableName
        table [ "size" ] = deref ( self.numOut )
        return table


class MaterializeTranslator ( UnaryTranslator ):
    
    def produce( self, ctxt ):
        self.child.produce ( ctxt )

    def consume( self, ctxt ):
        vars = ctxt.vars       
        codegen = ctxt.codegen
        matType = self.algExpr.matType
 
        commentOperator ("materialize", self.algExpr.opId, ctxt.codegen)

        tableName = "result"
        if matType == MaterializationType.TEMPTABLE:
            tableName = self.algExpr.table["name"]

        denseWrite = DenseWrite ( self.algExpr.outRelation, tableName, matType, self.algExpr.tupleNum, ctxt )
        
        if matType == MaterializationType.TEMPTABLE:
            self.algExpr.table["size"] = deref ( denseWrite.numOut )

        if hasattr ( self, "parent" ):
            self.parent.consume( ctxt )

        if matType == MaterializationType.RESULT:
            emit ( printf ( "\\nResult: %i tuples\\n", [ denseWrite.numOut ]), codegen.finish )  
            # error check for output sizes larger than allocated array size
            with IfClause ( larger ( denseWrite.numOut, intConst ( self.algExpr.tupleNum ) ), codegen.finish ):
                ERROR ( "Index out of range. Output size larger than allocated with expected result number.", codegen.finish )
            # print result sample
            self.consumePrintResultSample ( ctxt, denseWrite.numOut )
            # write query result to file
            self.printToFile = False
            if self.printToFile:
                self.consumePrintToFile ( ctxt, denseWrite.numOut )

    def consumePrintResultSample (self, ctxt, numOutVar ):
        # print sample of results
        codegen = ctxt.codegen
        codeout = codegen.finish
        loopVar = Variable.val ( CType.INT, "pv" )
        printLimit = intConst(10)
        with ForLoop ( assign ( declare ( loopVar ), intConst(0) ), 
                       andLogic ( smaller ( loopVar, printLimit ), smaller ( loopVar, numOutVar ) ),
                       assignAdd ( loopVar, intConst(1) ), codeout ):

            for id, att in self.algExpr.outRelation.items():
                emit ( printf ( att.name + ": " ), codeout )
                ovar = ctxt.attFile.ocolFile [ id ] 
                if att.dataType == Type.STRING:
                    offs = ovar.arrayAccess ( loopVar )
                    charCol = ctxt.attFile.incolFile [ att.id ][1]
                    emit ( call ( "stringPrint", [ charCol, offs ] ), codeout )    
                else:
                    emit ( printf ( CType.printFormat [ ovar.dataType ], [ ovar.arrayAccess ( loopVar ) ] ), codeout ) 
                emit ( printf ( "  " ), codeout )
            emit ( printf ( "\\n", [] ), codeout )

        with IfClause ( larger ( numOutVar, printLimit ), codeout ):
            emit ( printf ( "[...]\\n" ), codeout )
        
        emit ( printf ( "\\n" ), codeout )

    def consumePrintToFile (self, ctxt, numOutVar):
        # print results to file
        codeout = ctxt.codegen.finish
        outFile = Variable.val ( ptr ( CType.FILE ), "outFile", codeout )
        emit ( assign ( outFile, fopen ( "queryresult.csv", "w" ) ), codeout )
            
        for id, att in self.algExpr.outRelation.items():
            emit ( fprintf ( outFile, att.name + ", "), codeout ) 
        emit ( fprintf ( outFile, "\\n", [] ), codeout )

        loopVar = Variable.val ( CType.INT, "pv" )
        with ForLoop ( assign ( declare ( loopVar ), intConst(0) ), 
                       smaller ( loopVar, numOutVar ),
                       assignAdd ( loopVar, intConst(1) ), codeout ):

            for id, att in self.algExpr.outRelation.items():
                ovar = ctxt.attFile.ocolFile[id]
                if att.dataType == Type.STRING:
                    offs = ovar.arrayAccess ( loopVar )
                    charCol = ctxt.attFile.incolFile [ att.id ][1]
                    emit ( call ( "stringPrint", [ charCol, offs, outFile ] ), codeout )    
                else:
                    emit ( fprintf ( outFile, CType.printFormat[ovar.dataType] + "  ", [ ovar.arrayAccess ( loopVar ) ] ), codeout ) 
            emit ( fprintf ( outFile, "\\n", [] ), codeout )
