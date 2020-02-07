from dogqc.gpuio import GpuIO
from dogqc.cudalang import *
import dogqc.identifier as ident
import dogqc.querylib as qlib
from dogqc.variable import Variable
from dogqc.kernel import Kernel, KernelCall
from dogqc.types import Type
from dogqc.relationalAlgebra import Reduction


class Hash ( object ):
    
    @staticmethod 
    def attributes ( attributes, hashVar, ctxt ):
        emit ( assign ( hashVar, intConst(0) ), ctxt.codegen )
        for id, a in attributes.items():
            acc = ctxt.attFile.access ( a )
            if a.dataType == Type.STRING:
                acc = call ( "stringHash", [ acc ] ) 
                #acc = call ( "stringHashPushDown", [ ctxt.vars.activeVar, acc ] ) 
                emit ( assign ( hashVar, call ( qlib.Fct.HASH, [ add ( hashVar, acc ) ] ) ), ctxt.codegen )
            else:
                acc = cast ( CType.UINT64, acc )
                with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                    emit ( assign ( hashVar, call ( qlib.Fct.HASH, [ add ( hashVar, acc ) ] ) ), ctxt.codegen )


        return hashVar
                
    @staticmethod 
    def checkEquality ( equalvar, buildKeyAttributes, probeKeyAttributes, ctxt ):
        for (bid, b), (pid, p) in zip ( buildKeyAttributes.items(), probeKeyAttributes.items() ):
            if b.dataType == Type.STRING:
                emit ( assignAnd ( equalvar, call ( "stringEquals", [ ctxt.attFile.access ( b ), ctxt.attFile.access ( p ) ] ) ), ctxt.codegen )
            else:
                emit ( assignAnd ( equalvar, equals ( ctxt.attFile.access ( b ), ctxt.attFile.access ( p ) ) ), ctxt.codegen )


class Payload ( object ):
            
    def __init__ ( self, typeName, attributes, ctxt ):
        self.typeName = typeName
        self.attributes = attributes
        self.vars = dict()
        with StructClause ( self.typeName, ctxt.codegen.types ):
            for id, a in attributes.items():
                identifier = ident.att ( a )
                self.vars[id] = ctxt.attFile.variable ( a, identifier )
                self.vars[id].declare ( ctxt.codegen.types ) 
    
    def materialize ( self, varName, code, ctxt ):
        matvar = Variable.val ( self.typeName, varName, code )
        for id, a in self.attributes.items():
            emit ( assign ( member ( matvar, self.vars[id] ), ctxt.attFile.access ( a ) ), code )
        return matvar

    def dematerialize ( self, matvar, ctxt ):
        for id, a in self.attributes.items():
            ctxt.attFile.dematerializeAttributeFromSource ( a, member ( matvar, self.vars[id] ) )
    
    def getType ( self ):
        return self.typeName
                    
    def checkEquality ( self, equalvar, payla, paylb, ctxt ):
        emit ( assign ( equalvar, intConst (1) ), ctxt.codegen )
        for (id, attr) in self.attributes.items():
            if attr.dataType == Type.STRING:
                emit ( assignAnd ( equalvar, stringEquals ( self.access ( payla, attr ), self.access ( paylb, attr ) ) ), ctxt.codegen )
            else:
                emit ( assignAnd ( equalvar, equals ( self.access ( payla, attr ), self.access ( paylb, attr ) ) ), ctxt.codegen )

    def access ( self, paylVar, a ):
        return member ( paylVar, self.vars [ a.id ] ) 


class HashTableMemory ( object ):
    
    @staticmethod
    def power_bit_length(x):
        return 2**(int(x)-1).bit_length()

    def __init__ ( self, minEntries, codegen ):
        #self.numEntries = intConst ( self.power_bit_length ( int ( minEntries ) ) )
        self.numEntries = int ( minEntries )
        self.columns = []
        self.codegen = codegen
        self.aggregateAtts = []

    @staticmethod
    def createUnique ( name, minEntries, payload, codegen ):
        mem = HashTableMemory ( minEntries, codegen )
        mem.ht = mem.addColumn ( qlib.Type.UNIQUE_HT + "<" + payload.typeName + ">", name )
        call = KernelCall.library ( qlib.Krnl.INIT_UNIQUE_HT, [mem.ht.getGPU(), mem.numEntries], payload.typeName )
        codegen.gpumem.cudaMallocHT.add ( call.get() )   
        mem.addToKernel ( codegen.currentKernel )
        return mem

    @staticmethod
    def createMulti ( name, minHtEntries, payload, numPayloads, codegen ):
        mem = HashTableMemory ( minHtEntries, codegen )
        mem.ht = mem.addColumn ( qlib.Type.MULTI_HT, name )
        mem.payload = mem.addSizedColumn ( payload.typeName, name + "_payload", numPayloads )
        call = KernelCall.library ( qlib.Krnl.INIT_MULTI_HT, [mem.ht.getGPU(), mem.numEntries] )
        codegen.gpumem.cudaMallocHT.add ( call.get() )   

        mem.addToKernel ( codegen.currentKernel )
        return mem
    
    @staticmethod
    def createAgg ( name, minEntries, payload, codegen ):
        mem = HashTableMemory ( minEntries, codegen )
        mem.ht = mem.addColumn ( qlib.Type.AGG_HT + "<" + payload.typeName + ">", name )
        call = KernelCall.library ( qlib.Krnl.INIT_AGG_HT, [mem.ht.getGPU(), mem.numEntries], payload.typeName )
        codegen.gpumem.cudaMallocHT.add ( call.get() )   
        mem.addToKernel ( codegen.currentKernel )
        return mem
   
    def addAggregationAttributes ( self, aggregationAttributes, aggregateTuples, ctxt ):
        self.aggAtts = dict ( aggregationAttributes )
        self.aggCols = dict()
        for id, a in aggregationAttributes.items():
            init = None
            inId, reductionType = aggregateTuples [ id ]
            typ = ctxt.codegen.langType ( a.dataType )
            # additive
            if reductionType in [ Reduction.COUNT, Reduction.AVG, Reduction.SUM ]:
                init = CType.zeroValue [ typ ]
            # min
            if reductionType == Reduction.MIN:
                init = CType.maxValue [ typ ]
            # max
            elif reductionType == Reduction.MAX:
                init = CType.minValue [ typ ]

            ident = "agg" + str ( a.id )
            aggVar = ctxt.attFile.variable ( a, ident )
            self.aggCols[id] = self.addColumn ( aggVar.dataType, aggVar.get(), init )
    
    def accessAggregationAttribute ( self, id, index ):
        return self.aggCols[id].arrayAccess ( index ) 
        
    def dematerializeAggregationAttributes ( self, index, ctxt ):
        for id, a in self.aggAtts.items():
            ctxt.attFile.dematerializeAttributeFromSource ( a, self.accessAggregationAttribute ( id, index ) )
 
    def addColumnInternal ( self, dataType, name, numEntries, init ):
        col = Variable.col ( dataType, name, numEntries )
        self.codegen.gpumem.local ( col, init )
        self.columns.append ( col )
        return col
    
    def addColumn ( self, dataType, name, init=None ):
        return self.addColumnInternal ( dataType, name, self.numEntries, init )
    
    def addSizedColumn ( self, dataType, name, numEntries, init=None ):
        return self.addColumnInternal ( dataType, name, numEntries, init )

    def addToKernel ( self, kernel ):
        for c in self.columns:
            kernel.addVar ( c )

    def getTable ( self, opid ):
        table = { "name":"aggregation", "size": self.numEntries, "numColumns":0, "id":"_ht" + str(opid) }
        return table

