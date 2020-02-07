from dogqc.translatorBase import BinaryTranslator
from dogqc.hashTableUtil import Hash, Payload, HashTableMemory
from dogqc.cudalang import *
from dogqc.variable import Variable
from dogqc.kernel import KernelCall
import dogqc.querylib as qlib
from dogqc.relationalAlgebra import Join
from dogqc.cudalang import CType
from dogqc.cudaDivergenceBuffer import BufferedLoop
import copy

class EquiJoinTranslator ( BinaryTranslator ):

    usePushDownJoin = False
    
    def __init__ ( self, algExpr, leftChild, rightChild ):
        super().__init__( algExpr, leftChild, rightChild )
        self.probeWithDivergenceBuffer = False
        self.unrollDepth = 1
        self.consumeCall = 0
        self.htIdent = "jht" + str ( self.algExpr.opId )
        self.paylIdent = "jpayl" + str ( self.algExpr.opId )
        
    def produce ( self, ctxt ):
        self.leftChild.produce ( ctxt )
        self.rightChild.produce ( ctxt )

    def consume ( self, ctxt ):
        self.consumeCall += 1
        if ( self.consumeCall % 2 == 1):
            self.consumeHashJoinBuild ( ctxt )
        elif ( self.consumeCall % 2 == 0):
            self.consumeHashJoinProbe ( ctxt )

    def consumeHashJoinBuild ( self, ctxt ):
        
        commentOperator ("hash join build", ctxt.codegen)

        self.buildRelation = self.algExpr.leftChild.outRelation
        self.payload = Payload ( self.paylIdent, self.buildRelation, ctxt )

        numPayloads = self.leftChild.algExpr.tupleNum * 2
        minHtSize  = self.leftChild.algExpr.tupleNum * self.algExpr.htSizeFactor
        
        # plan
        if self.algExpr.multimatch: 
            self.htmem = HashTableMemory.createMulti ( self.htIdent, minHtSize, self.payload, numPayloads, ctxt.codegen )
            self.htmem.addToKernel ( ctxt.codegen.currentKernel )
            self.htInsertMultiMatch ( ctxt )
        else:
            # emit code for calling the hash probe function to do the insert
            if self.algExpr.joinType == Join.INNER:
                self.htmem = HashTableMemory.createUnique ( self.htIdent, minHtSize, self.payload, ctxt.codegen )
                self.htmem.addToKernel ( ctxt.codegen.currentKernel )
                self.htInsertSingleMatch ( ctxt )
            elif self.algExpr.joinType in [Join.SEMI, Join.ANTI]:
                self.htmem = HashTableMemory.createAgg ( self.htIdent, minHtSize, self.payload, ctxt.codegen )
                self.htmem.addToKernel ( ctxt.codegen.currentKernel )
                self.htInsertFilter ( ctxt )
            else:
                raise ValueError('Join type not supported by unique ht.')

        ctxt.vars.buf = []
       

    # create variables and generate code for hash join probe
    def consumeHashJoinProbe ( self, ctxt ):
            
        commentOperator ("hash join probe", ctxt.codegen)
            
        # get probe attribute and add hash table to pipeline kernel
        self.htmem.addToKernel ( ctxt.codegen.currentKernel )
           
        # plan
        if self.algExpr.multimatch:
            if self.algExpr.joinType in [Join.INNER, Join.OUTER]:
                if EquiJoinTranslator.usePushDownJoin:
                    #self.htProbeMultiMatchSingleBroadcast ( ctxt )
                    self.htProbeMultiMatchMultiBroadcast ( ctxt )
                else:
                    self.htProbeMultiMatch ( ctxt )

            elif self.algExpr.joinType in [Join.SEMI, Join.ANTI]:
                self.htProbeMultiMatchSemiAnti ( ctxt )
                #self.htProbeMultiMatchSingleBroadcastSemiAnti ( ctxt )
                #self.htProbeMultiMatchMultiBroadcastSemiAnti ( ctxt )
        else:
            if self.algExpr.joinType == Join.INNER:
                self.htProbeSingleMatch ( ctxt )
            elif self.algExpr.joinType in [Join.SEMI, Join.ANTI]:
                self.htProbeFilter ( ctxt )
    
   
    # add a call to single match hash build function into pipeline kernel
    def htInsertSingleMatch ( self, ctxt ):
        
        # execute only when current thread has active elements    
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):  
            
            # prepare payload
            payl = self.payload.materialize ( "payl" + str ( self.algExpr.opId ), ctxt.codegen, ctxt )
            
            # compute a non-unique hash over join attributes
            hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen )
            Hash.attributes ( self.algExpr.buildKeyAttributes, hashVar, ctxt )
            
            # do hash insert call 
            emit ( call ( qlib.Fct.HASH_BUILD_UNIQUE, 
                [ self.htmem.ht, self.htmem.numEntries, hashVar, addressof(payl) ] ), ctxt.codegen )


    # add a call to single match hash probe function into pipeline kernel
    def htProbeSingleMatch ( self, ctxt ):
        
        # compute a (possibly) non-unique hash over all join attributes
        hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )

        payl = Variable.ptr ( self.payload.getType(), "probepayl" + str ( self.algExpr.opId ), ctxt.codegen )

        # execute only when current thread has active elements    
        numLookups = Variable.val ( CType.INT, "numLookups" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        
        # allocate empty bucket or get tid from bucket
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):  
            emit ( assign ( ctxt.vars.activeVar, call ( qlib.Fct.HASH_PROBE_UNIQUE, 
                [ self.htmem.ht, self.htmem.numEntries, hashVar, numLookups, addressof(payl) ] ) ), ctxt.codegen )
        
        bucketFound = Variable.val ( CType.INT, "bucketFound" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        probeActive = Variable.val ( CType.INT, "probeActive" + str ( self.algExpr.opId ), ctxt.codegen, intConst(ctxt.vars.activeVar) )

        with WhileLoop ( andLogic ( probeActive, notLogic ( bucketFound ) ), ctxt.codegen ) as loop:
            paylVal = Variable.val ( self.payload.typeName, "jprobepayl" + str ( self.algExpr.opId ) )
            emit ( assign ( declare ( paylVal ), deref ( payl ) ), ctxt.codegen )
            self.payload.dematerialize ( paylVal, ctxt )
            emit ( assign ( bucketFound, intConst (1) ), ctxt.codegen )
            Hash.checkEquality ( bucketFound, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
            with IfClause ( notLogic ( bucketFound ), ctxt.codegen ):
                emit ( assign ( probeActive, call ( qlib.Fct.HASH_PROBE_UNIQUE, 
                    [ self.htmem.ht, self.htmem.numEntries, hashVar, numLookups, addressof(payl) ] ) ), ctxt.codegen )

        if self.algExpr.joinType == Join.INNER:
            emit ( assign ( ctxt.vars.activeVar, bucketFound ), ctxt.codegen )
        if self.algExpr.joinType == Join.OUTER:
            # remember null indicator for each attribute from build relation
            for nullable in self.buildRelation:
                ctxt.attFile.isNullFile [ nullable.id ] = notLogic ( bucketFound )
            
        #emit ( atomicAdd ( numCollisions, numLookups ), ctxt.codegen )
            
        # consume for parent operators
        self.parent.consume ( ctxt )

        #    emit ( assign ( ctxt.activeVar, intConst(0) ), ctxt.codegen )
        

    # add a call to single match hash build function into pipeline kernel
    def htInsertFilter ( self, ctxt ):
            
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                    
            # compute a non-unique hash over join attributes
            hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen )
            Hash.attributes ( self.algExpr.buildKeyAttributes, hashVar, ctxt )
            
            # find bucket
            bucketVar = Variable.val ( CType.INT, "bucket", ctxt.codegen, intConst(0) )
            payl = self.payload.materialize ( "payl" + str ( self.algExpr.opId ), ctxt.codegen, ctxt )
            bucketFound = Variable.val ( CType.INT, "bucketFound", ctxt.codegen, intConst(0) )
            numLookups = Variable.val ( CType.INT, "numLookups", ctxt.codegen, intConst(0) )
                
            with WhileLoop ( notLogic ( bucketFound ), ctxt.codegen ) as loop:
                # allocate empty bucket or get tid from bucket
                emit ( assign ( bucketVar, call ( qlib.Fct.HASH_AGG_BUCKET, 
                    [ self.htmem.ht, self.htmem.numEntries, hashVar, numLookups, addressof ( payl ) ] ) ), ctxt.codegen )
                # verify grouping attributes from bucket
                probepayl = Variable.val ( self.payload.getType(), "probepayl", ctxt.codegen, member ( self.htmem.ht.arrayAccess ( bucketVar ), "payload" ) )
                self.payload.checkEquality ( bucketFound, payl, probepayl, ctxt )
    

    # add a call to single match hash build function into pipeline kernel
    def htProbeFilter ( self, ctxt ):
        
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
            hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )
        
            numLookups = Variable.val ( CType.INT, "numLookups" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
            location = Variable.val ( CType.INT, "location" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
            filterMatch = Variable.val ( CType.INT, "filterMatch" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
            probeCall = call ( qlib.Fct.HASH_AGG_CHECK, [ self.htmem.ht, self.htmem.numEntries, hashVar, numLookups, location ] )
            activeProbe = Variable.val ( CType.INT, "activeProbe" + str ( self.algExpr.opId ), ctxt.codegen, intConst(1) )
             
            with WhileLoop ( andLogic ( notLogic ( filterMatch ), activeProbe ), ctxt.codegen ) as loop:
                emit ( assign ( activeProbe, probeCall ), ctxt.codegen )
                # verify grouping attributes from bucket
                with IfClause ( activeProbe, ctxt.codegen ):
                    probepayl = Variable.val ( self.payload.getType(), "probepayl", ctxt.codegen, member ( self.htmem.ht.arrayAccess ( location ), "payload" ) )
                    self.payload.dematerialize ( probepayl, ctxt )
                    emit ( assign ( filterMatch, intConst (1) ), ctxt.codegen )
                    Hash.checkEquality ( filterMatch, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
                    if self.algExpr.conditions is not None:
                        emit ( assignAnd ( filterMatch, self.algExpr.conditions.translate ( ctxt ) ), ctxt.codegen )

            if self.algExpr.joinType == Join.SEMI:
                emit ( assignAnd ( ctxt.vars.activeVar, filterMatch ), ctxt.codegen )
            if self.algExpr.joinType == Join.ANTI:
                emit ( assignAnd ( ctxt.vars.activeVar, notLogic ( filterMatch ) ), ctxt.codegen )
        
        self.parent.consume ( ctxt )

    
    def htInsertMultiMatch ( self, ctxt ):

        # execute only when current thread has active elements    
        with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):  
            
            # compute a (possibly) non-unique hash over all join attributes
            hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
            with IfClause ( ctxt.vars.activeVar, ctxt.codegen ):
                Hash.attributes ( self.algExpr.buildKeyAttributes, hashVar, ctxt )
            
            htRangeOffset  = Variable.val ( CType.INT, "offs" + str ( self.algExpr.opId ) )
            ctxt.codegen.gpumem.local ( htRangeOffset, intConst(0) )

            scanCall = KernelCall.library ( "scanMultiHT", [ self.htmem.ht.getGPU(), self.htmem.numEntries, htRangeOffset.getGPU() ] ) 
            ctxt.codegen.kernelCalls.append ( scanCall ) 
            
            ctxt.codegen.openMirrorKernel ( "_ins" )
            
            emit ( call ( qlib.Fct.HASH_COUNT_MULTI, 
                [self.htmem.ht, self.htmem.numEntries, hashVar] ), ctxt.codegen.currentKernel )
            
            ctxt.codegen.mirrorKernel.addVar ( htRangeOffset )

            payl = self.payload.materialize ( "payl", ctxt.codegen.mirrorKernel, ctxt )
        
            emit ( call ( qlib.Fct.HASH_INSERT_MULTI, 
                [self.htmem.ht, self.htmem.payload, htRangeOffset, self.htmem.numEntries, hashVar, addressof(payl) ] ), ctxt.codegen.mirrorKernel )
    
            
    
    def htProbeMultiMatchSemiAnti ( self, ctxt ):
        self.endVar = Variable.val ( CType.INT, "matchEnd" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        self.offsetVar = Variable.val ( CType.INT, "matchOffset" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        self.matchStepVar = Variable.val ( CType.INT, "matchStep" + str ( self.algExpr.opId ), ctxt.codegen, intConst(1) )
        filterMatch = Variable.val ( CType.INT, "filterMatch" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        probeActive = Variable.val ( CType.INT, "probeActive" + str ( self.algExpr.opId ), ctxt.codegen, ctxt.vars.activeVar )
            
        hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        with IfClause ( probeActive, ctxt.codegen ):
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )
            emit ( assign ( probeActive, call( qlib.Fct.HASH_PROBE_MULTI, 
                [self.htmem.ht, self.htmem.numEntries, hashVar, self.offsetVar, self.endVar] ) ), ctxt.codegen )
        
        with WhileLoop ( probeActive, ctxt.codegen ):
            payl = Variable.val ( self.htmem.payload.dataType, "payl", ctxt.codegen ) 
            emit ( assign ( payl, self.htmem.payload.arrayAccess ( self.offsetVar ) ), ctxt.codegen )
            self.payload.dematerialize ( payl, ctxt )
            
            emit ( assign ( filterMatch, intConst (1) ), ctxt.codegen )
            Hash.checkEquality ( filterMatch, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
            if self.algExpr.conditions is not None:
                emit ( assignAnd ( filterMatch, self.algExpr.conditions.translate ( ctxt ) ), ctxt.codegen )

            emit ( assignAdd ( self.offsetVar, self.matchStepVar ), ctxt.codegen )        
            emit ( assignAnd ( probeActive, notLogic ( filterMatch ) ), ctxt.codegen )
            emit ( assignAnd ( probeActive, smaller ( self.offsetVar, self.endVar ) ), ctxt.codegen )

        if self.algExpr.joinType == Join.SEMI:
            emit ( assignAnd ( ctxt.vars.activeVar, filterMatch ), ctxt.codegen )
        if self.algExpr.joinType == Join.ANTI:
            emit ( assignAnd ( ctxt.vars.activeVar, notLogic ( filterMatch ) ), ctxt.codegen )
        
        self.parent.consume ( ctxt )
    
    
    def htProbeMultiMatchSingleBroadcastSemiAnti ( self, ctxt ):
        commentOperator ("semi/anti multiprobe single broadcast", ctxt.codegen)
        endVar = Variable.val ( CType.INT, "matchEnd" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        endVarBuf = Variable.val ( CType.INT, "matchEndBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVar = Variable.val ( CType.INT, "matchOffset" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVarBuf = Variable.val ( CType.INT, "matchOffsetBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        filterMatch = Variable.val ( CType.INT, "filterMatch" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        probeActive = Variable.val ( CType.INT, "probeActive" + str ( self.algExpr.opId ), ctxt.codegen, ctxt.vars.activeVar )
            
        bufferAtts = dict()
        bufferAtts.update ( self.algExpr.probeKeyAttributes )
        bufferAtts.update ( self.algExpr.conditionProbeAttributes )
        bufferVars = []
        for id, att in bufferAtts.items():
            var = ctxt.attFile.regFile [ id ]
            bufVar = copy.deepcopy ( var )
            bufVar.name = bufVar.name + "_bcbuf" + str ( self.algExpr.opId )
            bufVar.declare ( ctxt.codegen )
            bufferVars.append ( (var, bufVar ) )
 
        hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        with IfClause ( probeActive, ctxt.codegen ):
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )
            emit ( assign ( probeActive, call( qlib.Fct.HASH_PROBE_MULTI, 
                [self.htmem.ht, self.htmem.numEntries, hashVar, offsetVarBuf, endVarBuf] ) ), ctxt.codegen )

        activeProbes = Variable.val ( CType.UINT, "activeProbes" + str ( self.algExpr.opId ) )
        activeProbes.declareAssign ( ballotIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen )

        # write register state to buffer to prevent overwriting 
        for var, bufVar in bufferVars:
            emit ( assign ( bufVar, var ), ctxt.codegen )      
 
        with WhileLoop ( larger ( activeProbes, intConst(0) ), ctxt.codegen ):
            tupleLane = Variable.val ( CType.UINT, "tupleLane", ctxt.codegen )
            emit ( assign ( tupleLane, sub ( ffsIntr ( activeProbes ), 1 ) ), ctxt.codegen ) 
            # shuffle gather offset
            emit ( assign ( offsetVar, add ( shuffleIntr ( qlib.Const.ALL_LANES, offsetVarBuf, tupleLane ), ctxt.codegen.warplane() ) ), ctxt.codegen )
            # shuffle gather end
            emit ( assign ( endVar, shuffleIntr ( qlib.Const.ALL_LANES, endVarBuf, tupleLane ) ), ctxt.codegen )      
            # shuffle other register vars
            for var, bufVar in bufferVars:
                emit ( assign ( var, shuffleIntr ( qlib.Const.ALL_LANES, bufVar, tupleLane ) ), ctxt.codegen )      
            # mark lane as processed
            emit ( assignSub ( activeProbes, ( shiftLeft ( intConst(1), tupleLane ) ) ), ctxt.codegen ) 

            emit ( assign ( filterMatch, intConst (0) ), ctxt.codegen )
            emit ( assign ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )
            with WhileLoop ( anyIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen ):
                with IfClause ( probeActive, ctxt.codegen ):
                    payl = Variable.val ( self.htmem.payload.dataType, "payl", ctxt.codegen ) 
                    emit ( assign ( payl, self.htmem.payload.arrayAccess ( offsetVar ) ), ctxt.codegen )
                    self.payload.dematerialize ( payl, ctxt )
                    emit ( assign ( filterMatch, intConst (1) ), ctxt.codegen )
                    Hash.checkEquality ( filterMatch, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
                    if self.algExpr.conditions is not None:
                        emit ( assignAnd ( filterMatch, self.algExpr.conditions.translate ( ctxt ) ), ctxt.codegen )
                emit ( assign ( filterMatch, anyIntr ( qlib.Const.ALL_LANES, filterMatch ) ), ctxt.codegen )
                emit ( assignAnd ( probeActive, notLogic ( filterMatch ) ), ctxt.codegen )
                emit ( assignAdd ( offsetVar, intConst(32) ), ctxt.codegen )        
                emit ( assignAnd ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )

            with IfClause ( equals ( ctxt.codegen.warplane(), tupleLane ), ctxt.codegen ):
                if self.algExpr.joinType == Join.SEMI:
                    emit ( assignAnd ( ctxt.vars.activeVar, filterMatch ), ctxt.codegen )
                if self.algExpr.joinType == Join.ANTI:
                    emit ( assignAnd ( ctxt.vars.activeVar, notLogic ( filterMatch ) ), ctxt.codegen )
        
        # write register state to buffer to prevent overwriting 
        for var, bufVar in bufferVars:
            emit ( assign ( var, bufVar ), ctxt.codegen )      

        self.parent.consume ( ctxt )

    
    def htProbeMultiMatchMultiBroadcastSemiAnti ( self, ctxt ):
        commentOperator ("semi/anti multiprobe multi broadcast", ctxt.codegen)
        endVar = Variable.val ( CType.INT, "matchEnd" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        endVarBuf = Variable.val ( CType.INT, "matchEndBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVar = Variable.val ( CType.INT, "matchOffset" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVarBuf = Variable.val ( CType.INT, "matchOffsetBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        filterMatch = Variable.val ( CType.INT, "filterMatch" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        probeActive = Variable.val ( CType.INT, "probeActive" + str ( self.algExpr.opId ), ctxt.codegen, ctxt.vars.activeVar )
            
        bufferAtts = dict()
        bufferAtts.update ( self.algExpr.probeKeyAttributes )
        bufferAtts.update ( self.algExpr.conditionProbeAttributes )
        bufferVars = []
        for id, att in bufferAtts.items():
            var = ctxt.attFile.regFile [ id ]
            bufVar = copy.deepcopy ( var )
            bufVar.name = bufVar.name + "_bc_buf" + str ( self.algExpr.opId )
            bufVar.declare ( ctxt.codegen )
            bufferVars.append ( (var, bufVar ) )
 
        hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        with IfClause ( probeActive, ctxt.codegen ):
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )
            emit ( assign ( probeActive, call( qlib.Fct.HASH_PROBE_MULTI, 
                [self.htmem.ht, self.htmem.numEntries, hashVar, offsetVarBuf, endVarBuf] ) ), ctxt.codegen )

        activeProbes = Variable.val ( CType.UINT, "activeProbes" + str ( self.algExpr.opId ) )
        activeProbes.declareAssign ( ballotIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen )
        # number of tuples in each buffered match
        numbuf = Variable.val ( CType.INT, "num" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        emit ( assign ( numbuf, sub ( endVarBuf, offsetVarBuf ) ), ctxt.codegen )      
        wideProbes = Variable.val ( CType.UINT, "wideProbes"  + str ( self.algExpr.opId ))
        wideProbes.declareAssign ( ballotIntr ( qlib.Const.ALL_LANES, largerEqual ( numbuf, intConst(32) ) ), ctxt.codegen )

        # write register state to buffer to prevent overwriting 
        for var, bufVar in bufferVars:
            emit ( assign ( bufVar, var ), ctxt.codegen )      
 
        with WhileLoop ( larger ( activeProbes, intConst(0) ), ctxt.codegen ):
            tupleLane = Variable.val ( CType.UINT, "tupleLane", ctxt.codegen )
            broadcastLane = Variable.val ( CType.UINT, "broadcastLane", ctxt.codegen )
            numFilled = Variable.val ( CType.INT, "numFilled", ctxt.codegen, intConst(0) )
            num = Variable.val ( CType.INT, "num", ctxt.codegen, intConst(0) )
            siblingsMask = Variable.val ( CType.UINT, "siblingsMask", ctxt.codegen )
            firstBroadcastDest = Variable.val ( CType.INT, "firstBroadcastDest", ctxt.codegen, intConst(-1) )
            
            with WhileLoop ( andLogic ( smaller ( numFilled, intConst(32) ), activeProbes ), ctxt.codegen ) as l: 
                # select leader
                with IfClause ( larger ( wideProbes, intConst (0) ), ctxt.codegen ):
                    emit ( assign ( tupleLane, sub ( ffsIntr ( wideProbes ), 1 ) ), ctxt.codegen ) 
                    emit ( assignSub ( wideProbes, ( shiftLeft ( intConst(1), tupleLane ) ) ), ctxt.codegen ) 
                with ElseClause ( ctxt.codegen ):
                    emit ( assign ( tupleLane, sub ( ffsIntr ( activeProbes ), 1 ) ), ctxt.codegen ) 
               
                # broadcast leader number of matches
                emit ( assign ( num, shuffleIntr ( qlib.Const.ALL_LANES, numbuf, tupleLane ) ), ctxt.codegen )
                with IfClause ( andLogic ( numFilled, larger ( add ( numFilled, num ), 32 ) ), ctxt.codegen ):
                    l.break_()
                
                with IfClause ( equals ( ctxt.codegen.warplane(), tupleLane ), ctxt.codegen ):
                    emit ( assign ( firstBroadcastDest, numFilled ), ctxt.codegen )
                with IfClause ( largerEqual ( ctxt.codegen.warplane(), numFilled ), ctxt.codegen ):
                    emit ( assign ( broadcastLane, tupleLane ), ctxt.codegen )
                    emit ( assign ( offsetVar, sub ( ctxt.codegen.warplane(), numFilled ) ), ctxt.codegen )      
                    emit ( assign ( siblingsMask, qlib.Const.ALL_LANES ), ctxt.codegen )
                    emit ( assignAnd ( siblingsMask, inverse ( shiftRight ( qlib.Const.ALL_LANES,  sub ( intConst(32), numFilled ) ) ) ), ctxt.codegen )
                    emit ( assignAnd ( siblingsMask, inverse ( shiftLeft ( qlib.Const.ALL_LANES, add ( numFilled, num ) ) ) ), ctxt.codegen )

                emit ( assignAdd ( numFilled, num ), ctxt.codegen )  
                # mark buffered probe tuple as processed            
                emit ( assignSub ( activeProbes, ( shiftLeft ( intConst(1), tupleLane ) ) ), ctxt.codegen ) 

            # shuffle gather offset
            emit ( assignAdd ( offsetVar, shuffleIntr ( qlib.Const.ALL_LANES, offsetVarBuf, broadcastLane ) ), ctxt.codegen )
            # shuffle gather end
            emit ( assign ( endVar, shuffleIntr ( qlib.Const.ALL_LANES, endVarBuf, broadcastLane ) ), ctxt.codegen )      
            # shuffle other register vars
            for var, bufVar in bufferVars:
                emit ( assign ( var, shuffleIntr ( qlib.Const.ALL_LANES, bufVar, broadcastLane ) ), ctxt.codegen )      

            emit ( assign ( filterMatch, intConst (0) ), ctxt.codegen )
            emit ( assign ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )
            with WhileLoop ( anyIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen ):
                with IfClause ( probeActive, ctxt.codegen ):
                    payl = Variable.val ( self.htmem.payload.dataType, "payl", ctxt.codegen ) 
                    emit ( assign ( payl, self.htmem.payload.arrayAccess ( offsetVar ) ), ctxt.codegen )
                    self.payload.dematerialize ( payl, ctxt )
                    emit ( assign ( filterMatch, intConst (1) ), ctxt.codegen )
                    Hash.checkEquality ( filterMatch, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
                    if self.algExpr.conditions is not None:
                        emit ( assignAnd ( filterMatch, self.algExpr.conditions.translate ( ctxt ) ), ctxt.codegen )
                emit ( assign ( filterMatch, larger ( andBitwise ( ballotIntr ( qlib.Const.ALL_LANES, filterMatch ), siblingsMask ), intConst(0) ) ), ctxt.codegen )  
                emit ( assignAnd ( probeActive, notLogic ( filterMatch ) ), ctxt.codegen )
                emit ( assignAdd ( offsetVar, intConst(32) ), ctxt.codegen )        
                emit ( assignAnd ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )
        
            emit ( assign ( filterMatch, shuffleIntr ( qlib.Const.ALL_LANES, filterMatch, firstBroadcastDest ) ), ctxt.codegen )      
            with IfClause ( largerEqual ( firstBroadcastDest, intConst(0) ), ctxt.codegen ):
                if self.algExpr.joinType == Join.SEMI:
                    emit ( assignAnd ( ctxt.vars.activeVar, filterMatch ), ctxt.codegen )
                if self.algExpr.joinType == Join.ANTI:
                    emit ( assignAnd ( ctxt.vars.activeVar, notLogic ( filterMatch ) ), ctxt.codegen )
            
        
        # write register state to buffer to prevent overwriting 
        for var, bufVar in bufferVars:
            emit ( assign ( var, bufVar ), ctxt.codegen )      

        self.parent.consume ( ctxt )


    def htProbeMultiMatch ( self, ctxt ):
        self.endVar = Variable.val ( CType.INT, "matchEnd" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        self.offsetVar = Variable.val ( CType.INT, "matchOffset" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        self.matchStepVar = Variable.val ( CType.INT, "matchStep" + str ( self.algExpr.opId ), ctxt.codegen, intConst(1) )
        matchFound = Variable.val ( CType.INT, "matchFound" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        probeActive = Variable.val ( CType.INT, "probeActive" + str ( self.algExpr.opId ), ctxt.codegen, ctxt.vars.activeVar )
        #ctxt.vars.buf.extend ( [ self.endVar, self.offsetVar, self.matchStepVar, matchFound, probeActive ] )
            
        if self.algExpr.joinType == Join.OUTER:
            doOuter = Variable.val ( CType.INT, "doOuter" + str ( self.algExpr.opId ), ctxt.codegen, intConst(1) )
            outerActive = Variable.val ( CType.INT, "outerActive" + str ( self.algExpr.opId ), ctxt.codegen, ctxt.vars.activeVar )
            for id, nullable in self.buildRelation.items():
                ctxt.attFile.isNullFile [ nullable.id ] = notLogic ( matchFound )
                
        # execute only when current thread has active elements    
        hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        with IfClause ( probeActive, ctxt.codegen ):
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )
            emit ( assign ( probeActive, call( qlib.Fct.HASH_PROBE_MULTI, 
                [self.htmem.ht, self.htmem.numEntries, hashVar, self.offsetVar, self.endVar] ) ), ctxt.codegen )
        emit ( assign ( ctxt.vars.activeVar, probeActive ), ctxt.codegen )
        
        
        self.probeWithDivergenceBuffer = False

        # -- start probe loop --
        if self.probeWithDivergenceBuffer:
            vars = [ self.offsetVar, self.endVar ]
            for att in self.algExpr.leftChild.outRelation:
                vars.append ( ctxt.attFile.regFile [ att ] )
            probeloop = BufferedLoop ( ctxt, vars, 0.8 )
            unrollDepth = self.unrollDepth
        else:
            probeloop = WhileLoop ( anyIntr ( qlib.Const.ALL_LANES, ctxt.vars.activeVar ) , ctxt.codegen )
            unrollDepth = 1

        ctxt.innerLoopCount += 1
        with UnrolledForLoop ( unrollDepth, ctxt.codegen): 
            
            emit ( assign ( probeActive, ctxt.vars.activeVar ), ctxt.codegen )

            payl = Variable.val ( self.htmem.payload.dataType, "payl", ctxt.codegen ) 
            with IfClause ( probeActive, ctxt.codegen ):
                emit ( assign ( payl, self.htmem.payload.arrayAccess ( self.offsetVar ) ), ctxt.codegen )
                self.payload.dematerialize ( payl, ctxt )
                Hash.checkEquality ( ctxt.vars.activeVar, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
                emit ( assignAdd ( matchFound, ctxt.vars.activeVar ), ctxt.codegen ) 

            self.parent.consume ( ctxt )

            # coalesced access for broadcast matches
            emit ( assignAdd ( self.offsetVar, self.matchStepVar ), ctxt.codegen )        
            
            # finish join matches
            emit ( assignAnd ( probeActive, smaller ( self.offsetVar, self.endVar ) ), ctxt.codegen )
            emit ( assign ( ctxt.vars.activeVar, probeActive ), ctxt.codegen )
        
            # handle nullable attributes for  outer join
            if self.algExpr.joinType == Join.OUTER:
                with IfClause ( andLogic ( notLogic ( anyIntr ( qlib.Const.ALL_LANES, probeActive ) ), doOuter ), ctxt.codegen ):
                    # remember null indicator for each attribute from build relation
                    with IfClause ( notLogic ( matchFound ), ctxt.codegen ):
                        emit ( assign ( ctxt.vars.activeVar, outerActive ), ctxt.codegen )
                    emit ( assign ( doOuter, intConst(0) ), ctxt.codegen )

        # -- close probe loop -- 
        probeloop.close()
        ctxt.innerLoopCount -= 1


    def htProbeMultiMatchSingleBroadcast ( self, ctxt ):
        commentOperator ("multiprobe single broadcast", ctxt.codegen)
        endVar = Variable.val ( CType.INT, "matchEnd" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        endVarBuf = Variable.val ( CType.INT, "matchEndBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVar = Variable.val ( CType.INT, "matchOffset" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVarBuf = Variable.val ( CType.INT, "matchOffsetBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        probeActive = Variable.val ( CType.INT, "probeActive" + str ( self.algExpr.opId ), ctxt.codegen, ctxt.vars.activeVar )

        bufferAtts = dict()
        bufferAtts.update ( self.algExpr.rightChild.outRelation )
        bufferAtts.update ( self.algExpr.conditionProbeAttributes )
        bufferVars = []
        for id, att in bufferAtts.items():
            var = ctxt.attFile.regFile [ id ]
            bufVar = copy.deepcopy ( var )
            bufVar.name = bufVar.name + "_bcbuf" + str ( self.algExpr.opId )
            bufVar.declare ( ctxt.codegen )
            bufferVars.append ( (var, bufVar ) )
 
        hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        with IfClause ( probeActive, ctxt.codegen ):
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )
            emit ( assign ( probeActive, call( qlib.Fct.HASH_PROBE_MULTI, 
                [self.htmem.ht, self.htmem.numEntries, hashVar, offsetVarBuf, endVarBuf] ) ), ctxt.codegen )

        activeProbes = Variable.val ( CType.UINT, "activeProbes" + str ( self.algExpr.opId ) )
        activeProbes.declareAssign ( ballotIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen )

        # write register state to buffer to prevent overwriting 
        for var, bufVar in bufferVars:
            emit ( assign ( bufVar, var ), ctxt.codegen )      

        with WhileLoop ( larger ( activeProbes, intConst(0) ), ctxt.codegen ):
            tupleLane = Variable.val ( CType.UINT, "tupleLane", ctxt.codegen )
            emit ( assign ( tupleLane, sub ( ffsIntr ( activeProbes ), 1 ) ), ctxt.codegen ) 
            # shuffle gather offset
            emit ( assign ( offsetVar, add ( shuffleIntr ( qlib.Const.ALL_LANES, offsetVarBuf, tupleLane ), ctxt.codegen.warplane() ) ), ctxt.codegen )
            # shuffle gather end
            emit ( assign ( endVar, shuffleIntr ( qlib.Const.ALL_LANES, endVarBuf, tupleLane ) ), ctxt.codegen )      
            # shuffle other register vars
            for var, bufVar in bufferVars:
                emit ( assign ( var, shuffleIntr ( qlib.Const.ALL_LANES, bufVar, tupleLane ) ), ctxt.codegen )      
            # mark lane as processed
            emit ( assignSub ( activeProbes, ( shiftLeft ( intConst(1), tupleLane ) ) ), ctxt.codegen ) 

            emit ( assign ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )
            ctxt.innerLoopCount += 1
            with WhileLoop ( anyIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen ):
                emit ( assign ( ctxt.vars.activeVar, intConst(0) ), ctxt.codegen )
                payl = Variable.val ( self.htmem.payload.dataType, "payl", ctxt.codegen ) 
                with IfClause ( probeActive, ctxt.codegen ):
                    emit ( assign ( payl, self.htmem.payload.arrayAccess ( offsetVar ) ), ctxt.codegen )
                    self.payload.dematerialize ( payl, ctxt )
                    emit ( assign ( ctxt.vars.activeVar, intConst(1) ), ctxt.codegen )
                    Hash.checkEquality ( ctxt.vars.activeVar, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
                    emit ( assignAdd ( offsetVar, intConst(32) ), ctxt.codegen )        
                    emit ( assignAnd ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )
                self.parent.consume ( ctxt )

            ctxt.innerLoopCount -= 1


    def htProbeMultiMatchMultiBroadcast ( self, ctxt ):
        commentOperator ("multiprobe multi broadcast", ctxt.codegen)
        endVar = Variable.val ( CType.INT, "matchEnd" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        endVarBuf = Variable.val ( CType.INT, "matchEndBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVar = Variable.val ( CType.INT, "matchOffset" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        offsetVarBuf = Variable.val ( CType.INT, "matchOffsetBuf" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        probeActive = Variable.val ( CType.INT, "probeActive" + str ( self.algExpr.opId ), ctxt.codegen, ctxt.vars.activeVar )

        bufferAtts = dict()
        bufferAtts.update ( self.algExpr.rightChild.outRelation )
        bufferAtts.update ( self.algExpr.conditionProbeAttributes )
        bufferVars = []
        for id, att in bufferAtts.items():
            var = ctxt.attFile.regFile [ id ]
            bufVar = copy.deepcopy ( var )
            bufVar.name = bufVar.name + "_bcbuf" + str ( self.algExpr.opId )
            bufVar.declare ( ctxt.codegen )
            bufferVars.append ( (var, bufVar ) )
 
        hashVar = Variable.val ( CType.UINT64, "hash" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        with IfClause ( probeActive, ctxt.codegen ):
            Hash.attributes ( self.algExpr.probeKeyAttributes, hashVar, ctxt )
            emit ( assign ( probeActive, call( qlib.Fct.HASH_PROBE_MULTI, 
                [self.htmem.ht, self.htmem.numEntries, hashVar, offsetVarBuf, endVarBuf] ) ), ctxt.codegen )

        activeProbes = Variable.val ( CType.UINT, "activeProbes" + str ( self.algExpr.opId ) )
        activeProbes.declareAssign ( ballotIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen )
        numbuf = Variable.val ( CType.INT, "num" + str ( self.algExpr.opId ), ctxt.codegen, intConst(0) )
        emit ( assign ( numbuf, sub ( endVarBuf, offsetVarBuf ) ), ctxt.codegen )      
        wideProbes = Variable.val ( CType.UINT, "wideProbes"  + str ( self.algExpr.opId ))
        wideProbes.declareAssign ( ballotIntr ( qlib.Const.ALL_LANES, largerEqual ( numbuf, intConst(32) ) ), ctxt.codegen )

        # write register state to buffer to prevent overwriting 
        for var, bufVar in bufferVars:
            emit ( assign ( bufVar, var ), ctxt.codegen )      
        
        with WhileLoop ( larger ( activeProbes, intConst(0) ), ctxt.codegen ):
            tupleLane = Variable.val ( CType.UINT, "tupleLane", ctxt.codegen )
            broadcastLane = Variable.val ( CType.UINT, "broadcastLane", ctxt.codegen )
            numFilled = Variable.val ( CType.INT, "numFilled", ctxt.codegen, intConst(0) )
            num = Variable.val ( CType.INT, "num", ctxt.codegen, intConst(0) )
            with WhileLoop ( andLogic ( smaller ( numFilled, intConst(32) ), activeProbes ), ctxt.codegen ) as l: 
                # select leader
                with IfClause ( larger ( wideProbes, intConst (0) ), ctxt.codegen ):
                    emit ( assign ( tupleLane, sub ( ffsIntr ( wideProbes ), 1 ) ), ctxt.codegen ) 
                    emit ( assignSub ( wideProbes, ( shiftLeft ( intConst(1), tupleLane ) ) ), ctxt.codegen ) 
                with ElseClause ( ctxt.codegen ):
                    emit ( assign ( tupleLane, sub ( ffsIntr ( activeProbes ), 1 ) ), ctxt.codegen ) 
                # broadcast leader number of matches
                emit ( assign ( num, shuffleIntr ( qlib.Const.ALL_LANES, numbuf, tupleLane ) ), ctxt.codegen )
                with IfClause ( andLogic ( numFilled, larger ( add ( numFilled, num ), 32 ) ), ctxt.codegen ):
                    l.break_()
                with IfClause ( largerEqual ( ctxt.codegen.warplane(), numFilled ), ctxt.codegen ):
                    emit ( assign ( broadcastLane, tupleLane ), ctxt.codegen )
                    emit ( assign ( offsetVar, sub ( ctxt.codegen.warplane(), numFilled ) ), ctxt.codegen )      
                emit ( assignAdd ( numFilled, num ), ctxt.codegen )  
                # mark buffered probe tuple as processed            
                emit ( assignSub ( activeProbes, ( shiftLeft ( intConst(1), tupleLane ) ) ), ctxt.codegen ) 

            # shuffle gather offset
            emit ( assignAdd ( offsetVar, shuffleIntr ( qlib.Const.ALL_LANES, offsetVarBuf, broadcastLane ) ), ctxt.codegen )
            # shuffle gather end
            emit ( assign ( endVar, shuffleIntr ( qlib.Const.ALL_LANES, endVarBuf, broadcastLane ) ), ctxt.codegen )      
            # shuffle other register vars
            for var, bufVar in bufferVars:
                emit ( assign ( var, shuffleIntr ( qlib.Const.ALL_LANES, bufVar, broadcastLane ) ), ctxt.codegen )      

            emit ( assign ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )
            ctxt.innerLoopCount += 1
            with WhileLoop ( anyIntr ( qlib.Const.ALL_LANES, probeActive ), ctxt.codegen ):
                emit ( assign ( ctxt.vars.activeVar, intConst(0) ), ctxt.codegen )
                payl = Variable.val ( self.htmem.payload.dataType, "payl", ctxt.codegen ) 
                with IfClause ( probeActive, ctxt.codegen ):
                    emit ( assign ( payl, self.htmem.payload.arrayAccess ( offsetVar ) ), ctxt.codegen )
                    self.payload.dematerialize ( payl, ctxt )
                    emit ( assign ( ctxt.vars.activeVar, intConst(1) ), ctxt.codegen )
                    Hash.checkEquality ( ctxt.vars.activeVar, self.algExpr.buildKeyAttributes, self.algExpr.probeKeyAttributes, ctxt )
                    emit ( assignAdd ( offsetVar, intConst(32) ), ctxt.codegen )        
                    emit ( assignAnd ( probeActive, smaller ( offsetVar, endVar ) ), ctxt.codegen )
                self.parent.consume ( ctxt )
            ctxt.innerLoopCount -= 1




