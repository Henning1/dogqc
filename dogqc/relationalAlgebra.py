import sys
from enum import Enum
from collections import OrderedDict
from dogqc.scalarAlgebra import ScalarExpr
from dogqc.types import Type
from dogqc.util import listWrap, intToHuman, formatDOTStr

class Attribute ( object ):

    def __init__ ( self, name, id, opId ):
        self.name = name
        self.id = id
        self.creation = opId
        self.numElements = None
        self.lastUse = 0
        self.dataType = None
        self.identifiers = [ name ]


class Context ( object ):

    def __init__ ( self, database ):
        self.database = database
        self.schema = dict ( database.schema )
        
        self.opIdNum = 0
        self.attributeIdNum = 0
        self.tableVersions = dict()

    def opId ( self ): 
        self.opIdNum += 1
        return self.opIdNum

    def createTable ( self, identifier ):
        table = dict ()
        table["name"] = identifier
        table["attributes"] = OrderedDict()
        self.schema [ identifier ] = table
        return table

    def resolveTable ( self, identifier ):
        table = self.schema [ identifier ]
        id = 0
        if identifier in self.tableVersions:
            id = self.tableVersions [ identifier ]
        id += 1
        self.tableVersions [ identifier ] = id
        return ( id, table )

    def createAttribute ( self, attributeName, tableName=None ):
        self.attributeIdNum += 1
        id = self.attributeIdNum
        attr = Attribute ( attributeName, id, self.opIdNum ) 
        return ( id, attr )
   

class RelationalAlgebra ( object ):

    def __init__ ( self, acc ):
        self.ctxt = Context ( acc )
        # keep track of sets in list
        self.selectionNodes = []
        self.joinNodes = []
        self.semiJoinNodes = []
        self.antiJoinNodes = []

    def scan ( self, table, alias=None ):         
        return Scan ( self.ctxt, table, alias )
 
    def selection ( self, condition, child ):
        node = Selection ( self.ctxt, condition, child )       
        self.selectionNodes.append ( node )
        return node
 
    def projection ( self, attributes, child ):
        return Projection ( self.ctxt, attributes, child )       
    
    def map ( self, attrName, expression, child ):
        return Map ( self.ctxt, attrName, expression, child )
    
    def createtemp ( self, identifier, child ):
        return Materialize.temp ( self.ctxt, identifier, child )
    
    def result ( self, child ):
        return Materialize.result ( self.ctxt, child )
 
    def join ( self, condition, leftChild, rightChild ):
        return self.innerjoin ( condition, None, leftChild, rightChild )
    
    def crossjoin ( self, condition, leftChild, rightChild ):
        # conditions type ScalarExpr
        return CrossJoin ( self.ctxt, Join.CROSS, condition, leftChild, rightChild )

    def innerjoin ( self, equalityConditions, otherConditions, leftChild, rightChild ):
        # condition type list of identifier tuples (equalities)
        node = EquiJoin ( self.ctxt, Join.INNER, equalityConditions, otherConditions, leftChild, rightChild )
        self.joinNodes.append ( node )
        return node
    
    def semijoin ( self, equalityConditions, otherConditions, leftChild, rightChild ):
        node = EquiJoin ( self.ctxt, Join.SEMI, equalityConditions, otherConditions, leftChild, rightChild )
        self.semiJoinNodes.append ( node )
        return node

    def antijoin ( self, equalityConditions, otherConditions, leftChild, rightChild ):
        node = EquiJoin ( self.ctxt, Join.ANTI, equalityConditions, otherConditions, leftChild, rightChild )
        self.antiJoinNodes.append ( node )
        return node
 
    def outerjoin ( self, equalityConditions, otherConditions, leftChild, rightChild ):
        return EquiJoin ( self.ctxt, Join.OUTER, equalityConditions, otherConditions, leftChild, rightChild )

    def aggregation ( self, groupAttributes, aggregates, child ):
        return Aggregation ( self.ctxt, groupAttributes, aggregates, child )
    
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
        print ( graph )
        graph.view()
    
    def resolveAlgebraPlan ( self, plan, cfg ):
        plan = listWrap ( plan )
        # add query result as root
        plan [ len ( plan ) - 1 ] = self.result ( plan [ len ( plan ) - 1 ] ) 
        translationPlan = list()
        for node in plan:
            node.resolve ( self.ctxt )
            attr = node.prune ()
            num = node.configure ( cfg, self.ctxt )
        return plan

    def translateToCompilerPlan ( self, plan, translator ):
        translationPlan = list()
        for node in plan:
            translationPlan.append ( node.translate ( translator, self.ctxt ) )
        return translationPlan



class AlgExpr ( object ):
   
    # create attributes
    def __init__ ( self, ctxt ):
        self.opId = ctxt.opId ()
        self.inRelation = dict()
        self.outRelation = dict()
        
 
    # resolve attributes and determine inRelation and outRelation
    def resolve ( self, ctxt ):
        pass
    
    # prune outRelation
    def prune ( self ):
        pass
    
    # set tupleNum
    def configure ( self, cfg, ctxt ):
        pass
    
    # translates relational operator to language
    def translate ( self, translator, ctxt ):
        pass

    def touchAttribute ( self, id ):
        self.inRelation [ id ].lastUse = self.opId

    def resolveAttribute ( self, identifier ):
        idents, ambigs = self.buildIdentifiers ( )
        if identifier in ambigs:
            raise SyntaxError ( "Identifier " + identifier + " is ambiguous in operator " +
                self.DOTcaption() + " (" + str ( self.opId ) + ")" )
        if identifier not in idents:
            raise NameError ( "Attribute " + identifier + " not found in " + 
                self.DOTcaption() + " operator (" + str ( self.opId ) + ")" )
        id = idents [ identifier ]
        self.touchAttribute ( id )
        return self.inRelation [ id ]

    def buildIdentifiers ( self ):
        identifiers = dict()
        ambiguousIdentifiers = dict()
        for id, att in self.inRelation.items():
            for ident in att.identifiers:
                if ident in identifiers:
                    ambiguousIdentifiers [ ident ] = True
                else:
                    identifiers [ ident ] = id
        return ( identifiers, ambiguousIdentifiers )
    
    def pruneOutRelation ( self ):
        prunedAtts = dict()
        for id, attr in self.outRelation.items():
            if attr.lastUse > self.opId:
                prunedAtts[id] = attr
        self.outRelation = prunedAtts

    # ---- plan visualization ---- 
    def toDOT ( self, graph ):
        cap = self.DOTcaption() + " (" + str ( self.opId ) + ")"
        sub = ""
        if len ( self.outRelation ) > 0:
            sub = self.DOTsubcaption() 
        dstr = formatDOTStr ( cap, sub )
        graph.node ( str(self.opId), dstr )
        #self.DOTscalarExpr( graph )

    def DOTcaption ( self ):
        return self.__class__.__name__
    
    def DOTsubcaption ( self ):
        return ""
    
    def DOTscalarExpr ( self, graph ):
        pass
    
    def edgeDOTstr ( self ):
        if len ( self.outRelation ) == 0:
            return ""
        labelList = []
        #labelList = list ( map ( lambda x: x.name + " (" + str(x.id) + ")", self.outRelation.values() ) )
        labelList.append ( """<FONT POINT-SIZE="10"><b>""" + intToHuman ( self.tupleNum ) + "</b></FONT>" )
        res = formatDOTStr ( None, labelList )
        return res
    

class LiteralAlgExpr ( AlgExpr ):
    
    def __init__ ( self, ctxt ):
        AlgExpr.__init__ ( self, ctxt )

    def prune ( self ):
        self.pruneOutRelation()
    
    def translate ( self, translator, ctxt ):
        return translator.translate ( self )


class UnaryAlgExpr ( AlgExpr ):
    
    def __init__ ( self, ctxt, child ):
        AlgExpr.__init__ ( self, ctxt )
        self.child = child
    
    def prune ( self ):
        self.child.prune()
        self.pruneOutRelation()
    
    def translate ( self, translator, ctxt ):
        inObj = self.child.translate ( translator, ctxt )
        return translator.translate ( self, inObj )
    
    def toDOT ( self, graph ):
        self.child.toDOT ( graph )
        AlgExpr.toDOT ( self, graph )
        graph.edge ( str ( self.child.opId ), str ( self.opId ), self.child.edgeDOTstr() )


class BinaryAlgExpr ( AlgExpr ):
    
    def __init__ ( self, ctxt, leftChild, rightChild ):
        AlgExpr.__init__ ( self, ctxt )
        self.leftChild = leftChild
        self.rightChild = rightChild
    
    def prune ( self ):
        self.leftChild.prune()
        self.rightChild.prune()
        self.pruneOutRelation()
    
    def translate ( self, translator, ctxt ):
        inObjLeft = self.leftChild.translate ( translator, ctxt )
        inObjRight = self.rightChild.translate ( translator, ctxt )
        return translator.translate ( self, inObjLeft, inObjRight )
    
    def toDOT ( self, graph ):
        self.leftChild.toDOT ( graph )
        self.rightChild.toDOT ( graph )
        AlgExpr.toDOT ( self, graph )
        graph.edge ( str ( self.leftChild.opId ), str ( self.opId ), self.leftChild.edgeDOTstr() )
        graph.edge ( str ( self.rightChild.opId ), str ( self.opId ), self.rightChild.edgeDOTstr() )


class Scan ( LiteralAlgExpr ):

    def __init__ ( self, ctxt, tableName, alias=None ):
        LiteralAlgExpr.__init__ ( self, ctxt )
        self.scanAttributes = dict()
        self.scanTableId, self.table = ctxt.resolveTable ( tableName )
        self.isTempScan = ( "isTempTable" in self.table )
        self.tableAlias = alias
    
    def resolve ( self, ctxt ):
        for (attrName, type) in self.table [ "attributes" ].items():
            id, attr = ctxt.createAttribute ( attrName, self.table["name"] )
            attr.dataType = type
            attr.numElements = self.table [ "size" ]
            attr.identifiers.append ( self.table["name"] + "." + attrName )
            if self.tableAlias != None:
                attr.identifiers.append ( self.tableAlias + "." + attrName )
            if self.isTempScan:
                attr.sourceId = self.table [ "sourceAttributes" ] [ attrName ]
            self.scanAttributes [ id ] = attr

        self.outRelation = self.scanAttributes
        return self.outRelation
    
    def configure ( self, cfg, ctxt ):
        self.tupleNum = self.table["size"]
        return self.tupleNum
    
    def DOTcaption ( self ):
        if self.isTempScan:
            return "Tempscan"
        else:
            return "Scan"

    def DOTsubcaption ( self ):
        return self.table["name"] + " (" + str ( self.scanTableId ) + ")"


class Selection ( UnaryAlgExpr ):

    def __init__ ( self, ctxt, condition, child ):
        UnaryAlgExpr.__init__ ( self, ctxt, child )
        self.condition = condition
    
    def resolve ( self, ctxt ):
        self.inRelation = self.child.resolve ( ctxt )
        self.conditionAttributes = self.condition.resolve ( self )
        self.outRelation = self.inRelation
        return self.outRelation
    
    def configure ( self, cfg, ctxt ):
        childTupleNum = self.child.configure ( cfg, ctxt )
        try:
            self.selectivity = cfg[self.opId]["selectivity"]
        except:
            self.selectivity = 1.0
        self.tupleNum = int ( childTupleNum * self.selectivity )
        return self.tupleNum

    def DOTscalarExpr ( self, graph ):
        self.condition.toDOT ( graph )
        graph.edge ( str ( self.condition.exprId ) , str ( self.opId ) )


class Map ( UnaryAlgExpr ):

    def __init__ ( self, ctxt, attrName, expression, child ):
        UnaryAlgExpr.__init__ ( self, ctxt, child )
        self.expression = expression
        self.attrName = attrName
    
    def resolve ( self, ctxt ):
        self.inRelation = self.child.resolve ( ctxt )
        mappedAttributes = self.expression.resolve ( self )
        id, attr = ctxt.createAttribute ( self.attrName )
        self.mapAttr = attr
        self.mapAttr.dataType = self.expression.type
        self.mapStringAttributes = [ att for (id, att) in mappedAttributes.items() if att.dataType == Type.STRING ]
        res = dict ( self.inRelation ) 
        res [ self.mapAttr.id ] = self.mapAttr
        self.outRelation = res
        return res
    
    def configure ( self, cfg, ctxt ):
        childTupleNum = self.child.configure ( cfg, ctxt )
        self.tupleNum = childTupleNum
        return self.tupleNum

    def DOTscalarExpr ( self, graph ):
        self.expression.toDOT ( graph )
        graph.edge ( str ( self.expression.exprId ) , str ( self.opId ) )


class Join ( Enum ):
    # handled by CrossJoin
    CROSS = 1
    # handled by EquiJoin
    INNER = 2
    SEMI  = 3
    ANTI  = 4
    OUTER = 5


class EquiJoin  ( BinaryAlgExpr ):

    def __init__ ( self, ctxt, joinType, equalityConditions, otherConditions, leftChild, rightChild ):
        BinaryAlgExpr.__init__ ( self, ctxt, leftChild, rightChild )
        self.equalities = listWrap ( equalityConditions )
        self.conditions = otherConditions
        self.joinType = joinType
    
    def prune ( self ):
        super().prune ( )

    def resolve ( self, ctxt ): 
        inLeft = self.leftChild.resolve ( ctxt )
        self.inRelation.update ( inLeft )
        inRight = self.rightChild.resolve ( ctxt )
        self.inRelation.update ( inRight )

        conditionAttributes = dict()
        if self.conditions != None:
            conditionAttributes = self.conditions.resolve ( self )
        self.conditionProbeAttributes = dict()
        for id, att in conditionAttributes.items():
            if id in inRight:
                print ( "from right: " + str(id) )
                self.conditionProbeAttributes[id] = att
        
        # determine source relation for condition attributes
        self.buildKeyAttributes = OrderedDict()
        self.probeKeyAttributes = OrderedDict()
        for ( ident1, ident2 ) in self.equalities:
            att1 = self.resolveAttribute ( ident1 )
            att2 = self.resolveAttribute ( ident2 )
            if att1.id in inLeft and att2.id in inRight:
                self.buildKeyAttributes[att1.id] = att1
                self.probeKeyAttributes[att2.id] = att2
            elif att1.id in inRight and att2.id in inLeft:
                self.buildKeyAttributes[att2.id] = att2
                self.probeKeyAttributes[att1.id] = att1
            else:
                raise NameError ( "Attributes " + att1.name + " and " + att2.name + "come from the same relation." )
        if self.joinType in [ Join.INNER, Join.OUTER ]:
            self.outRelation = self.inRelation
        else:
            self.outRelation = inRight
        return self.outRelation

    def configure ( self, cfg, ctxt ):
        leftChildTupleNum = self.leftChild.configure ( cfg, ctxt )
        rightChildTupleNum = self.rightChild.configure ( cfg, ctxt )
        try:
            self.multimatch = cfg[self.opId]["multimatch"]
        except:
            self.multimatch = True
        try:
            self.selectivity = cfg[self.opId]["selectivity"]
        except:
            self.selectivity = 1.0
        try:
            self.htSizeFactor = cfg[self.opId]["htSizeFactor"]
        except:
            self.htSizeFactor = 2.0
    
        self.tupleNum = int ( max ( leftChildTupleNum, rightChildTupleNum ) * self.selectivity )
        return self.tupleNum
    
    def DOTcaption ( self ):
        joinStr = str ( self.joinType )
        # indicate that join has more non-equality conditions
        if self.conditions != None:
            joinStr += '*'
        return joinStr[0] + joinStr[1:].lower()

    def DOTsubcaption ( self ):
        return list ( map ( lambda x,y: x[1].name + "=" + y[1].name, self.buildKeyAttributes.items(), 
            self.probeKeyAttributes.items() ) ) 


class CrossJoin ( BinaryAlgExpr ):

    def __init__ ( self, ctxt, joinType, condition, leftChild, rightChild ):
        BinaryAlgExpr.__init__ ( self, ctxt, leftChild, rightChild )
        self.condition = condition
        self.joinType = joinType

    def resolve ( self, ctxt ):
        inLeft = self.leftChild.resolve ( ctxt )
        self.inRelation.update ( inLeft )
        inRight = self.rightChild.resolve ( ctxt )
        self.inRelation.update ( inRight )
        self.conditionAttributes = self.condition.resolve ( self )
        self.outRelation = self.inRelation
        return self.outRelation

    def configure ( self, cfg, ctxt ):
        leftChildTupleNum = self.leftChild.configure ( cfg, ctxt )
        rightChildTupleNum = self.rightChild.configure ( cfg, ctxt )
        try:
            self.multimatch = cfg[self.opId]["multimatch"]
        except:
            self.multimatch = True
        try:
            self.selectivity = cfg[self.opId]["selectivity"]
        except:
            self.selectivity = 1.0
        self.tupleNum = int ( leftChildTupleNum * rightChildTupleNum * self.selectivity )
        return self.tupleNum
    
    def DOTscalarExpr ( self, graph ):
        self.condition.toDOT ( graph )
        graph.edge ( str ( self.condition.exprId ) , str ( self.opId ) )
    
    def DOTcaption ( self ):
        return str ( self.joinType ).lower() 
    

class Reduction ( Enum ):
    SUM   = 1
    COUNT = 2
    AVG   = 3
    MIN   = 4
    MAX   = 5


class Aggregation ( UnaryAlgExpr ):

    def __init__ ( self, ctxt, groupingIdentifiers, aggregates, child ):
        UnaryAlgExpr.__init__ ( self, ctxt, child )
        self.groupingIdentifiers = listWrap ( groupingIdentifiers ) 
        self.aggregates = listWrap ( aggregates ) 
        self.aggregateTuplesCreated = OrderedDict ()
        self.aggregateAttributes = dict ()
        self.avgAggregates = dict ()

        # add count for avg if necessary
        if any ( [ agg[0] == Reduction.AVG for agg in aggregates ] ):
            if not any ( [ agg[0] == Reduction.COUNT for agg in aggregates ] ):
                self.aggregates.append ( ( Reduction.COUNT, "", "count_agg" ) )

        # create aggregation attributes 
        for reductionType, inputIdentifier, aliasName in aggregates:
            id, aggAttr = ctxt.createAttribute ( aliasName )
            aggAttr.dataType = Type.DOUBLE
            if reductionType == Reduction.COUNT:
                aggAttr.dataType = Type.INT
                self.countAttr = aggAttr
            self.aggregateAttributes [ id ] = aggAttr
            if reductionType == Reduction.AVG:
                self.avgAggregates [ id ] = aggAttr
            self.aggregateTuplesCreated [ id ] = ( aggAttr, inputIdentifier, reductionType )
            
    def resolve ( self, ctxt ):
        self.inRelation = self.child.resolve ( ctxt )
        self.groupAttributes = OrderedDict()
        for ident in self.groupingIdentifiers:
            att = self.resolveAttribute ( ident )
            self.groupAttributes [ att.id ] = att
        self.aggregateInAttributes = OrderedDict()
        self.aggregateTuples = dict()
        for id, ( aggAttr, inputIdentifier, reductionType ) in self.aggregateTuplesCreated.items():
            inId = None
            if inputIdentifier != "":
                inAtt = self.resolveAttribute ( inputIdentifier )
                self.aggregateInAttributes [ inAtt.id ] = inAtt
                inId = inAtt.id
            self.aggregateTuples [ id ] = ( inId, reductionType )
        self.outRelation.update ( self.groupAttributes )
        self.outRelation.update ( self.aggregateAttributes )
        return self.outRelation
 
    def configure ( self, cfg, ctxt ):
        childTupleNum = self.child.configure ( cfg, ctxt )
        self.doGroup = len ( self.groupAttributes ) > 0
        try:
            self.numgroups = cfg[self.opId]["numgroups"]
        except:
            self.numgroups = childTupleNum
        if not self.doGroup:
            self.numgroups = 1
        self.tupleNum = self.numgroups
        return self.tupleNum
    
    def DOTsubcaption ( self ):       
        if len ( self.groupAttributes ) == 0: 
            return ""
        return list ( map ( lambda x: x[1].name, self.groupAttributes.items() ) ) 
 


class Projection ( UnaryAlgExpr ):
    
    def __init__ ( self, ctxt, identifiers, child ):
        UnaryAlgExpr.__init__ ( self, ctxt, child )
        self.identifiers = listWrap ( identifiers )
    
    def resolve ( self, ctxt ):
        self.inRelation = self.child.resolve ( ctxt )
        self.projectionAttributes = dict()
        for ident in self.identifiers:
            att = self.resolveAttribute ( ident )
            self.projectionAttributes [ att.id ] = att  
        self.outRelation = self.projectionAttributes
        return self.outRelation
    
    def configure ( self, cfg, ctxt ):
        self.tupleNum = self.child.configure ( cfg, ctxt )
        return self.tupleNum


class MaterializationType ( Enum ):
    RESULT = 1
    TEMPTABLE = 2


class Materialize ( UnaryAlgExpr ):
    
    def __init__ ( self, ctxt, child ):
        UnaryAlgExpr.__init__ ( self, ctxt, child )
   
    # -- constructor1 
    def result ( ctxt, child ):
        mat = Materialize ( ctxt, child )
        mat.matType = MaterializationType.RESULT
        return mat

    # -- constructor2
    def temp ( ctxt, identifier, child ):
        mat = Materialize ( ctxt, child )
        mat.table = ctxt.createTable ( identifier )
        mat.table [ "isTempTable" ] = True
        mat.matType = MaterializationType.TEMPTABLE
        return mat

    def resolve ( self, ctxt ):
        self.inRelation = self.child.resolve ( ctxt )
        for id, att in self.inRelation.items():
            self.touchAttribute ( id )
        if self.matType == MaterializationType.TEMPTABLE:
            self.table [ "sourceAttributes" ] = dict()
            for id, att in self.inRelation.items():
                self.table [ "attributes" ] [ att.name ] = att.dataType 
                self.table [ "sourceAttributes" ] [ att.name ] = att.id 
            self.table [ "numColumns" ] = len ( self.inRelation )
        self.outRelation = self.inRelation
        return self.outRelation
   
    # skip pruning for this operator 
    def prune ( self ):
        self.child.prune()
    
    def configure ( self, cfg, ctxt ):
        self.tupleNum = self.child.configure ( cfg, ctxt )
        if self.matType == MaterializationType.TEMPTABLE:
            self.table [ "size" ] = self.tupleNum
        return self.tupleNum
    
    def DOTcaption ( self ):
        if self.matType == MaterializationType.RESULT:
            return "Result"
        elif self.matType == MaterializationType.TEMPTABLE:
            return "Temptable"
 
    def DOTsubcaption ( self ):       
        if self.matType == MaterializationType.RESULT:
            return ""
        elif self.matType == MaterializationType.TEMPTABLE:
            return self.table["name"]


    
        
