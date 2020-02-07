
from enum import Enum
import dogqc.cudalang as lang
from dogqc.types import Type
from dogqc.code import Code
from dogqc.variable import Variable
from dogqc.codegen import CodeGenerator

class ScalarExpr ( object ):

    idSource = 1000

    def __init__ ( self ):
        self.type = None
        self.exprId = ScalarExpr.idSource
        ScalarExpr.idSource += 1
    
    def DOT ( self, graph ):
        graph.node ( str ( self.exprId ), self.DOTstr(), 
            {
             'shape': 'box', 
             'color': 'lightgray', 
             'style': 'filled',
             'fontsize':'9'
            } )
    
    def DOTstr ( self ):
        return self.__class__.__name__ + " (" + str(self.exprId) + ")"


class LiteralExpr ( object ):
    
    def resolve ( self, algExpr ):
        return self.resolveImpl ( algExpr )

    def resolveImpl ( self, algExpr ):
        return dict ()
    
    def translate ( self, ctxt ):
        return self.translateImpl ( ctxt )

    def translateImpl ( self, ctxt ):
        pass
    
    def toDOT ( self, graph ):
        self.DOT ( graph )


class UnaryExpr ( object ):

    def __init__ ( self, child ):
        self.child = child
    
    def resolve ( self, algExpr ):
        child = self.child.resolve ( algExpr )
        self.type = self.child.type
        return self.resolveImpl ( child, algExpr )

    def resolveImpl ( self, child, algExpr ):
        return child

    def translate ( self, ctxt ):
        child = self.child.translate ( ctxt )
        return self.translateImpl ( child, ctxt )

    def translateImpl ( self, child, ctxt ):
        pass
    
    def toDOT ( self, graph ):
        self.child.toDOT ( graph )
        self.DOT ( graph )
        graph.edge ( str ( self.child.exprId ), str ( self.exprId ) )


class BinaryExpr ( object ):
    
    def __init__ ( self, leftChild, rightChild ):
        self.leftChild = leftChild
        self.rightChild = rightChild

    def resolve ( self, algExpr ):
        left = self.leftChild.resolve ( algExpr )
        right = self.rightChild.resolve ( algExpr )
        if self.leftChild.type == self.rightChild.type:
            self.type = self.leftChild.type
        else:
            self.type = Type.DOUBLE
        return self.resolveImpl ( left, right, algExpr )

    def resolveImpl ( self, left, right, algExpr ):
        attributes = dict ( left )
        attributes.update ( right )
        return attributes

    def translate ( self, ctxt ):
        left = self.leftChild.translate ( ctxt )
        right = self.rightChild.translate ( ctxt )
        return self.translateImpl ( left, right, ctxt )

    def translateImpl ( self, left, right, ctxt ):
        pass
    
    def toDOT ( self, graph ):
        self.leftChild.toDOT ( graph )
        self.rightChild.toDOT ( graph )
        self.DOT ( graph )
        graph.edge ( str ( self.leftChild.exprId ), str ( self.exprId ) )
        graph.edge ( str ( self.rightChild.exprId ), str ( self.exprId ) )


class AttrExpr ( ScalarExpr, LiteralExpr ):
    
    def __init__ ( self, identifier ):
        ScalarExpr.__init__ ( self )
        LiteralExpr.__init__ ( self )
        self.identifier = identifier
    
    def resolveImpl ( self, algExpr ):
        self.attr = algExpr.resolveAttribute ( self.identifier )
        attributes = dict()
        attributes [ self.attr.id ] = self.attr
        self.type = self.attr.dataType
        return attributes

    def translateImpl ( self, ctxt ):
        return ctxt.attFile.access ( self.attr )
    
    def DOTstr ( self ):
        if not hasattr ( self, "attr" ):
            return self.identifier
        return ScalarExpr.DOTstr ( self ) + "\n" + self.attr.name


class ConstExpr ( ScalarExpr, LiteralExpr ):
    
    def __init__ ( self, token, type ):
        ScalarExpr.__init__ ( self )
        LiteralExpr.__init__ ( self )
        self.token = token
        self.type = type
    
    def translateImpl ( self, ctxt ):
        if self.type == Type.INT:
            return self.token
        if self.type == Type.FLOAT:
            return ("(float)" + self.token )
        if self.type == Type.DOUBLE:
            return self.token
        if self.type == Type.DATE:
            return self.token
        if self.type == Type.CHAR:
            return "'" + self.token + "'"
        if self.type == Type.STRING:
            return ctxt.codegen.stringConstant ( self.token )
    
    def DOTstr ( self ):
        return ScalarExpr.DOTstr ( self ) + "\n'" + self.token + "'"


class BoolExpr ( ScalarExpr ):
    
    def __init__ ( self ):
        ScalarExpr.__init__ ( self )
        self.type = Type.BOOLEAN


class EqualsExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def resolveImpl ( self, left, right, algExpr ):
        if self.leftChild.type == Type.FLOAT or self.rightChild.type == Type.FLOAT:
            self.comparisonType = Type.DOUBLE
        elif self.leftChild.type != self.rightChild.type:
            raise SyntaxError ( "Comparing unmatched types: " + str(self.leftChild.type) 
                + " and " + str(self.rightChild.type) + " in scalar expr " + str ( self.exprId )) 
        self.comparisonType = self.leftChild.type
        base = BinaryExpr.resolveImpl ( self, left, right, algExpr )
        return base
          
    def translateImpl ( self, left, right, ctxt ):
        if self.comparisonType == Type.STRING:
            return lang.stringEquals ( left, right )
        else:
            return lang.equals ( left, right )


class LikeExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def resolveImpl ( self, left, right, algExpr ):
        if self.leftChild.type != self.rightChild.type:
            raise SyntaxError ( "Comparing unmatched types: " + str(self.leftChild.type) 
                + " and " + str(self.rightChild.type) + " in scalar expr " + str ( self.exprId )) 
        self.comparisonType = self.leftChild.type
        if self.leftChild.type != Type.STRING:
            raise SyntaxError ( "Prefix check only for strings." ) 
        return BinaryExpr.resolveImpl ( self, left, right, algExpr )
          
    def translateImpl ( self, left, right, ctxt ):
        return lang.stringLike ( left, right )


class SubstringExpr ( ScalarExpr ):
    
    def __init__ ( self, exprString, exprFrom, exprFor ):
        ScalarExpr.__init__ ( self )
        self.exprString = exprString
        self.exprFrom = exprFrom
        self.exprFor = exprFor
        self.type = Type.STRING
    
    def resolve ( self, algExpr ):
        resStr = self.exprString.resolve(algExpr)
        self.baseStr = resStr
        resFor = self.exprFrom.resolve(algExpr)
        resFrm = self.exprFor.resolve(algExpr)
        if self.exprString.type != Type.STRING:
            raise SyntaxError ( "Need STRING type, type is: " + str(self.exprString.type) )
        if self.exprFrom.type != Type.INT:
            raise SyntaxError ( "Need INT type, type is: " + str(self.exprFrom.type) )
        if self.exprFor.type != Type.INT:
            raise SyntaxError ( "Need INT type, type is: " + str(self.exprFor.type)  )
        res = dict()
        res.update ( resStr )
        res.update ( resFor )
        res.update ( resFrm )
        return res

    def translate ( self, ctxt ):
        trStr = self.exprString.translate(ctxt)
        trFor = self.exprFrom.translate(ctxt)
        trFrm = self.exprFor.translate(ctxt)
        return lang.stringSubstring ( trStr, trFor, trFrm )
        
    def toDOT ( self, graph ):
        self.DOT ( graph )
        self.exprString.toDOT ( graph )
        self.exprFrom.toDOT ( graph )
        self.exprFor.toDOT ( graph )
        graph.edge ( str ( self.exprString.exprId ), str ( self.exprId ) )
        graph.edge ( str ( self.exprFrom.exprId ), str ( self.exprId ) )
        graph.edge ( str ( self.exprFor.exprId ), str ( self.exprId ) )


class CaseExpr ( ScalarExpr ):
    
    def __init__ ( self, exprListWhenThen, exprElse=None ):
        ScalarExpr.__init__ ( self )
        if len ( exprListWhenThen ) == 0:
            raise SyntaxError ( "The case expression needs at least one when clause." ) 
        self.exprListWhenThen = exprListWhenThen
        self.exprElse = exprElse 
        self.type = Type.FLOAT
    
    def resolve ( self, algExpr ):
        attributes = dict()
        for w, t in self.exprListWhenThen:
            attributes.update ( w.resolve ( algExpr ) )
            attributes.update ( t.resolve ( algExpr ) )
        if self.exprElse != None:
            attributes.update ( self.exprElse.resolve ( algExpr ) )
        return attributes

    def translate ( self, ctxt ):
        code = Code() 

        var = Variable.val ( ctxt.codegen.langType ( self.type ), "casevar" + str(self.exprId) )
        var.declare ( ctxt.codegen )

        #declare variable 
        w0,t0 = self.exprListWhenThen[0]
        with lang.IfClause ( w0.translate ( ctxt ), ctxt.codegen ):
            lang.emit ( lang.assign ( var, t0.translate ( ctxt ) ), ctxt.codegen )
        for w,t in self.exprListWhenThen[1:]:
            with lang.ElseIfClause ( w.translate ( ctxt ), ctxt.codegen ):
                lang.emit ( lang.assign ( var, t.translate ( ctxt ) ), ctxt.codegen )
        if self.exprElse != None:
            with lang.ElseClause ( ctxt.codegen ):
                lang.emit ( lang.assign ( var, self.exprElse.translate ( ctxt ) ), ctxt.codegen )
        return var.get()
    
    def toDOT ( self, graph ):
        self.DOT ( graph )
        for (w,t) in self.exprListWhenThen:
            w.toDOT ( graph )
            t.toDOT ( graph )
            graph.edge ( str ( w.exprId ), str ( self.exprId ) )
            graph.edge ( str ( t.exprId ), str ( self.exprId ) )
        self.exprElse.toDOT ( graph )
        graph.edge ( str ( self.exprElse.exprId ), str ( self.exprId ) )
          


class AndExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return lang.andLogic ( left, right )


class OrExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return lang.orLogic ( left, right )


class NotExpr ( BoolExpr, UnaryExpr ):
    
    def __init__ ( self, child ):
        BoolExpr.__init__ ( self )
        UnaryExpr.__init__ ( self, child )
    
    def translateImpl ( self, child, ctxt ):
        return lang.notLogic ( child )


def InExpr ( v, exprList ):
    orTree = EqualsExpr ( v, exprList[0] )
    for e in exprList[1:]:
        orTree = OrExpr ( EqualsExpr ( v, e ), orTree )
    return orTree



class SmallerExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return lang.smaller ( left, right )


class SmallerEqualExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return lang.smallerEqual ( left, right )


class LargerExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return lang.larger ( left, right )


class LargerEqualExpr ( BoolExpr, BinaryExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BoolExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return lang.largerEqual ( left, right )


class ArithExpr ( ScalarExpr ):
    
    def __init__ ( self ):
        ScalarExpr.__init__ ( self )
        self.type = Type.INT


class BinaryNumericExpr ( ArithExpr, BinaryExpr ):

    def __init__ ( self, leftChild, rightChild ):
        ArithExpr.__init__ ( self )
        BinaryExpr.__init__ ( self, leftChild, rightChild )

    
class MulExpr ( BinaryNumericExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BinaryNumericExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return ctxt.lang.mul ( left, right )


class AbsExpr ( UnaryExpr, ArithExpr ):
    
    def __init__ ( self, child ):
        ArithExpr.__init__ ( self )
        UnaryExpr.__init__ ( self, child )
    
    def translateImpl ( self, child, ctxt ):
        return ctxt.lang.abs ( child )


class DivExpr ( BinaryNumericExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BinaryNumericExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return ctxt.lang.div ( left, right )


class AddExpr ( BinaryNumericExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BinaryNumericExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return ctxt.lang.add ( left, right )


class SubExpr ( BinaryNumericExpr ):
    
    def __init__ ( self, leftChild, rightChild ):
        BinaryNumericExpr.__init__ ( self, leftChild, rightChild )
    
    def translateImpl ( self, left, right, ctxt ):
        return ctxt.lang.sub ( left, right )


class ExtractType ( Enum ):
    DAY   = 1
    MONTH = 2
    YEAR  = 3


class ExtractExpr ( ArithExpr, UnaryExpr ):

    def __init__ ( self, child, extractType ):
        ArithExpr.__init__ ( self )
        UnaryExpr.__init__ ( self, child )
        self.extract = extractType
    
    def translateImpl ( self, child, ctxt ):
        if self.extract is ExtractType.DAY:
            return ctxt.lang.modulo ( child, ctxt.lang.intConst(100) )
        if self.extract is ExtractType.MONTH:
            return ctxt.lang.modulo ( div ( child, ctxt.lang.intConst(100), ctxt.lang.intConst(100) ) )
        if self.extract is ExtractType.YEAR:
            return ctxt.lang.div ( child, ctxt.lang.intConst(10000) )







