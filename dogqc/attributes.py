from dogqc.codegen import Variable
from dogqc.codegen import CodeGenerator
import dogqc.identifier
from dogqc.cudalang import *
import dogqc.identifier as ident
from dogqc.types import Type
from dogqc.cudalang import CType


class Attribute ( object ):
    
    def __init__ ( self, table, name, attType, computed=False, version=1 ):
        self.attType = attType
        self.name = name
        self.computed = computed
        if version > 1:
            self.name += str(version)
        self.unversionedName = name
        self.table = table
        self.numElements = table["size"]
        self.version = version
   
    def declareRegister ( self, codegen ):
        for v in self.variables():
            v.declare ( codegen )
            
    def variables ( self, codegen, sizeKnown=True ):
        #isHostVector = self.acc.isHostVector() 
        self.isHostVector = False
        if ( self.attType == Type.STRING ):
            var1 = Variable ( CType.SIZE, ident.column ( self ) + "_offset", self.table["size"], self.isHostVector );
            var2 = Variable ( CType.CHAR, ident.column ( self ) + "_char", self.table["size"], self.isHostVector );
            #if sizeKnown:
            #    var2.numElements = self.getNumBytes ( var2 )

            # add num elements to column
            return [var1, var2] 
        else:
            return [ Variable ( codegen.langType ( self.attType ), ident.column ( self ), self.table["size"], self.isHostVector ) ] 
    
    def inputVariables ( self, codegen ):
        resVars = self.variables ( codegen )
        for r in resVars:
            r.name = "input_" + r.name
            r.numElements = self.table["size"]
            r.ishostvector = False
        return resVars
    
    def intermediateVariables ( self ):
        resVars = self.variables ( )
        for r in resVars:
            r.name = "intm_" + r.name
            r.numElements = self.table["size"]
            r.ishostvector = False
        return resVars

    def resultVariables ( self, expectedResultNum ):
        resVars = self.variables ( )
        for r in resVars:
            r.name = "result_" + r.name
            r.numElements = expectedResultNum
            r.ishostvector = True
        return resVars

    

def initAttributeFromTable ( table, name, version=1 ):
    return Attribute ( table, name, table["attributes"][name], False, version)

def initAttributesFromTable ( table, names, version=1):
    atts = []
    for c in names:
        atts.append ( initAttributeFromTable ( table, c, version) )    
    return atts

def getAttributeFromSet ( attributes, tableName, name ):
    for a in attributes:
        if ( a.table["name"] == tableName and a.name == name):
            return a
