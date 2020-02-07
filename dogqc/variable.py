from dogqc.cudalang import *
import dogqc.identifier as ident

class Variable ( object ):

    def __init__ ( self, dataType, name, numElements, ishostvector = False ):
        self.dataType = dataType
        if dataType == "string":
            self.dataType = "std::string"
        self.name = name
        self.numElements = numElements
        self.ishostvector = ishostvector
        self.ishostvalue = False
    
    @staticmethod
    def col ( dataType, name, numElements ):
        return Variable ( dataType, name, numElements, False )
    
    @staticmethod
    def val ( dataType, name, codegen=None, init=None ):
        v = Variable ( dataType, name, 1, False )
        v.ishostvalue = True
        if codegen is not None and init is None:
            v.declare ( codegen )
        if codegen is not None and init is not None:
            v.declareAssign ( init, codegen )
        return v
    
    @staticmethod
    def ptr ( dataType, name, codegen=None ):
        v = Variable ( dataType + "*", name, 1, False )
        if codegen is not None:
            v.declare ( codegen )
        return v
    
    @staticmethod
    def ref ( dataType, name ):
        return Variable ( dataType + "&", name, 1, False )

    @staticmethod
    def vec ( dataType, name, numElements ):
        return Variable ( dataType, name, numElements, True )

    @staticmethod
    def tid ( table ):
        return Variable ( CType.INT, ident.tid ( table ), 1 ) 
    
    @staticmethod
    def tidLit ( table, scanId ):
        return Variable ( CType.INT, "tid_" + table["name"] + str ( scanId ), 1 ) 

    def get ( self ):
        return self.name
    
    def getPointer ( self ):
        if(self.ishostvector):
            return self.name + ".data()"
        else:
            if(self.ishostvalue):
                return "&" + self.name
            else:
                return self.name

    def declare ( self, code ):
        code.add ( self.dataType + " " + self.name + ";" )
    
    def declareAssign ( self, expr, code ) :
        code.add ( self.dataType + " " + self.name + " = " + str(expr) + ";" )
    
    def declarePointer ( self, code ):
        code.add ( self.dataType + "* " + self.name + ";" )
    
    def declareVector ( self, code ):
        code.add( "std::vector < " + self.dataType + " > " + self.name + "(" + str(self.numElements) + ");" ) 
    
    def declareSharedArray ( self, length, code ):
        emit ( declareSharedArray ( self, length ), code )
    
    def declareShared ( self, code ):
        emit ( "volatile " + declareShared ( self ), code )
 
    def arrayAccess ( self, index ):
        return self.name + "[" + str(index) + "]"
    
    def length ( self ):
        return self.name + ".length()"
    
    def cstr ( self ):
        return self.name + ".c_str()"

    def filename ( self ):
        return self.get() + ".dat"

    def __str__ ( self ):
        return self.get()

    def getGPU ( self ):
        return "d_" + self.name
    
    def declareGPU ( self ):
        return self.dataType + "* " + self.getGPU() + ";"


    
