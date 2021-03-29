from dogqc.relationalAlgebra import AlgExpr


# --- translator base class ---

class Translator ( object ):
 
    def __init__ ( self, algExpr ):
        self.algExpr = algExpr

    def produce ( self, ctxt ):
        pass
    
    def consume ( self, ctxt ):
        pass

    def children ( self, ctxt ):
        pass
    
    def opId ( self ):
        return self.algExpr.opId

    def toDOT ( self, graph ): 
        pass
    
    

# --- translator tree structure ---

class LiteralTranslator ( Translator ):
 
    def __init__ ( self, algExpr ):
        Translator.__init__ ( self, algExpr ) 

    def toDOT ( self, graph ): 
        AlgExpr.toDOT ( self.algExpr, graph )

class UnaryTranslator ( Translator ):
 
    def __init__ ( self, algExpr, child ):
        Translator.__init__ ( self, algExpr ) 
        self.child = child
        child.parent = self
    
    def toDOT ( self, graph ):
        self.child.toDOT ( graph )
        AlgExpr.toDOT ( self.algExpr, graph )
        graph.edge ( str ( self.child.opId() ), str ( self.opId() ), self.algExpr.child.edgeDOTstr() )
    

class BinaryTranslator ( Translator ):
 
    def __init__ ( self, algExpr, leftChild, rightChild ):
        Translator.__init__ ( self, algExpr ) 
        self.leftChild = leftChild
        self.rightChild = rightChild
        leftChild.parent = self
        rightChild.parent = self
    
    def toDOT ( self, graph ):
        self.leftChild.toDOT ( graph )
        self.rightChild.toDOT ( graph )
        AlgExpr.toDOT ( self.algExpr, graph )
        graph.edge ( str ( self.leftChild.opId() ), str ( self.opId() ), self.algExpr.leftChild.edgeDOTstr() )
        graph.edge ( str ( self.rightChild.opId() ), str ( self.opId() ), self.algExpr.rightChild.edgeDOTstr() )
