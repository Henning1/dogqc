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

# --- translator tree structure ---

class LiteralTranslator ( object ):
 
    def __init__ ( self, algExpr ):
        Translator.__init__ ( self, algExpr ) 
    

class UnaryTranslator ( object ):
 
    def __init__ ( self, algExpr, child ):
        Translator.__init__ ( self, algExpr ) 
        self.child = child
        child.parent = self
    

class BinaryTranslator ( object ):
 
    def __init__ ( self, algExpr, leftChild, rightChild ):
        Translator.__init__ ( self, algExpr ) 
        self.leftChild = leftChild
        self.rightChild = rightChild
        leftChild.parent = self
        rightChild.parent = self
    
