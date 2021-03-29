from dogqc.translatorBase import UnaryTranslator
from dogqc.codegen import CodeGenerator
from dogqc.cudalang import *
from dogqc.variable import Variable
from dogqc.kernel import Kernel, KernelCall
import dogqc.querylib as qlib



class LaneActivityProfiler ( UnaryTranslator ):
 
    def __init__( self, parentAlgExpr, child ):
        UnaryTranslator.__init__ ( self, parentAlgExpr, child )
 
    def produce( self, ctxt ):
        self.child.produce( ctxt )

    def consume ( self, ctxt ):
        ctxt.codegen.currentKernel.annotate ( "P" + str(self.algExpr.opId) )
        counters = list()

        emit ( printf ( "<p" + str(self.algExpr.opId) + ">\\n" ), ctxt.codegen.finish )  
        for i in range(0,33):
            counters.append ( ctxt.codegen.newStatisticsCounter ( "its" + str(i) + "active_" + "p" + str(self.algExpr.opId), str(i) + ", " ) )
        emit ( printf ( "</p" + str(self.algExpr.opId) + ">\\n\\n" ), ctxt.codegen.finish )  

        numActiveProfile = Variable.val (CType.INT, "numActiveProfile" + "_p" + str(self.algExpr.opId), ctxt.codegen )
        emit ( assign ( numActiveProfile, popcount ( ballotIntr ( qlib.Const.ALL_LANES, ctxt.vars.activeVar ) ) ), ctxt.codegen )

        with IfClause ( equals ( ctxt.codegen.warplane(), intConst(0) ), ctxt.codegen ): 
            for i in range(0,33):
                with IfClause ( equals ( numActiveProfile, intConst(i) ), ctxt.codegen ): 
                    emit ( atomicAdd ( counters[i], intConst(1) ), ctxt.codegen )

        self.parent.consume ( ctxt )
    
    def opId ( self ):
        return self.algExpr.opId + 2000000
    
    def toDOT ( self, graph ):
        self.child.toDOT ( graph )
        graph.node ( str ( self.opId() ), "Profile P" + str(self.algExpr.opId), style='filled', color='#98FB98' )
        graph.edge ( str ( self.child.opId() ), str ( self.opId() ), self.algExpr.edgeDOTstr() )
