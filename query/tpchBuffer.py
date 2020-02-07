import sys
sys.path.insert(0,'..')
import os
import importlib
import dogqc.dbio as io
import schema.tpch
from dogqc.util import loadScript

# algebra types
from dogqc.relationalAlgebra import Context
from dogqc.relationalAlgebra import RelationalAlgebra
from dogqc.relationalAlgebra import Reduction
from dogqc.cudaTranslator import CudaCompiler
from dogqc.types import Type
from dogqc.cudalang import CType
import dogqc.scalarAlgebra as scal
from dogqc.kernel import KernelCall
from dogqc.hashJoins import EquiJoinTranslator 

KernelCall.defaultGridSize = 920
KernelCall.defaultBlockSize = 128
EquiJoinTranslator.usePushDownJoin = False


def experimentBuffer ( alg, plan, cfg, num ):
    # determine buffer positions
    joinNodes = alg.joinNodes + alg.semiJoinNodes + alg.antiJoinNodes
    singleMatchJoinNodes = [ n for n in joinNodes if not n.multimatch]
    possibleBufferPositions = [ [n.opId] for n in alg.selectionNodes + singleMatchJoinNodes ]
    print ( "possible buffer positions: " + str ( possibleBufferPositions ) )
    possibleBufferPositions.insert ( 0, [] )
    for p in possibleBufferPositions:
        print ( "buffer position: " + str ( p ) )
        for r in [1]:
            print ( "repeat: " + str ( r ) )
            compiler = CudaCompiler ( algebraContext = alg, smArchitecture = "sm_75", decimalRepr = CType.FP32, debug = False )
            compiler.setBuffers ( p )
            compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
            # compile
            compiler.gencode ( compilerPlan )
            # compile and execute cuda binary
            compiler.compile ( "tpch" + str(num) )
            compiler.execute ()



def main():
    # access database
    if len(sys.argv) < 3:
        print("Please provide the following arguments:\n1. The path TPC-H *.tbl data.\n2. The TPC-H query number 1-22.")
        quit()
    acc = io.dbAccess ( schema.tpch.tpchSchema, "mmdb", sys.argv[1] )
    # execute all tpch 
    if sys.argv[2] == "all":
        for i in range(1,23):
            print ( "-----------------------Executing TPCH-H query " + str(i) + "-----------------------" )
            execTpch ( acc, i )
    # execute one tpch 
    else:
        execTpch ( acc, sys.argv[2] )


def execTpch ( acc, num, showPlan=False ):
    
    qPath = "../query/plans/tpch" + str(num) + ".py"
    cfgPath = "../query/plans/tpch" + str(num) + "cfg.py"

    # read query plan
    alg = RelationalAlgebra ( acc )
    plan = eval ( loadScript ( qPath ) )
    
    # read plan configuration ( if exists )
    cfg = {}
    if os.path.isfile ( cfgPath ):
        cfg = eval ( loadScript ( cfgPath ) )

    # show basic plan
    if showPlan:
        alg.showGraph ( plan )
        
    plan = alg.resolveAlgebraPlan ( plan, cfg )
        
    # show refined plan 
    if showPlan:
        alg.showGraph ( plan )

    experimentBuffer ( alg, plan, cfg, num ) 

main()
