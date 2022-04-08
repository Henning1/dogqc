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
EquiJoinTranslator.usePushDownJoin = True

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

    compiler = CudaCompiler ( algebraContext = alg, smArchitecture = "sm_75", decimalRepr = CType.FP64, debug = False )

    compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
    compiler.gencode ( compilerPlan )
    compiler.compile ( "tpch" + str(num) )
    compiler.execute ()


main()
