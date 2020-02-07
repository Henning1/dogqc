import sys
sys.path.insert(0,'..')
import os
import importlib
import dogqc.dbio as io
import schema.tpch
import subprocess
import random
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
import datagen.generator


#KernelCall.defaultGridSize = 46
#KernelCall.defaultBlockSize = 32

#KernelCall.defaultGridSize = 184 #4 warps per sm
#KernelCall.defaultBlockSize = 32

KernelCall.defaultGridSize = 184 #8 warps per sm
KernelCall.defaultBlockSize = 64

#KernelCall.defaultGridSize = 368 #16 warps per sm
#KernelCall.defaultBlockSize = 64


sys.setrecursionlimit(100000)


if len(sys.argv) < 2:
    print("Please provide a path to TPCH *.tbl files as argument.")
    quit()
    
acc = io.dbAccess ( schema.tpch.tpchSchema, "mmdb", sys.argv[1] )

def experimentDivergence ( doBuffer ):
    for predicateValue in [5,10,15,20,25,30,35,40,45,50]:
        alg = RelationalAlgebra ( acc )
        plan = alg.join ( 
            ("l_partkey", "p_partkey"),
            alg.scan ( "part" ),
            alg.selection ( 
                scal.SmallerExpr ( 
                    scal.AttrExpr ( "l_quantity" ),
                    scal.ConstExpr ( str(predicateValue), Type.INT )
                ),
                alg.scan ( "lineitem" )
            )
        )
        plan = alg.aggregation (
            [ "l_quantity" ],
            [ ( Reduction.SUM, "l_extendedprice", "total_price" ) ],
            plan
        )
        cfg = {}
        cfg[5] = {}
        cfg[5]["numgroups"] = 100000
        plan = alg.resolveAlgebraPlan ( plan, cfg )
        compiler = CudaCompiler ( alg, "sm_75", CType.FP64, False )
        if doBuffer:
            compiler.setBuffers ( [ 3 ] )
        compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
        compiler.gencode ( compilerPlan )
        compiler.compile ( "filterbenchTPCH" )
        compiler.execute ()


experimentDivergence ( False )

experimentDivergence ( True )


