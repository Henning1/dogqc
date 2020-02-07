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

KernelCall.defaultGridSize = 920
KernelCall.defaultBlockSize = 128

sys.setrecursionlimit(100000)

if len(sys.argv) < 2:
    print("Please provide a path to TPCH *.tbl files as argument.")
    quit()
    
acc = io.dbAccess ( schema.tpch.tpchSchema, "mmdb", sys.argv[1] )



def experimentDivergence ( addLaneRefill ):
    predicateValue = 45
    for r in [1,2,3,4,5]:
        for n in [ 0,3,6,9,12,15,18,21,24,27 ]:
            alg = RelationalAlgebra ( acc )
            plan = alg.selection ( 
                scal.LargerExpr ( 
                    scal.AttrExpr ( "l_quantity" ),
                    scal.ConstExpr ( str(predicateValue), Type.INT )
                ),
                alg.scan ( "lineitem" )
            )
            name = "l_extendedprice"
            for i in range(0,n):
                plan = alg.selection (
                    scal.NotExpr (
                        scal.SmallerExpr ( 
                            scal.AbsExpr ( 
                                scal.SubExpr ( 
                                    scal.AttrExpr ( name ),
                                    scal.ConstExpr ( str ( random.randint (1, 999999999) ), Type.FLOAT )
                                )
                            ),
                            scal.ConstExpr ( "0.01", Type.FLOAT )
                        )
                    ),
                    plan
                )
                newname = "l_extprice" + str(i)
                plan = alg.map ( newname,
                    scal.MulExpr ( 
                        scal.AttrExpr ( name ),
                        scal.ConstExpr ( "0.99", Type.FLOAT )
                    ),
                    plan
                )
                name = newname
           
            plan = alg.projection ( [ name ], plan ) 
            cfg = {}
            plan = alg.resolveAlgebraPlan ( plan, cfg )
            compiler = CudaCompiler ( alg, "sm_75", CType.FP64, False )
            if addLaneRefill:
                compiler.setBuffers ( [ 2 ] )
            compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
            compiler.gencode ( compilerPlan )
            compiler.compile ( "filterbenchNumOps" )
            compiler.execute ()


experimentDivergence ( False )


