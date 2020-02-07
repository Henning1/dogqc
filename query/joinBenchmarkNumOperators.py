import sys
sys.path.insert(0,'..')
import os
import importlib
import dogqc.dbio as io
import schema.join
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

from dogqc.hashJoins import EquiJoinTranslator 

KernelCall.defaultGridSize = 920
KernelCall.defaultBlockSize = 128


sys.setrecursionlimit(100000)


def executePlanNumOperators ( usePushDownJoin = False ):

    EquiJoinTranslator.usePushDownJoin = usePushDownJoin 

    datagen.generator.joinData ( "pk-4zipf-fk" )
    acc = io.dbAccess ( schema.join.joinSchema, "mmdb", "./", True )

    for r in [2]:
        for n in [ 0,3,6,9,12,15,18,21,24,27 ]:

            alg = RelationalAlgebra ( acc )
            plan = alg.join (
                ("r_build", "s_probe"),
                alg.scan ( "r_build" ),
                alg.scan ( "s_probe" )
            )
            
            name = "r_linenumber"
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
                newname = "l_linenumber" + str(i)
                plan = alg.map ( newname,
                    scal.MulExpr ( 
                        scal.AttrExpr ( name ),
                        scal.ConstExpr ( "0.99", Type.FLOAT )
                    ),
                    plan
                )
                name = newname
           
            cfg = {}

            plan = alg.projection ( [ name ], plan )
            
            plan = alg.resolveAlgebraPlan ( plan, cfg )
            compiler = CudaCompiler ( alg, "sm_75", CType.FP64, False )
            compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
            compiler.gencode ( compilerPlan )
            compiler.compile ( "joinbenchNumOps" )
            compiler.execute ()


executePlanNumOperators ( True )

