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

    for data in [ "pk-fk", "pk-8-fk", "pk-32-fk", "pk-zipf-fk", "pk-4zipf-fk" ]:
        for r in [2]:

            datagen.generator.joinData ( data )
            acc = io.dbAccess ( schema.join.joinSchema, "mmdb", "./", True )

            alg = RelationalAlgebra ( acc )
            plan = alg.join (
                ("r_build", "s_probe"),
                alg.scan ( "r_build" ),
                alg.scan ( "s_probe" )
            )
            
            plan = alg.resolveAlgebraPlan ( plan, {} )
            compiler = CudaCompiler ( alg, "sm_75", CType.FP32, False )
            compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
            compiler.gencode ( compilerPlan )
            compiler.compile ( "joinbenchNumOps" )
            compiler.execute ()


executePlanNumOperators ( False )

