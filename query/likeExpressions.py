import sys
sys.path.insert(0,'..')

import dogqc.scalarAlgebra as scal
import dogqc.dbio as io
import schema.tpch
from dogqc.relationalAlgebra import Context
from dogqc.relationalAlgebra import RelationalAlgebra
from dogqc.relationalAlgebra import Reduction
from dogqc.cudaTranslator import CudaCompiler
from dogqc.types import Type
from dogqc.cudalang import CType


# Executes like expressions from the TPC-H benchmark.

# ---- access database ----
if len(sys.argv) < 2:
    print("Please provide a path to TPC-H *.tbl files as argument.")
    quit()
acc = io.dbAccess ( schema.tpch.tpchSchema, "mmdb", sys.argv[1] )
alg = RelationalAlgebra ( acc )

def getPlan ( i ):
    if i == 1:
        # like tpch2
        # select p_type from part where p_type like '%BRASS'
        root = alg.projection ( 
            [ "p_type" ],
            alg.selection ( 
                scal.LikeExpr ( 
                    scal.AttrExpr ( "p_type" ),
                    scal.ConstExpr ( "%POLISHED%", Type.STRING )
                ),
                alg.scan ( "part" )
            )
        )
    
    if i == 2:
        # like tpch9
        # select p_name from part where p_name like '%green%'
        root = alg.projection ( 
            [ "p_name" ],
            alg.selection ( 
                scal.LikeExpr ( 
                    scal.AttrExpr ( "p_name" ),
                    scal.ConstExpr ( "%green%", Type.STRING )
                ),
                alg.scan ( "part" )
            )
        )
    if i == 3:
        # like tpch13
        # select o_comment from orders where o_comment not like '%special%requests%'
        root = alg.projection ( 
            [ "o_comment" ],
            alg.selection ( 
                scal.NotExpr ( 
                    scal.LikeExpr ( 
                        scal.AttrExpr ( "o_comment" ),
                        scal.ConstExpr ( "%special%requests%", Type.STRING )
                    ),
                ),
                alg.scan ( "orders" )
            )
        )
    if i == 4:
        # like tpch14
        # select p_type from part where p_type like 'PROMO%'
        root = alg.projection ( 
            [ "p_type" ],
            alg.selection ( 
                scal.LikeExpr ( 
                    scal.AttrExpr ( "p_type" ),
                    scal.ConstExpr ( "PROMO%", Type.STRING )
                ),
                alg.scan ( "part" )
            )
        )
    if i == 5:
        # like tpch16
        # select s_comment from supplier where s_comment like '%Customer%Complaints%'
        root = alg.projection ( 
            [ "s_comment" ],
            alg.selection ( 
                scal.LikeExpr ( 
                    scal.AttrExpr ( "s_comment" ),
                    scal.ConstExpr ( "%Customer%Complaints%", Type.STRING )
                ),
                alg.scan ( "supplier" )
            )
        )
    if i == 6:
        # like tpch20
        # select p_name from part where p_name like 'forest%'
        root = alg.projection ( 
            [ "p_name" ],
            alg.selection ( 
                scal.LikeExpr ( 
                    scal.AttrExpr ( "p_name" ),
                    scal.ConstExpr ( "forest%", Type.STRING )
                ),
                alg.scan ( "part" )
            )
        )
    return root


for i in [1,2,3,4,5,6]:
    plan = getPlan ( i ) 
    plan = alg.resolveAlgebraPlan ( plan, {} )
    compiler = CudaCompiler ( alg, "sm_75", CType.FP32, False )
    compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
    compiler.gencode ( compilerPlan )
    compiler.compile ( "likeExpressions" )
    compiler.execute ()

quit()
