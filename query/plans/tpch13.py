


alg.aggregation (
    [ "c_custkey" ],
    [ ( Reduction.COUNT, "o_orderkey", "c_count" ) ],
    alg.outerjoin (
        [ ( "c_custkey", "o_custkey" ) ], None,
        alg.selection (
            scal.NotExpr (
                scal.LikeExpr ( 
                    scal.AttrExpr ( "o_comment" ),
                    scal.ConstExpr ( "%special%requests%", Type.STRING )
                )
            ),
            alg.scan ( "orders" )
        ),
        alg.scan ( "customer" )
    )
)
