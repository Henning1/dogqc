alg.aggregation (
    [ "c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice" ],
    [ ( Reduction.SUM, "l_quantity", "sum_qty" ) ],
    alg.join (
        ( "o_orderkey", "l_orderkey" ),
        alg.join (
            ( "c_custkey", "o_custkey" ),
            alg.scan ( "customer" ),
            alg.semijoin (
                ( "o_orderkey", "l_orderkey" ), None,
                alg.selection (
                    scal.LargerExpr (
                        scal.AttrExpr ( "sum_qty" ),
                        scal.ConstExpr ( "300.0f", Type.DOUBLE )
                    ),
                    alg.aggregation ( 
                        [ "l_orderkey" ],
                        [ ( Reduction.SUM, "l_quantity", "sum_qty" ) ],
                        alg.scan ( "lineitem" )
                    )
                ),
                alg.scan ( "orders" )
            )
        ),
        alg.scan ( "lineitem" )
    )
)
