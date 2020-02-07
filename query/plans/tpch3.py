alg.aggregation (
    [ "l_orderkey", "o_orderdate", "o_shippriority" ],
    [ ( Reduction.SUM, "revenue", "sum_rev" ) ],
    alg.map (
        "revenue",
        scal.MulExpr ( 
            scal.AttrExpr ( "l_extendedprice" ),
            scal.SubExpr ( 
                scal.ConstExpr ( "1.0f", Type.FLOAT ),
                scal.AttrExpr ( "l_discount" )
            )
        ),
        alg.join (
            ( "o_orderkey", "l_orderkey" ),
            alg.join (
                ( "c_custkey", "o_custkey" ),
                alg.selection (
                    scal.EqualsExpr (
                        scal.AttrExpr ( "c_mktsegment" ),
                        scal.ConstExpr ( "BUILDING", Type.STRING )
                    ),
                    alg.scan ( "customer" )
                ),
                alg.selection (
                    scal.SmallerExpr (
                        scal.AttrExpr ( "o_orderdate" ),
                        scal.ConstExpr ( "19950315", Type.DATE )
                    ),
                    alg.scan ( "orders" )
                )
            ),
            alg.selection (
                scal.LargerExpr (
                    scal.AttrExpr ( "l_shipdate" ),
                    scal.ConstExpr ( "19950315", Type.DATE )
                ),
                alg.scan ( "lineitem" )
            )
        )
    )
)
