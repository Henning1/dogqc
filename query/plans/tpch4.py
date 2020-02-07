alg.projection (
    [ "o_orderpriority", "order_count" ],
    alg.aggregation (
        [ "o_orderpriority" ],
        [ ( Reduction.COUNT, "", "order_count" ) ],
        alg.semijoin (
            ( "l_orderkey", "o_orderkey" ), None,
            alg.selection (
                scal.SmallerExpr ( 
                    scal.AttrExpr ( "l_commitdate" ),
                    scal.AttrExpr ( "l_receiptdate" )
                ),
                alg.scan ( "lineitem" )
            ),
            alg.selection (
                scal.AndExpr (
                    scal.SmallerExpr ( 
                        scal.AttrExpr ( "o_orderdate" ),
                        scal.ConstExpr ( "19931001", Type.DATE )
                    ),
                    scal.LargerEqualExpr ( 
                        scal.AttrExpr ( "o_orderdate" ),
                        scal.ConstExpr ( "19930701", Type.DATE )
                    )
                ),
                alg.scan ( "orders" )
            )
        )
    )
)
