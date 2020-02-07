alg.projection (
    [ "s_name", "s_address" ],
    alg.semijoin (
        ( "s_suppkey", "ps_suppkey" ), None,
        alg.selection (
            scal.LargerExpr (
                scal.AttrExpr ( "ps_availqty" ),
                scal.MulExpr (
                    scal.ConstExpr ( "0.5f", Type.DOUBLE ),
                    scal.AttrExpr ( "sum_qty" )
                )
            ),
            alg.join (
                [ ( "l_partkey", "ps_partkey" ), ( "l_suppkey", "ps_suppkey" ) ],
                alg.semijoin (
                    ( "ps_partkey", "p_partkey" ), None,
                    alg.selection (
                        scal.LikeExpr (
                            scal.AttrExpr ( "p_name" ),
                            scal.ConstExpr( "forest%", Type.STRING )
                        ),
                        alg.scan ( "part" )
                    ),
                    alg.scan ( "partsupp" )
                ),
                alg.aggregation (
                    [ "l_partkey", "l_suppkey" ],
                    [ ( Reduction.SUM, "l_quantity", "sum_qty" ) ],
                    alg.selection (
                        scal.AndExpr (
                            scal.LargerEqualExpr (
                                scal.AttrExpr ( "l_shipdate" ),
                                scal.ConstExpr ( "19940101", Type.DATE )
                            ),
                            scal.SmallerExpr (
                                scal.AttrExpr ( "l_shipdate" ),
                                scal.ConstExpr ( "19950101", Type.DATE )
                            )
                        ),
                        alg.scan ( "lineitem" )
                    )
                )
            )
        ),
        alg.join (
            ( "s_nationkey", "n_nationkey" ),
            alg.selection (
                scal.EqualsExpr (
                    scal.AttrExpr ( "n_name" ),
                    scal.ConstExpr ( "CANADA", Type.STRING )
                ),
                alg.scan ( "nation" )
            ),
            alg.scan ( "supplier" )
        )
    )
)

