alg.projection ( 
    [ "n_name", "o_year", "sum_profit" ],
    alg.aggregation (
        [ "n_name", "o_year" ],
        [ ( Reduction.SUM, "amount", "sum_profit" ) ],
        alg.map (
            "o_year",
            scal.ExtractExpr ( scal.AttrExpr ( "o_orderdate" ), scal.ExtractType.YEAR ),
            alg.join ( 
                ( "o_orderkey", "l_orderkey" ),
                alg.map (
                    "amount",
                    scal.SubExpr (
                        scal.MulExpr ( 
                            scal.AttrExpr ( "l_extendedprice" ),
                            scal.SubExpr ( 
                                scal.ConstExpr ( "1.0f", Type.FLOAT ),
                                scal.AttrExpr ( "l_discount" )
                            )
                        ),
                        scal.MulExpr ( 
                            scal.AttrExpr ( "ps_supplycost" ),
                            scal.AttrExpr ( "l_quantity" ),
                        )
                    ),
                    alg.join ( 
                        [ ( "p_partkey", "l_partkey" ), ( "s_suppkey", "l_suppkey" ) ],
                        alg.join ( 
                            ( "s_suppkey", "ps_suppkey" ),
                            alg.join (
                                ( "n_nationkey", "s_nationkey" ),
                                alg.scan ( "nation" ),
                                alg.scan ( "supplier" )
                            ),
                            alg.join ( 
                                ( "p_partkey", "ps_partkey" ),
                                alg.selection ( 
                                    scal.LikeExpr ( 
                                        scal.AttrExpr ( "p_name" ),
                                        scal.ConstExpr ( "%green%", Type.STRING )
                                    ),
                                    alg.scan ( "part" )
                                ),
                                alg.scan ( "partsupp" )
                            )
                        ),
                        alg.scan ( "lineitem" )
                    )
                ),
                alg.scan ( "orders" )
            )
        )
    )
)
