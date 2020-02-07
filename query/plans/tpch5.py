alg.aggregation ( 
    [ "n_name" ],
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
            [ ( "l_suppkey", "s_suppkey" ),
    # todo: remove bijection assumption ( "c_nationkey", "s_nationkey" ),
              ( "s_nationkey", "n_nationkey" ) ],
            alg.scan ( "supplier" ),
            alg.join (
                ( "l_orderkey", "o_orderkey" ),
                alg.join (
                    ( "c_custkey", "o_custkey" ), 
                    alg.join (
                        ( "c_nationkey", "n_nationkey" ),
                        alg.join (
                            ( "n_regionkey", "r_regionkey" ),
                            alg.selection ( 
                                scal.EqualsExpr (
                                    scal.AttrExpr ( "r_name" ),
                                    scal.ConstExpr ( "ASIA", Type.STRING )
                                ),
                                alg.scan ( "region" )
                            ),
                            alg.scan ( "nation" )
                        ),
                        alg.scan ( "customer" )
                    ),
                    alg.selection (
                        scal.AndExpr (
                            scal.LargerEqualExpr (
                                scal.AttrExpr ( "o_orderdate" ),
                                scal.ConstExpr ( "19940101", Type.DATE )
                            ),
                            scal.SmallerExpr (
                                scal.AttrExpr ( "o_orderdate" ),
                                scal.ConstExpr ( "19950101", Type.DATE )
                            )
                        ),
                        alg.scan ( "orders" )
                    )
                ),
                alg.scan ( "lineitem" )
            )
        )
    )
)
