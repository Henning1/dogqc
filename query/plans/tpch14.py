
alg.projection (
    [ "promo_revenue" ],
    alg.map (
        "promo_revenue",
        scal.DivExpr (
            scal.MulExpr (
                scal.ConstExpr ( "100.0f", Type.DOUBLE ),
                scal.AttrExpr ( "sum_promo" )
            ),
            scal.AttrExpr ( "sum_rev" )
        ),
        alg.aggregation (
            [],
            [ ( Reduction.SUM, "rev", "sum_rev" ),
              ( Reduction.SUM, "promo", "sum_promo" ) ],
            alg.map (
                "rev",
                scal.MulExpr ( 
                    scal.AttrExpr ( "l_extendedprice" ),
                    scal.SubExpr ( 
                        scal.ConstExpr ( "1.0f", Type.FLOAT ),
                        scal.AttrExpr ( "l_discount" )
                    )
                ),
                alg.map (
                    "promo",
                    scal.CaseExpr (
                        [
                            (
                                scal.LikeExpr ( 
                                    scal.AttrExpr ( "p_type" ),
                                    scal.ConstExpr ( "PROMO%", Type.STRING )
                                ),
                                scal.MulExpr ( 
                                    scal.AttrExpr ( "l_extendedprice" ),
                                    scal.SubExpr ( 
                                        scal.ConstExpr ( "1.0f", Type.DOUBLE ),
                                        scal.AttrExpr ( "l_discount" )
                                    )
                                )
                            )
                        ],
                        scal.ConstExpr ( "0.0f", Type.INT )
                    ),
                    alg.join (
                        ( "l_partkey", "p_partkey" ),
                        alg.selection ( 
                            scal.AndExpr (
                                scal.LargerEqualExpr (
                                    scal.AttrExpr ( "l_shipdate" ),
                                    scal.ConstExpr ( "19950901", Type.DATE )
                                ),
                                scal.SmallerExpr (
                                    scal.AttrExpr ( "l_shipdate" ),
                                    scal.ConstExpr ( "19951001", Type.DATE )
                                )
                            ),
                            alg.scan ( "lineitem" )
                        ),
                        alg.scan ( "part" )
                    )
                )
            )
        )
    )
)
