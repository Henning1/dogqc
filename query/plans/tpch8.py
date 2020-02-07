alg.projection ( 
    [ "o_year", "mkt_share", "salesnum" ],
    alg.map ( "mkt_share",
        scal.DivExpr (
            scal.AttrExpr ( "sum_volume_brazil" ),
            scal.AttrExpr ( "sum_volume" )
        ),
        alg.aggregation ( 
            [ "o_year" ], 
            [ ( Reduction.SUM, "case_volume", "sum_volume_brazil" ),
              ( Reduction.SUM, "volume", "sum_volume" ),
              ( Reduction.COUNT, "", "salesnum" ) ],
            alg.map ( "case_volume",
                scal.CaseExpr (
                    [ ( scal.EqualsExpr ( scal.AttrExpr ( "n2.n_name" ), scal.ConstExpr ( "BRAZIL", Type.STRING ) ), scal.AttrExpr ( "volume" ) ) ],
                    scal.ConstExpr ( "0", Type.FLOAT )
                ),
                alg.map ( "volume",
                    scal.MulExpr (
                        scal.AttrExpr ( "l_extendedprice" ),
                        scal.SubExpr (
                            scal.ConstExpr ( "1.0f", Type.FLOAT ), 
                            scal.AttrExpr ( "l_discount" )
                        )
                    ),
                    alg.map ( "o_year",
                        scal.ExtractExpr (
                            scal.AttrExpr ( "o_orderdate" ), 
                            scal.ExtractType.YEAR 
                        ),
                        alg.join (
                            ( "s_nationkey", "n2.n_nationkey" ),
                            alg.scan ( "nation", "n2" ),
                            alg.join (
                                ( "s_suppkey", "l_suppkey" ),
                                alg.join (
                                    ( "c_nationkey", "n1.n_nationkey" ), 
                                    alg.join (
                                        ( "n1.n_regionkey", "r_regionkey" ),
                                        alg.selection (
                                            scal.EqualsExpr (
                                                scal.AttrExpr ( "r_name" ),
                                                scal.ConstExpr ( "AMERICA", Type.STRING )
                                            ),
                                            alg.scan ( "region" )
                                        ),
                                        alg.scan ( "nation", "n1" )
                                    ),
                                    alg.join (
                                        ( "o_custkey", "c_custkey" ),
                                        alg.join (
                                            ( "l_orderkey", "o_orderkey" ),
                                            alg.join (
                                                [ ( "p_partkey", "l_partkey" ) ],
                                                alg.selection ( 
                                                    scal.EqualsExpr (
                                                        scal.AttrExpr ( "p_type" ), 
                                                        scal.ConstExpr ( "ECONOMY ANODIZED STEEL", Type.STRING )
                                                    ),
                                                    alg.scan ( "part" )
                                                ),
                                                alg.scan ( "lineitem" )
                                            ),
                                            alg.selection ( 
                                                scal.AndExpr (
                                                    scal.LargerEqualExpr (
                                                        scal.AttrExpr ( "o_orderdate" ), 
                                                        scal.ConstExpr ( "19950101", Type.DATE )
                                                    ),
                                                    scal.SmallerEqualExpr (
                                                        scal.AttrExpr ( "o_orderdate" ), 
                                                        scal.ConstExpr ( "19961231", Type.DATE )
                                                    )
                                                ),
                                                alg.scan ( "orders" )
                                            )
                                        ),
                                        alg.scan ( "customer" )
                                    )
                                ),
                                alg.scan ( "supplier" )
                            )
                        )
                    )
                )
            )
        )
    )
) 
