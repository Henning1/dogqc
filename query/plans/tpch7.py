alg.projection ( 
    [ "n1.n_name", "n2.n_name", "l_year", "sum_volume", "avg_volume" ],
    alg.aggregation ( 
        [ "n1.n_name", "n2.n_name", "l_year" ], 
        [ ( Reduction.SUM, "volume", "sum_volume" ),
        ( Reduction.AVG, "volume", "avg_volume" ) ],
        alg.map ( "l_year", 
            scal.ExtractExpr ( scal.AttrExpr ( "l_shipdate" ), 
            scal.ExtractType.YEAR 
        ),
            alg.map ( "volume", 
                scal.MulExpr ( 
                    scal.AttrExpr ( "l_extendedprice" ),
                    scal.SubExpr ( 
                        scal.ConstExpr ( "1", Type.INT ),
                        scal.AttrExpr ( "l_discount" )
                    )
                ),
                alg.join (
                    [ ( "n1.n_nationkey", "s_nationkey" ), ( "s_suppkey", "l_suppkey" ) ],
                    alg.scan ( "supplier" ),
                    alg.join (
                        ( "l_orderkey", "o_orderkey" ),            
                        alg.join (
                            ( "c_custkey", "o_custkey" ),
                            alg.join (
                                ( "n2.n_nationkey", "c_nationkey" ),
                                alg.crossjoin (
                                    scal.OrExpr (  
                                        scal.AndExpr (  
                                            scal.EqualsExpr ( 
                                                scal.AttrExpr ( "n1.n_name" ), 
                                                scal.ConstExpr ( "GERMANY", Type.STRING ) ), 
                                            scal.EqualsExpr ( 
                                                scal.AttrExpr ( "n2.n_name" ), 
                                                scal.ConstExpr ( "FRANCE", Type.STRING ) ), 
                                        ),
                                        scal.AndExpr (  
                                            scal.EqualsExpr ( 
                                                scal.AttrExpr ( "n1.n_name" ), 
                                                scal.ConstExpr ( "FRANCE", Type.STRING ) ), 
                                            scal.EqualsExpr ( 
                                                scal.AttrExpr ( "n2.n_name" ), 
                                                scal.ConstExpr ( "GERMANY", Type.STRING ) ), 
                                        )
                                    ),
                                    alg.scan ( "nation", "n1" ),
                                    alg.scan ( "nation", "n2" )
                                ),
                                alg.scan ( "customer" )
                            ),
                            alg.scan ( "orders" )
                        ),
                        alg.selection (
                            scal.AndExpr (
                                scal.LargerEqualExpr (
                                    scal.AttrExpr ( "l_shipdate" ),
                                    scal.ConstExpr ( "19950101", Type.DATE ) 
                                ),
                                scal.SmallerEqualExpr (
                                    scal.AttrExpr ( "l_shipdate" ),
                                    scal.ConstExpr ( "19961231", Type.DATE ) 
                                )
                            ),
                            alg.scan ( "lineitem" )
                        )
                    )
                )
            )
        )
    )
)
