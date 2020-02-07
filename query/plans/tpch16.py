alg.projection (
    [ "p_brand", "p_type", "p_size", "supp_count" ],
    alg.aggregation (
        [ "p_brand", "p_type", "p_size" ],
        [ ( Reduction.COUNT, "", "supp_count" ) ],
        # add distinct attribute to grouping
        alg.aggregation (
            [ "p_brand", "p_type", "p_size", "ps_suppkey" ],
            [ ( Reduction.COUNT, "", "supp_count_resolvedistinct" ) ],
            alg.antijoin (
                ( "ps_suppkey", "s_suppkey" ), None,
                alg.selection (
                    scal.LikeExpr ( 
                        scal.AttrExpr ( "s_comment" ),
                        scal.ConstExpr ( "%Customer%Complaints%", Type.STRING )
                    ),
                    alg.scan ( "supplier" )
                ),
                alg.join (
                    ( "p_partkey", "ps_partkey" ),
                    alg.selection ( 
                        scal.AndExpr ( 
                            scal.AndExpr (
                                scal.NotExpr ( 
                                    scal.EqualsExpr ( 
                                        scal.AttrExpr ( "p_brand" ),
                                        scal.ConstExpr ( "Brand#45", Type.STRING )
                                    )
                                ),
                                scal.NotExpr ( 
                                    scal.LikeExpr ( 
                                        scal.AttrExpr ( "p_type" ),
                                        scal.ConstExpr ( "MEDIUM POLISHED%", Type.STRING )
                                    )
                                )
                            ),
                            scal.InExpr (
                                scal.AttrExpr ( "p_size" ),
                                [
                                    scal.ConstExpr ( "49", Type.INT ),
                                    scal.ConstExpr ( "14", Type.INT ),
                                    scal.ConstExpr ( "23", Type.INT ),
                                    scal.ConstExpr ( "45", Type.INT ),
                                    scal.ConstExpr ( "19", Type.INT ),
                                    scal.ConstExpr (  "3", Type.INT ),
                                    scal.ConstExpr ( "36", Type.INT ),
                                    scal.ConstExpr (  "9", Type.INT ),
                                ]
                            )
                        ),
                        alg.scan ( "part" )
                    ),
                    alg.scan ( "partsupp" )
                )
            )
        )
    )
)
