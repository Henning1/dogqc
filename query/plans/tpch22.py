alg.aggregation ( 
    [ "cntrycode" ],
    [ ( Reduction.COUNT, "", "numcust" ),
    ( Reduction.SUM, "c_acctbal", "totacctbal" ) ],
    alg.map ( 
        "cntrycode",
        scal.SubstringExpr ( 
            scal.AttrExpr ( "c_phone" ),
            scal.ConstExpr ( "1", Type.INT ),
            scal.ConstExpr ( "2", Type.INT )
        ),
        alg.antijoin (
            [ ( "o_custkey", "c_custkey" ) ],
            None,
            alg.scan ( "orders" ), 
            alg.crossjoin (
                scal.LargerExpr ( 
                    scal.AttrExpr ( "c2.c_acctbal" ), 
                    scal.AttrExpr ( "avg" )
                ),
                alg.aggregation ( 
                    [],
                    [ ( Reduction.AVG, "c1.c_acctbal", "avg" ) ],
                    alg.selection (
                        scal.AndExpr (
                            scal.LargerExpr ( 
                                scal.AttrExpr ( "c1.c_acctbal" ), 
                                scal.ConstExpr ( "0.00", Type.DOUBLE )
                            ),
                            scal.InExpr ( 
                                scal.SubstringExpr ( 
                                    scal.AttrExpr ( "c1.c_phone" ),
                                    scal.ConstExpr ( "1", Type.INT ),
                                    scal.ConstExpr ( "2", Type.INT )
                                ),
                                [
                                    scal.ConstExpr ( "13", Type.STRING ),
                                    scal.ConstExpr ( "31", Type.STRING ),
                                    scal.ConstExpr ( "23", Type.STRING ),
                                    scal.ConstExpr ( "29", Type.STRING ),
                                    scal.ConstExpr ( "30", Type.STRING ),
                                    scal.ConstExpr ( "18", Type.STRING ),
                                    scal.ConstExpr ( "17", Type.STRING )
                                ]
                            ) 
                        ),
                        alg.scan ( "customer", "c1" )
                    )
                ),
                alg.selection ( 
                    scal.InExpr ( 
                        scal.SubstringExpr ( 
                            scal.AttrExpr ( "c2.c_phone" ),
                            scal.ConstExpr ( "1", Type.INT ),
                            scal.ConstExpr ( "2", Type.INT )
                        ),
                        [
                            scal.ConstExpr ( "13", Type.STRING ),
                            scal.ConstExpr ( "31", Type.STRING ),
                            scal.ConstExpr ( "23", Type.STRING ),
                            scal.ConstExpr ( "29", Type.STRING ),
                            scal.ConstExpr ( "30", Type.STRING ),
                            scal.ConstExpr ( "18", Type.STRING ),
                            scal.ConstExpr ( "17", Type.STRING )
                        ]
                    ),
                    alg.scan ( "customer", "c2" )
                )
            )
        )
    )
)


