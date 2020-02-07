alg.projection (
    [ "ps_partkey", "sum_suppval2" ],
    alg.crossjoin (
        scal.LargerExpr (
            scal.AttrExpr ( "sum_suppval2" ),
            scal.AttrExpr ( "lim_suppval" )
        ),
        alg.map (
            "lim_suppval",
            scal.MulExpr ( 
                scal.AttrExpr ( "sum_suppval" ),
                scal.ConstExpr ( "0.0001f", Type.DOUBLE )
            ),
            alg.aggregation (
                [],
                [ ( Reduction.SUM, "suppval", "sum_suppval" ) ],
                alg.map (
                    "suppval",
                    scal.MulExpr ( 
                        scal.AttrExpr ( "ps_supplycost" ),
                        scal.AttrExpr ( "ps_availqty" )
                    ),
                    alg.join (
                        ( "ps_suppkey", "s_suppkey" ),
                        alg.join (
                            ( "s_nationkey", "n_nationkey" ),
                            alg.selection (
                                scal.EqualsExpr ( 
                                    scal.AttrExpr ( "n_name" ),
                                    scal.ConstExpr ( "GERMANY", Type.STRING )
                                ),
                                alg.scan ( "nation" )
                            ),
                            alg.scan ( "supplier" )
                        ),
                        alg.scan ( "partsupp" )
                    )
                )
            )
        ),
        alg.aggregation (
            [ "ps_partkey" ],
            [ ( Reduction.SUM, "suppval2", "sum_suppval2" ) ],
            alg.map (
                "suppval2",
                scal.MulExpr ( 
                    scal.AttrExpr ( "ps_supplycost" ),
                    scal.AttrExpr ( "ps_availqty" )
                ),
                alg.join (
                    ( "ps_suppkey", "s_suppkey" ),
                    alg.join (
                        ( "s_nationkey", "n_nationkey" ),
                        alg.selection (
                            scal.EqualsExpr ( 
                                scal.AttrExpr ( "n_name" ),
                                scal.ConstExpr ( "GERMANY", Type.STRING )
                            ),
                            alg.scan ( "nation" )
                        ),
                        alg.scan ( "supplier" )
                    ),
                    alg.scan ( "partsupp" )
                )
            )
        )
    )
)
