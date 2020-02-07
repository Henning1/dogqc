alg.projection (
    [ "s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment" ],
    alg.join (
        ( "s2.s_suppkey", "ps2.ps_suppkey" ),
        alg.join ( 
            ( "s_nationkey", "n_nationkey" ),
            alg.join (
                ( "r_regionkey", "n_regionkey" ),
                alg.selection (
                    scal.EqualsExpr (
                        scal.AttrExpr ( "r_name" ),
                        scal.ConstExpr ( "EUROPE", Type.STRING )
                    ),
                    alg.scan ( "region" )
                ),
                alg.scan ( "nation" )
            ),
            alg.scan ( "supplier", "s2" )
        ),
        alg.selection (
            scal.EqualsExpr ( 
                scal.AttrExpr ( "min_supplycost" ),
                scal.AttrExpr ( "ps2.ps_supplycost" )
            ),
            alg.join ( 
                [ ( "ps2.ps_partkey", "ps1.ps_partkey" ) ],
                alg.join (
                    ( "p_partkey", "ps_partkey" ),
                    alg.aggregation (
                        [ "ps_partkey" ],
                        [ ( Reduction.MIN, "ps_supplycost", "min_supplycost" ) ],
                        alg.join (
                            ( "s_suppkey", "ps_suppkey" ),
                            alg.join ( 
                                ( "s_nationkey", "n_nationkey" ),
                                alg.join (
                                    ( "r_regionkey", "n_regionkey" ),
                                    alg.selection (
                                        scal.EqualsExpr (
                                            scal.AttrExpr ( "r_name" ),
                                            scal.ConstExpr ( "EUROPE", Type.STRING )
                                        ),
                                        alg.scan ( "region" )
                                    ),
                                    alg.scan ( "nation" )
                                ),
                                alg.scan ( "supplier" )
                            ),
                            alg.semijoin (
                                ( "ps_partkey", "earlyp.p_partkey" ), None,
                                alg.selection (
                                    scal.AndExpr (
                                        scal.LikeExpr (
                                            scal.AttrExpr ( "p_type" ),
                                            scal.ConstExpr ( "%BRASS", Type.STRING )
                                        ),
                                        scal.EqualsExpr (
                                            scal.AttrExpr ( "p_size" ),
                                            scal.ConstExpr ( "15", Type.INT )
                                        )
                                    ),
                                    alg.scan ( "part", "earlyp" )
                                ),
                                alg.scan ( "partsupp", "ps1" )
                            )
                        )
                    ),
                    alg.selection (
                        scal.AndExpr (
                            scal.LikeExpr (
                                scal.AttrExpr ( "p_type" ),
                                scal.ConstExpr ( "%BRASS", Type.STRING )
                            ),
                            scal.EqualsExpr (
                                scal.AttrExpr ( "p_size" ),
                                scal.ConstExpr ( "15", Type.INT )
                            )
                        ),
                        alg.scan ( "part" )
                    )
                ),
                alg.scan ( "partsupp", "ps2" )
            )
        )
    )
)
