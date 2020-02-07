alg.aggregation (
    [ "s_name" ],
    [ ( Reduction.COUNT, "", "numwait" ) ],
    #alg.semijoin ( 
    #    ( "l2.l_orderkey", "l1.l_orderkey" ),
    #    scal.NotExpr ( 
    #        scal.EqualsExpr (
    #            scal.AttrExpr ( "l2.l_suppkey" ),
    #            scal.AttrExpr ( "l1.l_suppkey" )
    #        )
    #    ),
    alg.semijoin ( 
        ( "l2.l_orderkey", "l1.l_orderkey" ),
        scal.NotExpr ( 
            scal.EqualsExpr (
                scal.AttrExpr ( "l2.l_suppkey" ),
                scal.AttrExpr ( "l1.l_suppkey" )
            )
        ),
        alg.scan ( "lineitem", "l2" ),
        alg.antijoin ( 
            ( "l3.l_orderkey", "l1.l_orderkey" ), 
            scal.NotExpr ( 
                scal.EqualsExpr (
                    scal.AttrExpr ( "l3.l_suppkey" ),
                    scal.AttrExpr ( "l1.l_suppkey" )
                )
            ),
            alg.selection (
                scal.LargerExpr (
                    scal.AttrExpr ( "l_receiptdate" ),
                    scal.AttrExpr ( "l_commitdate" )
                ),
                alg.scan ( "lineitem", "l3" )
            ),
            alg.join ( 
                ( "o_orderkey", "l_orderkey" ),
                alg.join (
                    ( "s_suppkey", "l_suppkey" ),
                    alg.join (
                        ( "s_nationkey", "n_nationkey" ),
                        alg.selection (
                            scal.EqualsExpr (
                                scal.AttrExpr ( "n_name" ),
                                scal.ConstExpr ( "SAUDI ARABIA", Type.STRING )
                            ),
                            alg.scan ( "nation" )
                        ),
                        alg.scan ( "supplier" )
                    ),
                    alg.selection (
                        scal.LargerExpr ( 
                            scal.AttrExpr ( "l_receiptdate" ),
                            scal.AttrExpr ( "l_commitdate" )
                        ),
                        alg.scan ( "lineitem", "l1" )
                    )
                ),
                alg.selection ( 
                    scal.EqualsExpr (
                        scal.AttrExpr ( "o_orderstatus" ),
                        scal.ConstExpr ( "F", Type.CHAR )
                    ),
                    alg.scan ( "orders" )
                )
            )
        )
    )
)
