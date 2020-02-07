alg.aggregation (
    [ "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment" ],
    [ ( Reduction.SUM, "rev", "revenue" ) ],
    alg.map (
        "rev",
        scal.MulExpr ( 
            scal.AttrExpr ( "l_extendedprice" ),
            scal.SubExpr ( 
                scal.ConstExpr ( "1.0f", Type.FLOAT ),
                scal.AttrExpr ( "l_discount" )
            )
        ),
        alg.join (
            ( "l_orderkey", "o_orderkey" ),
            alg.join (
                ( "c_nationkey", "n_nationkey" ),
                alg.scan ( "nation" ),
                alg.join (
                    ( "o_custkey", "c_custkey" ),
                    alg.selection (
                        scal.AndExpr (
                            scal.LargerEqualExpr (
                                scal.AttrExpr ( "o_orderdate" ),
                                scal.ConstExpr ( "19931001", Type.DATE )
                            ),
                            scal.SmallerExpr (
                                scal.AttrExpr ( "o_orderdate" ),
                                scal.ConstExpr ( "19940101", Type.DATE )
                            )
                        ),
                        alg.scan ( "orders" )
                    ),
                    alg.scan ( "customer" )
                )
            ),
            alg.selection (
                scal.EqualsExpr (
                    scal.AttrExpr ( "l_returnflag" ),
                    scal.ConstExpr ( "R", Type.CHAR )
                ),
                alg.scan ( "lineitem" )
            )
        )
    )
)
