
alg.aggregation (
    [ "l_shipmode" ],
    [ ( Reduction.SUM, "highline", "high_line_count" ),
      ( Reduction.SUM, "lowline", "low_line_count" ) ],
    alg.map (
        "highline",
        scal.CaseExpr (
            [ 
                ( 
                    scal.OrExpr (
                        scal.EqualsExpr ( 
                            scal.AttrExpr ( "o_orderpriority"), 
                            scal.ConstExpr ( "1-URGENT", Type.STRING )
                        ),
                        scal.EqualsExpr ( 
                            scal.AttrExpr ( "o_orderpriority"), 
                            scal.ConstExpr ( "2-HIGH", Type.STRING )
                        )
                    ),
                    scal.ConstExpr ( "1", Type.INT ) 
                )
            ],
            scal.ConstExpr ( "0", Type.INT ) 
        ),
        alg.map (
            "lowline",
            scal.CaseExpr (
                [ 
                    ( 
                        scal.AndExpr (
                            scal.NotExpr ( scal.EqualsExpr ( 
                                scal.AttrExpr ( "o_orderpriority"), 
                                scal.ConstExpr ( "1-URGENT", Type.STRING )
                            ) ),
                            scal.NotExpr ( scal.EqualsExpr ( 
                                scal.AttrExpr ( "o_orderpriority"), 
                                scal.ConstExpr ( "2-HIGH", Type.STRING )
                            ) ),
                        ),
                        scal.ConstExpr ( "1", Type.INT ) 
                    )
                ],
                scal.ConstExpr ( "0", Type.INT ) 
            ),
            alg.join (
                ( "o_orderkey", "l_orderkey" ),
                alg.selection (
                    scal.AndExpr (
                        scal.InExpr (
                            scal.AttrExpr ( "l_shipmode" ),
                            [
                                scal.ConstExpr ( "MAIL", Type.STRING ),
                                scal.ConstExpr ( "SHIP", Type.STRING )
                            ]
                        ),
                        scal.AndExpr (
                            scal.SmallerExpr (
                                scal.AttrExpr ( "l_commitdate" ),
                                scal.AttrExpr ( "l_receiptdate" )
                            ),
                            scal.AndExpr (
                                scal.SmallerExpr (
                                    scal.AttrExpr ( "l_shipdate" ),
                                    scal.AttrExpr ( "l_commitdate" )
                                ),
                                scal.AndExpr (
                                    scal.LargerEqualExpr (
                                        scal.AttrExpr ( "l_receiptdate" ),
                                        scal.ConstExpr ( "19940101", Type.DATE )
                                    ),
                                    scal.SmallerExpr (
                                        scal.AttrExpr ( "l_receiptdate" ),
                                        scal.ConstExpr ( "19950101", Type.DATE )
                                    )
                                )
                            )
                        )
                    ),
                    alg.scan ( "lineitem" )
                ),
                alg.scan ( "orders" )
            )
        )
    )
)

