
alg.aggregation (
    [],
    [ ( Reduction.SUM, "rev", "revenue" ) ],
    alg.map (
        "rev",
        scal.MulExpr ( 
            scal.AttrExpr ( "l_extendedprice" ),
            scal.SubExpr ( 
                scal.ConstExpr ( "1.0", Type.DOUBLE ),
                scal.AttrExpr ( "l_discount" )
            )
        ),
        alg.selection (
            scal.OrExpr (
                scal.OrExpr (
                    #or1
                    scal.AndExpr (
                        scal.AndExpr (
                            scal.AndExpr (
                                scal.AndExpr (
                                    scal.AndExpr (
                                        scal.AndExpr (
                                            scal.AndExpr (
                                                scal.EqualsExpr (
                                                    scal.AttrExpr ( "p_brand" ),
                                                    scal.ConstExpr ( "Brand#12", Type.STRING )
                                                ),
                                                scal.InExpr (
                                                    scal.AttrExpr ( "p_container" ),
                                                    [
                                                        scal.ConstExpr ( "SM CASE", Type.STRING ),
                                                        scal.ConstExpr ( "SM BOX", Type.STRING ),
                                                        scal.ConstExpr ( "SM PACK", Type.STRING ),
                                                        scal.ConstExpr ( "SM PKG", Type.STRING )
                                                    ]
                                                )
                                            ),
                                            scal.LargerEqualExpr (
                                                scal.AttrExpr ( "l_quantity" ),
                                                scal.ConstExpr ( "1.0f", Type.DOUBLE )
                                            )
                                        ),
                                        scal.SmallerEqualExpr (
                                            scal.AttrExpr ( "l_quantity" ),
                                            scal.ConstExpr ( "11.0f", Type.DOUBLE )
                                        )
                                    ),
                                    scal.LargerEqualExpr (
                                        scal.AttrExpr ( "p_size" ),
                                        scal.ConstExpr ( "1", Type.DOUBLE )
                                    )
                                ),
                                scal.SmallerEqualExpr (
                                    scal.AttrExpr ( "p_size" ),
                                    scal.ConstExpr ( "5", Type.DOUBLE )
                                )
                            ),
                            scal.InExpr (
                                scal.AttrExpr ( "l_shipmode" ),
                                [
                                    scal.ConstExpr ( "AIR", Type.STRING ),
                                    scal.ConstExpr ( "AIR REG", Type.STRING ),
                                ]
                            ),
                        ),
                        scal.EqualsExpr (
                            scal.AttrExpr ( "l_shipinstruct" ),
                            scal.ConstExpr ( "DELIVER IN PERSON", Type.STRING )
                        )
                    ),
                    #or2
                    scal.AndExpr (
                        scal.AndExpr (
                            scal.AndExpr (
                                scal.AndExpr (
                                    scal.AndExpr (
                                        scal.AndExpr (
                                            scal.AndExpr (
                                                scal.EqualsExpr (
                                                    scal.AttrExpr ( "p_brand" ),
                                                    scal.ConstExpr ( "Brand#23", Type.STRING )
                                                ),
                                                scal.InExpr (
                                                    scal.AttrExpr ( "p_container" ),
                                                    [
                                                        scal.ConstExpr ( "MED BAG", Type.STRING ),
                                                        scal.ConstExpr ( "MED BOX", Type.STRING ),
                                                        scal.ConstExpr ( "MED PKG", Type.STRING ),
                                                        scal.ConstExpr ( "MED PACK", Type.STRING )
                                                    ]
                                                )
                                            ),
                                            scal.LargerEqualExpr (
                                                scal.AttrExpr ( "l_quantity" ),
                                                scal.ConstExpr ( "10.0f", Type.DOUBLE )
                                            )
                                        ),
                                        scal.SmallerEqualExpr (
                                            scal.AttrExpr ( "l_quantity" ),
                                            scal.ConstExpr ( "20.0f", Type.DOUBLE )
                                        )
                                    ),
                                    scal.LargerEqualExpr (
                                        scal.AttrExpr ( "p_size" ),
                                        scal.ConstExpr ( "1", Type.DOUBLE )
                                    )
                                ),
                                scal.SmallerEqualExpr (
                                    scal.AttrExpr ( "p_size" ),
                                    scal.ConstExpr ( "10", Type.DOUBLE )
                                )
                            ),
                            scal.InExpr (
                                scal.AttrExpr ( "l_shipmode" ),
                                [
                                    scal.ConstExpr ( "AIR", Type.STRING ),
                                    scal.ConstExpr ( "AIR REG", Type.STRING ),
                                ]
                            ),
                        ),
                        scal.EqualsExpr (
                            scal.AttrExpr ( "l_shipinstruct" ),
                            scal.ConstExpr ( "DELIVER IN PERSON", Type.STRING )
                        )
                    ),
                ),
                #or3
                scal.AndExpr (
                    scal.AndExpr (
                        scal.AndExpr (
                            scal.AndExpr (
                                scal.AndExpr (
                                    scal.AndExpr (
                                        scal.AndExpr (
                                            scal.EqualsExpr (
                                                scal.AttrExpr ( "p_brand" ),
                                                scal.ConstExpr ( "Brand#34", Type.STRING )
                                            ),
                                            scal.InExpr (
                                                scal.AttrExpr ( "p_container" ),
                                                [
                                                    scal.ConstExpr ( "LG CASE", Type.STRING ),
                                                    scal.ConstExpr ( "LG BOX", Type.STRING ),
                                                    scal.ConstExpr ( "LG PACK", Type.STRING ),
                                                    scal.ConstExpr ( "LG PKG", Type.STRING )
                                                ]
                                            )
                                        ),
                                        scal.LargerEqualExpr (
                                            scal.AttrExpr ( "l_quantity" ),
                                            scal.ConstExpr ( "20.0f", Type.DOUBLE )
                                        )
                                    ),
                                    scal.SmallerEqualExpr (
                                        scal.AttrExpr ( "l_quantity" ),
                                        scal.ConstExpr ( "30.0f", Type.DOUBLE )
                                    )
                                ),
                                scal.LargerEqualExpr (
                                    scal.AttrExpr ( "p_size" ),
                                    scal.ConstExpr ( "1", Type.DOUBLE )
                                )
                            ),
                            scal.SmallerEqualExpr (
                                scal.AttrExpr ( "p_size" ),
                                scal.ConstExpr ( "15", Type.DOUBLE )
                            )
                        ),
                        scal.InExpr (
                            scal.AttrExpr ( "l_shipmode" ),
                            [
                                scal.ConstExpr ( "AIR", Type.STRING ),
                                scal.ConstExpr ( "AIR REG", Type.STRING ),
                            ]
                        ),
                    ),
                    scal.EqualsExpr (
                        scal.AttrExpr ( "l_shipinstruct" ),
                        scal.ConstExpr ( "DELIVER IN PERSON", Type.STRING )
                    )
                )
            ),
            alg.join (
                ( "p_partkey", "l_partkey" ),
                alg.scan ( "part" ),
                alg.scan ( "lineitem" )
            )
        )
    )
)


