#todo: check result with double precision

alg.aggregation ( 
    [],
    [ ( Reduction.SUM, "rev", "revenue" ),
      ( Reduction.COUNT, "rev", "count" ) ],
    alg.map (
        "rev",
        scal.MulExpr ( 
            scal.AttrExpr ( "l_extendedprice" ),
            scal.AttrExpr ( "l_discount" )
        ),
        alg.selection (
            scal.AndExpr (
                scal.LargerEqualExpr (
                    scal.AttrExpr ( "l_shipdate" ),
                    scal.ConstExpr ( "19940101", Type.DATE )
                ),
                scal.AndExpr (
                    scal.SmallerExpr (
                        scal.AttrExpr ( "l_shipdate" ),
                        scal.ConstExpr ( "19950101", Type.DATE )
                    ),
                    scal.AndExpr (
                        scal.LargerEqualExpr (
                            scal.AttrExpr ( "l_discount" ),
                            scal.ConstExpr ( "0.05", Type.DOUBLE )
                        ),
                        scal.AndExpr (
                            scal.SmallerEqualExpr (
                                scal.AttrExpr ( "l_discount" ),
                                scal.ConstExpr ( "0.07", Type.DOUBLE )
                            ),
                            scal.SmallerExpr (
                                scal.AttrExpr ( "l_quantity" ),
                                scal.ConstExpr ( "24", Type.DOUBLE )
                            )
                        )
                    )
                )
            ),
            alg.scan ( "lineitem" )
        )
    )
)

