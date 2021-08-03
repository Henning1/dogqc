# Query 14+19
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
                    alg.join (
                        ( "l_partkey", "p_partkey" ),
                        alg.scan ( "lineitem" ) ,
                        alg.scan ( "part" )
                    )
                )
            )
        # )
