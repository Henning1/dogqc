[
alg.createtemp (
    "lineitem_filtered",
    alg.projection ( 
        [ "l_quantity", "l_extendedprice", "l_partkey" ],
        alg.join (
            ( "l_partkey", "p_partkey" ),
            alg.selection (
                scal.AndExpr (
                    scal.EqualsExpr (
                        scal.AttrExpr ( "p_brand" ),
                        scal.ConstExpr ( "Brand#23", Type.STRING )
                    ),
                    scal.EqualsExpr (
                        scal.AttrExpr ( "p_container" ),
                        scal.ConstExpr ( "MED BOX", Type.STRING )
                    )
                ),
                alg.scan ( "part" )
            ),
            alg.scan ( "lineitem" )
        )
    )
),

alg.projection (
    [ "avg_yearly", "count_price" ],
    alg.map (
        "avg_yearly",
        scal.DivExpr ( 
            scal.AttrExpr ( "sum_price" ),
            scal.ConstExpr ( "7.0f", Type.DOUBLE )
        ),
        alg.aggregation (
            [],
            [ ( Reduction.SUM, "l_extendedprice", "sum_price" ),
              ( Reduction.COUNT, "l_extendedprice", "count_price" ) ],
            alg.selection (
                scal.SmallerExpr (
                    scal.AttrExpr ( "l_quantity" ),
                    scal.AttrExpr ( "lim_quan" )
                ),
                alg.map (
                    "lim_quan",
                    scal.MulExpr (
                        scal.AttrExpr ( "avg_quan" ),
                        scal.ConstExpr ( "0.2f", Type.DOUBLE )
                    ),
                    alg.join (
                        ( "l1.l_partkey", "l2.l_partkey" ),
                        alg.aggregation (
                            [ "l_partkey" ],
                            [ ( Reduction.AVG, "l_quantity", "avg_quan" ) ],
                            alg.scan ( "lineitem_filtered", "l1" )
                        ),
                        alg.scan ( "lineitem_filtered", "l2" )
                    )
                )
            )
        )
    )
)
]
