[
alg.createtemp ( 
    "revenue",
    alg.aggregation (
        "l_suppkey",
        [ ( Reduction.SUM, "revenue", "sum_revenue" ) ],
        alg.map ( 
            "revenue",
            scal.MulExpr ( 
                scal.AttrExpr ( "l_extendedprice" ),
                scal.SubExpr ( 
                    scal.ConstExpr ( "1.0", Type.FLOAT ),
                    scal.AttrExpr ( "l_discount" )
                )
            ),
            alg.selection (
                scal.AndExpr (
                    scal.LargerEqualExpr ( 
                        scal.AttrExpr ( "l_shipdate" ),
                        scal.ConstExpr ( "19960101", Type.DATE )
                    ),
                    scal.SmallerExpr ( 
                        scal.AttrExpr ( "l_shipdate" ),
                        scal.ConstExpr ( "19960401", Type.DATE )
                    )
                ),
                alg.scan ( "lineitem" )
            )
        )
    )
),
alg.projection (
    [ "s_suppkey", "s_name", "s_address", "s_phone", "total_revenue" ],
    alg.join (
        ( "l_suppkey", "s_suppkey" ),
        alg.join (
            ( "total_revenue", "sum_revenue" ),
            alg.aggregation (
                [],
                [( Reduction.MAX, "sum_revenue", "total_revenue" )],
                alg.scan ( "revenue", "r1" )
            ),
            alg.scan ( "revenue", "r2" )
        ),
        alg.scan ( "supplier" )
    )
)
]
