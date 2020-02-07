alg.projection ( 
        [ "l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "avg_qty", "avg_price", "avg_disc", "count_order" ],
    alg.aggregation ( 
        [ "l_returnflag", "l_linestatus" ], 
        [ ( Reduction.SUM, "l_quantity", "sum_qty" ),
          ( Reduction.SUM, "l_extendedprice", "sum_base_price" ),
          ( Reduction.SUM, "disc_price", "sum_disc_price" ),
          ( Reduction.SUM, "charge", "sum_charge" ),
          ( Reduction.AVG, "l_quantity", "avg_qty" ),
          ( Reduction.AVG, "l_extendedprice", "avg_price" ),
          ( Reduction.AVG, "l_discount", "avg_disc" ),
          ( Reduction.COUNT, "", "count_order" ) ],
        alg.map (
            "disc_price",
            scal.MulExpr (
                scal.AttrExpr ( "l_extendedprice" ),
                scal.SubExpr (
                    scal.ConstExpr ( "1.0f", Type.FLOAT ), 
                    scal.AttrExpr ( "l_discount" )
                )
            ),
            alg.map (
                "charge",
                scal.MulExpr (
                    scal.MulExpr (
                        scal.AttrExpr ( "l_extendedprice" ),
                        scal.SubExpr (
                            scal.ConstExpr ( "1.0f", Type.FLOAT ), 
                            scal.AttrExpr ( "l_discount" )
                        )
                    ),
                    scal.AddExpr (
                        scal.ConstExpr ( "1.0f", Type.FLOAT ), 
                        scal.AttrExpr ( "l_tax" )
                    )
                ),
                alg.selection (
                    scal.SmallerEqualExpr (
                        scal.AttrExpr ( "l_shipdate" ),
                        scal.ConstExpr ( "19980902", Type.DATE ) 
                    ),
                    alg.scan ( "lineitem" )
                )
            )
        )
    )
)
