#alg.aggregation (
#    [ "ps_comment" ],
#    [ ( Reduction.COUNT, "", "num" ) ],
#    alg.scan ( "partsupp" )
#)

#alg.aggregation (
#    [ "o_comment" ],
#    [ ( Reduction.COUNT, "", "num" ) ],
#    alg.scan ( "orders" )
#)

alg.aggregation (
    [ "l_comment" ],
    [ ( Reduction.COUNT, "", "num" ) ],
    alg.scan ( "lineitem" )
)

#alg.aggregation (
#    [ "l_shipmode" ],
#    [ ( Reduction.COUNT, "", "num" ) ],
#    alg.scan ( "lineitem" )
#)

