{
    5 : {
        "operator" : "join", 
        "multimatch" : False
    },
    15 : {
        "operator" : "join", 
        "multimatch" : False
    },
    9 : {
        #join p_partkey=l_partkey
        "operator" : "join", 
        "selectivity" : 0.01,
        "multimatch" : False
    },
    11 : {
        # orderdate between
        "operator" : "selection",
        "selectivity" : 0.1
    },
    14 : {
        # o_custkey=c_custkey
        "operator" : "join",
        "selectivity" : 0.25
    }
}
