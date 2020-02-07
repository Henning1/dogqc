{
    3 : {
        "operator" : "aggregation", 
        "numgroups" : 25000000
    }, 
    4 : {
        "operator" : "selection", 
        "selectivity" : 0.5
    }, 
    6 : {
        "operator" : "semijoin", 
        "selectivity" : 0.25,
        "multimatch" : False
    },
    7 : {
        "operator" : "join", 
        "multimatch" : False
    },
    9 : {
        "operator" : "join", 
        "selectivity" : 0.34,
        "multimatch" : False
    },
    10 : {
        "operator" : "aggregation", 
        "numgroups" : 2500000
    }
}
