{
    7 : {
        "operator" : "join", 
        "multimatch" : False,
    },
    9 : {
        "operator" : "selection", 
        "selectivity" : 0.5
    }, 
    10 : {
        "operator" : "join", 
        "multimatch" : False,
        "selectivity" : 0.04
    }, 
    12 : {
        "operator" : "selection", 
        "selectivity" : 0.48
    }, 
    13 : {
        "operator" : "join", 
        "multimatch" : True,
        "selectivity" : 0.09
    }, 
    3 : {
        "operator" : "selection", 
        "selectivity" : 0.5
    }, 
    14 : {
        "operator" : "antijoin", 
        "selectivity" : 0.02,
        "htSizeFactor" : 1.0,
        "multimatch" : True
    },
    15 : {
        "operator" : "semijoin", 
        "selectivity" : 0.02,
        "htSizeFactor" : 0.5,
        "multimatch" : True
    } 
}
