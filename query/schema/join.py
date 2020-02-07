import sys
sys.path.insert(0,'..')

from collections import OrderedDict
from dogqc.types import Type 

joinSchema = {}

joinSchema["r_build"] = { "name":"r_build", "size":0, "numColumns":2, "attributes":
                             OrderedDict([
                                 ("r_build",Type.INT),
                                 ("r_linenumber",Type.FLOAT),
                             ])
                         }

joinSchema["s_probe"] = { "name":"s_probe", "size":0, "numColumns":2, "attributes": 
                        OrderedDict([
                            ("s_probe",Type.INT),
                            ("s_linenumber",Type.INT),
                        ])
			}
