from dogqc.cudalang import *


class Timestamp ( object ):
  
    id=0
    
    def __init__ ( self, name, code ):
        self.label = name
        self.startTime = "start_" + name.replace(" ", "_") + str(Timestamp.id)
        self.stopTime = "stop_" + name.replace(" ", "_") + str(Timestamp.id)
        self.code = code
        self.code.add("std::clock_t " + self.startTime + " = std::clock();")
        Timestamp.id += 1

    def stop ( self ):
        self.code.add("std::clock_t " + self.stopTime + " = std::clock();")
    
    def printTime(self ):
        self.code.add ( "printf ( \"%32s: %8.1f ms\\n\", \"" + self.label + "\", (" + self.stopTime + " - " + self.startTime + ") / (double) (CLOCKS_PER_SEC / 1000) );" )


class Code ( object ):

    def __init__( self ):
        self.content = ""
        self.timestamps = []
        self.hasCode = False

    def add(self, line):
        self.content = self.content + str(line)
        if isinstance ( line, str ):
            self.content += "\n"
        self.hasCode = True
    
    def addFragment ( self, fragment ):
        if not fragment.hasCode:
            return
        self.add( fragment ) 
        self.add("")
    
    def addTimedFragment ( self, fragment, name ):
        if not fragment.hasCode:
            return
        ts = Timestamp ( name, self )
        self.add( fragment ) 
        ts.stop ()    
        self.add("")
        self.timestamps.append ( ts )
         
    def addCudaFragment ( self, fragment, name ):
        if not fragment.hasCode:
            return
        ts = Timestamp ( name, self )
        self.add( fragment ) 
        emit ( deviceSynchronize(), self )
        ts.stop ()    
        cudaErrorCheck( name, self )
        self.add("")
        self.timestamps.append ( ts )

    def __str__ ( self ):
        return self.content



