import os

# enables comments and blank lines in plan files
def loadScript ( path ):
    # read query plan
    with open ( path, "r") as planFile:
        script = planFile.read()
        script = os.linesep.join([s for s in script.splitlines() if s and s != "\n" ])
        script = os.linesep.join([s for s in script.splitlines() if not s.lstrip().startswith('#')])
    if len ( script ) > 0:
        return script
    else:
        return "{}"

def listWrap ( elem ):
    if isinstance ( elem, list ):
        return elem
    else:
        return [elem] 

def intToHuman ( number ):
    B = 1000*1000*1000
    M = 1000*1000
    K = 1000
    if ( number >= B ):
        numStr = f"{number/B:.1f}" + "&#8239;B"
    elif ( number >= M ):
        numStr = f"{number/M:.1f}" + "&#8239;M"
    elif ( number >= K ):
        numStr = f"{number/K:.1f}" + "&#8239;K"
    else:
        numStr = str ( int ( number ) )
    return numStr

def formatDOTStr ( title, sub ):
    dotstr = '''<'''
    if title != None:
        dotstr += title
    if sub != None and sub != "":
        sub = listWrap ( sub )
        dotstr += '''<FONT POINT-SIZE="10">'''        
        for line in sub:
            dotstr += '''<br/> ''' + line
        dotstr += '''</FONT>'''
    dotstr += '''>'''
    return dotstr
