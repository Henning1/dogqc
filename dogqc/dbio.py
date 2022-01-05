import subprocess
import pickle
import os

import dogqc.identifier as ident
from dogqc.attributes import Attribute 
from dogqc.codegen import Code, CodeGenerator, Variable
from dogqc.cudalang import * 
from dogqc.types import Type
from dogqc.codegen import CType


def dbAccess ( schema, dbpath, csvpath, doReload=False, waitEnter=False ):
    if not os.path.exists ( dbpath ):
        os.makedirs ( dbpath )
    sizedSchema = Importer.retrieveTableSizes ( schema, csvpath )
    acc = Accessor ( dbpath, sizedSchema )
    if acc.checkLoaded ( dbpath, sizedSchema ):
        acc.schema = Accessor.loadSchema ( dbpath )
    if not acc.checkLoaded ( dbpath, sizedSchema ) or doReload:
        if waitEnter:
            input("Press Enter to OVERWRITE database...")
        acc.initDB ()
        im = Importer ( acc, csvpath )
        im.loadDatabase ()
        acc.writeSchema ()
    return acc


class Accessor ( object ):
   
    def __init__ ( self, dbpath, schema ):
        self.dbpath = dbpath
        self.schema = schema

    def initDB ( self ):
        # clear folder
        #  - todo
        pass

    @staticmethod
    def checkLoaded ( dbpath, schema ): 
        loadedSchema = Accessor.loadSchema ( dbpath )
        if loadedSchema is None:
            return False
        else:
            for tableName, tableDict in loadedSchema.items():
                if tableName == "dateformat":
                    continue
                del tableDict["charSizes"]
            return loadedSchema == schema

    @staticmethod
    def loadSchema ( dbpath ): 
        spath = dbpath + "/" + 'dbschema.pickle'
        if not os.path.exists ( spath ):
            return None
        with open( spath, 'rb') as handle:
            schema = pickle.load(handle)
        return schema

    def writeSchema ( self ):
        with open( self.dbpath + "/" + 'dbschema.pickle', 'wb') as handle:
            pickle.dump(self.schema, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def getCodeAccessDatabase ( self, codegen, attributes ):
        code = Code()
        for a in attributes:
            cols = a.inputVariables ( codegen )
            colsFile = a.variables ( codegen )
            for c, f in zip ( cols, colsFile ):
                c.declarePointer( code )
                emit ( assign ( c.get(), mmapFile ( c.dataType,  self.file ( f ) ) ), code )
                emit ( unmapFile ( a.name ) , self.codegen.end )
        return code

    def isHostVector ( self ):
        return False

    def getNumBytes ( self, column ):
        with open ( self.file ( column ), "rb" ) as f:
            sizebin = f.read(8)
            sizenum = int.from_bytes ( sizebin, byteorder='little' )
        return sizenum
    
    def file ( self, var ):
        return self.dbpath + '/' + var.get()




class CSVCodegen ( object ):

    i = 0
    
    def getInit ( self, filename, numColumns ): 
        self.varname = "reader" + str(CSVCodegen.i)
        initCode = "io::CSVReader <" + str(numColumns) + ", io::trim_chars<' '>, io::no_quote_escape<'|'> > "
        initCode += self.varname + "(\"" + filename + "\");"
        CSVCodegen.i += 1
        return initCode

    def __init__ ( self, filename, numColumns, code ):
        self.code = code
        code.add ( self.getInit ( filename, numColumns ) )
        self.code = code
        self.numColumns = numColumns
        self.filename = filename
  
    def reset ( self ):
        self.code.add ( self.getInit (self.filename, self.numColumns ) )

    def getLine( self, parsevars ):
        return self.varname + ".read_row(" + ",".join( parsevars ) + ")"



class Importer ( object ):

    def __init__ ( self, accessor, csvpath ):
        self.csvpath = csvpath + "/"
        self.acc = accessor
        self.schema = accessor.schema

    @staticmethod
    def retrieveTableSizes ( schema, csvpath ):
        # get table sizes from csv files
        for table, attributes in schema.items():
            if table is "dateformat":
                continue
            csvFile = csvpath + '/' + table + ".tbl"
            if not os.path.isfile(csvFile):
                print ( "File: " + csvFile + " not found. Quit." )
                quit()
            cmd = [ 'wc', '-l', csvFile ]
            output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
            numRows = int ( output.decode('utf8').split(" ")[0] )
            schema[table]["size"] = numRows
        return schema.copy()

    def loadDatabase ( self ):
        codegen = CodeGenerator ( CType.FP32 )
        if "dateformat" in self.schema.keys():
            codegen.types.add ( self.schema["dateformat"] )
        self.codegen = codegen
        tableReaderCode = self.getCodeReadDatabase ( ) 
        codegen.read.add(tableReaderCode)
        codegen.compileCpu("loadDB")
        codegen.execute()
        self.annotateCharSizes ( self.schema )

    def annotateCharSizes ( self, schema ):
        for tableName, tableDict in schema.items():
            if tableName is "dateformat":
                continue
            tableDict [ "charSizes" ] = dict()
            for attributeName, dataType in tableDict["attributes"].items():
                if dataType == Type.STRING:
                    att = Attribute ( tableDict, attributeName, dataType ) 
                    col = att.variables ( self.codegen, False )[1] # char column
                    filename = self.acc.file ( col )
                    tableDict [ "charSizes" ] [ attributeName ] = os.path.getsize ( filename ) 

    def getCodeReadDatabase ( self ):
        code = Code()
        for table, attributes in self.schema.items():
            if table is "dateformat":
                continue
            code.add ( self.getCodeReadTable ( self.schema[table] ) )
        return code.content

    def getCodeReadTable ( self, table ):
        code = Code()

        tableName = table["name"]
        tableSize = str(table["size"])
        csvFilename = self.csvpath + tableName + ".tbl"
        hasStringAttributes = False # init to False

        # csv reader with one extra column for terminal delimiter
        reader = CSVCodegen ( csvFilename, table["numColumns"] + 1, code)
        simpleParseList = []
        stringParseList = []
        csvParseArgs = []

        # create columns and variables for parsing
        for attributeName, dataType in table [ "attributes" ].items():
            parseType = self.codegen.langType ( dataType )
            if dataType == Type.DATE:
                parseType = "std::string"
            if dataType == Type.STRING:
                parseType = "std::string"
            parseVar = Variable.val ( parseType, attributeName )
            parseVar.declare ( code )
            att = Attribute ( table, attributeName, dataType ) 
            csvParseArgs.append ( parseVar.get ( ) )
            colLen = int ( tableSize )
            if ( att.attType == Type.STRING ):
                charLenVar = Variable.val ( CType.SIZE, ident.charLenVar ( att ) ) 
                charLenVar.declareAssign ( intConst(0), code )
                stringParseList.append ( ( att, parseVar, charLenVar ) )
                hasStringAttributes = True
                # add another offset for ending of the last string
                colLen += 1
            else:
                simpleParseList.append ( ( att, parseVar ) ) 
            
            col = att.variables ( self.codegen, False )[0] # get first column of each attribute
            filename = self.acc.file ( col )
            col.declarePointer ( code )
            emit ( assign ( col.get(), mmapMalloc ( col.dataType, colLen, filename ) ), code )
            if ( att.attType == Type.STRING ):
                emit ( assign ( col.arrayAccess ( intConst(0) ), intConst(0) ), code )
        
        # parser needs additional empty variable due to terminal delimiter
        nothing = Variable.val ( CType.CHAR, ident.nothingVar ( tableName ) )
        nothing.declarePointer( code ) 
        csvParseArgs.append(nothing.get())
         
        # loop over csv 
        with CountingWhileLoop ( reader.getLine ( csvParseArgs ), code ) as loop:
            with IfClause ( larger ( loop.countVar, tableSize ), code ):
                printError ( code )

            # fill simple columns with data and count string sizes
            for att, parseVar in simpleParseList:
                col = att.variables ( self.codegen, False )[0] # get first column of each attribute
                if att.attType == Type.DATE:
                    emit ( assign ( col.arrayAccess ( loop.countVar ), "toDate (" + str(parseVar) + ".c_str())" ), code ) 
                else:
                    emit ( assign ( col.arrayAccess ( loop.countVar ), parseVar.get() ), code ) 

            for att, parseVar, charLenVar in stringParseList:
                col = att.variables ( self.codegen, False )[0] # first column with offset
                emit ( assignAdd ( charLenVar.get(), parseVar.length() ), code )
                emit ( assign ( col.arrayAccess ( add ( loop.countVar, intConst(1) ) ), charLenVar.get() ), code )

        if hasStringAttributes:        
            # size of string columns is now known, allocate
            for att, parseVar, charLenVar in stringParseList:
                col = att.variables ( self.codegen, False )[1] # get second column (currently relevant for strings)
                filename = self.acc.file ( col )
                col.declarePointer ( code )
                emit ( assign ( col.get ( ), mmapMalloc ( col.dataType, charLenVar.get(), filename ) ), code )

            # reset csv reader
            reader.reset( )
            # loop over csv second time to fill strings
            with CountingWhileLoop ( reader.getLine ( csvParseArgs ), code ) as loop:
                for att, parseVar, charLenVar in stringParseList:
                    col = att.variables ( self.codegen, False )
                    emit ( strcpy ( addressof ( col[1].arrayAccess ( col[0].arrayAccess ( loop.countVar ) ) ), parseVar.cstr() ), code )
        
        return code.content     
