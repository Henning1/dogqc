import sys
sys.path.insert(0,'..')
import os
from os import path
import importlib
import dogqc.dbio as io
import schema.tpch
from dogqc.util import loadScript

# algebra types
from dogqc.relationalAlgebra import Context
from dogqc.relationalAlgebra import RelationalAlgebra
from dogqc.relationalAlgebra import Reduction
from dogqc.cudaTranslator import CudaCompiler
from dogqc.types import Type
from dogqc.cudalang import CType
import dogqc.scalarAlgebra as scal
from dogqc.kernel import KernelCall
from dogqc.hashJoins import EquiJoinTranslator 


KernelCall.defaultGridSize = 100
KernelCall.defaultBlockSize = 32
EquiJoinTranslator.usePushDownJoin = False


def execute ( alg, plan, qname, buffers=[], profilers=[] ):
    compiler = CudaCompiler ( algebraContext = alg, smArchitecture = "sm_75", decimalRepr = CType.FP32, debug = False )
    compiler.setBuffers ( buffers )
    compiler.setProfilers ( profilers )
    compilerPlan = alg.translateToCompilerPlan ( plan, compiler )
    compiler.showGraph ( compilerPlan )
    compiler.gencode ( compilerPlan )
    compiler.compile ( qname )
    compiler.execute ()


def getBufferPositions ( alg ):
    joinNodes = alg.joinNodes + alg.semiJoinNodes + alg.antiJoinNodes
    singleMatchJoinNodes = [ n for n in joinNodes if not n.multimatch]
    possibleBufferPositions = [ [n.opId] for n in alg.selectionNodes + singleMatchJoinNodes ]
    return sum(possibleBufferPositions, [] )


def main():
    # access database
    if len(sys.argv) < 3:
        print("Please provide the following arguments:"
               "\n1. The path TPC-H *.tbl data.\n"
               "\n2. The TPC-H query number 1-22."
               "\n3. [optional] The desired profiler locations"
               "\n4. [optional] The desired lane refill buffer locations")
        quit()
    
    profilers = "[]"
    if len(sys.argv) >= 4:
        profilers = sys.argv[3]
    buffers = "[]"
    if len(sys.argv) >= 5:
        buffers = sys.argv[4]

    acc = io.dbAccess ( schema.tpch.tpchSchema, "mmdb", sys.argv[1] )
    execTpch ( acc, sys.argv[2], profilers, buffers )


def execTpch ( acc, num, profilers, buffers ):
    
    qPath = "../query/plans/tpch" + str(num) + ".py"
    cfgPath = "../query/plans/tpch" + str(num) + "cfg.py"

    # read query plan
    alg = RelationalAlgebra ( acc )
    plan = eval ( loadScript ( qPath ) )
    
    # read plan configuration ( if exists )
    cfg = {}
    if os.path.isfile ( cfgPath ):
        cfg = eval ( loadScript ( cfgPath ) )
    plan = alg.resolveAlgebraPlan ( plan, cfg )

    qfilename = "tpch" + str(num)

    if profilers == 'all':
        profilers = getBufferPositions ( alg )
    else:
        profilers = eval ( profilers )
    if buffers == 'all':
        buffers = getBufferPositions ( alg )
    else:
        buffers = eval ( buffers )

    execute ( alg, plan, qfilename, buffers, profilers )
    report ( qfilename, profilers )



from pylatex import Document, Section, Subsection, Command, Figure, Package, MiniPage, SubFigure, LineBreak
from pylatex.utils import italic, NoEscape, escape_latex, verbatim, bold
from pylatex.position import Center
from pylatex.base_classes import Environment

   

class AllTT(Environment):
    """A class to wrap LaTeX's alltt environment."""

    packages = [Package('alltt')]
    escape = False
    content_separator = "\n"

 
def report ( qfilename, profiles ):
    with open(qfilename + ".log", 'r') as file:
        log = file.read()

    from bs4 import BeautifulSoup
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import sys
    import math
    from io import StringIO

    soup = BeautifulSoup(log, 'html.parser')

    if len ( profiles ) > 0:
        profileResults = dict()
        maxIts = 0

        for p in profiles:
            tagName = "p" + str(p)
            profileResults[p] =  pd.read_csv(StringIO(soup.find(tagName).string), names=["lanes", "iters"])
            profileResults[p] = profileResults[p].iloc[1:]
            maxIts = max ( maxIts, max ( profileResults[p].iters ) )
    
        nRows = math.ceil ( len ( profiles ) / 2 )
        nCols = min ( 2, len ( profiles ) ) 

        fig, axs = plt.subplots ( nRows, nCols, figsize=(nCols*4, nRows*2.4) )
        fig.tight_layout(pad=3.0)

        SMALL_SIZE = 8
        MEDIUM_SIZE = 10
        BIGGER_SIZE = 14
        plt.rc('axes', labelsize=BIGGER_SIZE)  
        plt.rc('axes', titlesize=BIGGER_SIZE)  

        fig.add_subplot(111, frameon=False)
        plt.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)
        plt.xlabel("Active warp lanes")
        plt.ylabel("Iterations")    

        for y in range(0,nRows):
            for x in range(0,2):
                idx = y*2+x
                if len ( profiles ) == 1:
                    ax = axs
                elif len ( profiles ) == 2:
                    ax = axs[x]
                else:
                    ax = axs[y,x]
                if idx >= len ( profiles ):
                    if len ( profiles ) > 1:
                        fig.delaxes(ax)
                    break
                p = profiles[idx]
                ax.set_title('Profile P' + str(p))
                ax.set_ylim(ymax=maxIts*1.1, ymin=0)
                ax.set_xlim(xmax=32.7, xmin=0.3)
                ax.set_xticks([1,4,8,12,16,20,24,28,32])
                ax.ticklabel_format(axis="y", style="sci", scilimits=(0,0))
                ax.bar( profileResults[p].lanes, profileResults[p].iters )
        
        plt.savefig('plots.pdf', bbox_inches='tight')

    geometry_options = {"hscale": "0.95","vscale": "0.95"}
    doc = Document(geometry_options=geometry_options)
    doc.packages.append(Package('tikz'))
    doc.append(NoEscape("\\usetikzlibrary{shapes}"))
    doc.append(NoEscape("\\definecolor{colLR}{HTML}{FFA494}\n"))
    doc.append(NoEscape("\\definecolor{colP}{HTML}{98FB98}\n"))
        
    timings = ""                
    for line in soup.timing.string.splitlines():
        tokens = ["krnl_", "scanMulti", "totalKernel"]
        if any(tok in line for tok in tokens):
            timings += ( line + "\n" )
    import textwrap
    timings = textwrap.indent(textwrap.dedent(timings),"  ")

    with doc.create(Figure(position='h!')):
        with doc.create(SubFigure(width=NoEscape(r'0.5\linewidth'))) as planSubfig:
            with planSubfig.create(Center()):
                planSubfig.add_image("qplan.pdf", width=NoEscape("8cm"))
                planSubfig.add_caption('Query execution plan for ' + qfilename )
        with doc.create(SubFigure(width=NoEscape(r'0.5\linewidth'))) as plotsSubfig:
            with plotsSubfig.create(MiniPage(width=r"\linewidth")) as mp:
                with mp.create(Center()) as c:
                    if len ( profiles ) > 0:
                        plotsSubfig.add_image("plots.pdf", width=NoEscape("9cm"))
                for line in timings.splitlines():
                     LRs = []
                     Ps = []

                     if "total" in line:
                         mp.append ( NoEscape ( "\\noindent\\rule{8cm}{0.4pt}\n" ) )

                     start = line.find ( "LR" )
                     while start != -1:
                         space = line.find ( " ", start )
                         colon = line.find ( ":", start )
                         end = min(space,colon)
                         LRs.append ( line[start:end] )
                         line = " "*(end-start+1) + line[:start-1] + line[end:]
                         start = line.find ( "LR" )
                     
                     start = line.find ( "P" )
                     while start != -1:
                         space = line.find ( " ", start )
                         colon = line.find ( ":", start )
                         end = min(space,colon)
                         Ps.append ( line[start:end] )
                         line = " "*(end-start+1) + line[:start-1] + line[end:]
                         start = line.find ( "P" )
                      
                     mp.append ( NoEscape ( "\\mbox{\\texttt{\\footnotesize" + escape_latex ( line ).replace(" ", "~") + "}}" ) )
                     
                     for lr in LRs:
                         mp.append(NoEscape("\,\,\\raisebox{-1ex}{\\tikz{\\node[ellipse,fill=colLR](l){\\tiny " + lr + "};}}"))
                     for p in Ps:
                         mp.append(NoEscape("\,\,\\raisebox{-1ex}{\\tikz{\\node[ellipse,fill=colP](l){\\tiny " + p + "};}}"))
                     mp.append("\n")


            plotsSubfig.add_caption('Profiling results')


    n = 0
    fname = "profiling_report" + str(n)
    while path.exists ( fname + ".pdf" ): 
        fname = "profiling_report" + str(n)
        n = n + 1

    doc.generate_pdf(fname, clean_tex=False, silent=True)


main()
