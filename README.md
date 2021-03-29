# DogQC Query Compiler

DogQC is a query compiler prototype that translates relational algebra to native code for execution on data-parallel coprocessors.
As a research project DogQC's aim is to investigate non-uniform use cases, where data-parallelism can not easily be applied in a 1:1 mapping of data elements to processing lanes [1].
The features of DogQC include the following:

**Data Import**

DogQC parses `.tbl` files and loads the data columns into memory mapped files. 

**Query Execution**

Pipelines of non-blocking operators are translated to GPU kernels with a similar approach as [2].

**Divergence**

DogQC employs mechanisms to address the effects of data-parallel lanes following varying execution paths (*divergence*). These include two techniques: *Lane refill*, adapted from a CPU-SIMD approach [3], reassigns work to lanes that have become passive in a pipeline.  *Push-down parallelism* changes the parallelization to a finer granularity for operators with expansion processes.

### Development Environment

- Ubuntu 18.04 (memory mapped I/O)
- Cuda 10.1
- NVidia RTX 2080
- G++ 7.4.0
- Python 3.6.9 with package graphviz to visualize query plans
- astyle 3.1

### Project Structure
`./bin/`   is the working directory for query execution.

`./query/` contains scripts to execute queries.

`./query/plans` contains relational query plans.

`./dogqc/` contains the query compiler.

`./sample/` samples for generated `.cu` files and profiling reports.

### Getting Started
First ensure that your environment provides the soft- and hardware described in development environment. 
Similar may work, but has not been tested.
Then use the `dbgen` tool from the TPC-H Benchmark [3] to generate a test database. 
Finally go to the `./bin/` folder and use the following command to execute, e.g., tpch query 1:
```
python3 ../query/tpch.py /path/to/your/tpch/.tbl/files/ 1
```
We suggest starting with smaller scale factors and for larger scale factors it may be necessary to adjust `./query/plans/tpch*cfg.py` by providing hash table size estimates etc.
DogQC will proceed as follow:

1. The `.tbl` files are imported to memory mapped files in `./bin/mmdb/`

2. The GPU code for tpch query 1 is generated to `./bin/tpch1.cu`

3. The `.cu` file is compiled with `nvcc` to `./bin/tpch1` and executed. 

If consecutive queries are executed on the *same* database, DogQC will skip step 1. 
Various other queries and divergence workloads can be found in `./query/`.
To clear the database delete the folder `./bin/mmdb/`.
To get additional information during query execution, the `CudaCompiler` object can be created with `debug=True`.
Query plans can be visualized with the `RelationalAlgebra.showGraph(..)` method when `graphviz` for python is installed.
The `clang` compiler works aswell and achieves shorter compilation times.

### Profiler
DogQC provides a profiler for divergence balancing [5]. The profiling tool allows users to freely place balancing operators into DogQC-generated query plans and to observe their effects. Sample outputs of the profiling tool are shown in `sample/profiling_report_divergent.pdf` and `sample/profiling_report_balanced.pdf`.

To execute queries with the profiler you can go to the `bin` directory and try the following commands.
```
% Execute TPC-H Q10 and create profiling report
%  - result: profiling_report0.pdf
python3 ../query/profiler.py ~/tpch/sf1 10

% profile lane activity after operator 9
python3 ../query/profiler.py ~/tpch/sf1 10 [9] []

% profile with divergence balancing and lane activity profile
% after operator 9 (balancing comes first)
python3 ../query/profiler.py ~/tpch/sf1 10 [9] [9]
```

### Query Building
Currently DogQC does not yet provide an SQL interface.
Query execution is performed based on relational query plans provided by the user.
Samples are given in `./query/plans`.
To build your own queries you can write a query plan as tree of operators.
To specify attributes the attribute name (e.g. `l_extendedprice`)  can be used or the attribute name with preceding table name (e.g. `lineitem.extendedprice`).

### Testing
For testing, we execute the series of tpch queries with:
```
python3 ../query/tpch.py /path/to/your/tpch/.tbl/files/ all
```

### References
 [1] Henning Funke, Jens Teubner:
Data-Parallel Query Processing on Non-Uniform Data. VLDB Conference 2020.

 [2] Henning Funke, Sebastian Breß, Stefan Noll, Volker Markl, Jens Teubner:
Pipelined Query Processing in Coprocessor Environments. SIGMOD Conference 2018.

 [3] Harald Lang, Andreas Kipf, Linnea Passing, Peter A. Boncz, Thomas Neumann, Alfons Kemper:
Make the most out of your SIMD investments: counter control flow divergence in compiled query pipelines. DaMoN 2018.

 [4] https://github.com/electrum/tpch-dbgen
 
 [5] Henning Funke, Jens Teubner:
 Like water and oil: with a proper emulsifier, query compilation and data parallelism will mix well. VLDB Conference 2020.
