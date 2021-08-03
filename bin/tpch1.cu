#include <list>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <ctime>
#include <limits.h>
#include <float.h>
#include "../dogqc/include/csv.h"
#include "../dogqc/include/util.h"
#include "../dogqc/include/mappedmalloc.h"
#include "../dogqc/include/util.cuh"
#include "../dogqc/include/hashing.cuh"
struct apayl5 {
    char att17_lreturnf;
    char att18_llinesta;
};

__global__ void krnl_lineitem1(
    int* iatt13_lquantit, float* iatt14_lextende, float* iatt15_ldiscoun, float* iatt16_ltax, char* iatt17_lreturnf, char* iatt18_llinesta, unsigned* iatt19_lshipdat, agg_ht<apayl5>* aht5, float* agg1, float* agg2, float* agg3, float* agg4, float* agg5, float* agg6, float* agg7, int* agg8) {
    int att13_lquantit;
    float att14_lextende;
    float att15_ldiscoun;
    float att16_ltax;
    char att17_lreturnf;
    char att18_llinesta;
    unsigned att19_lshipdat;
    float att25_charge;
    float att26_discpric;

    int tid_lineitem1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_lineitem1 = loopVar;
        active = (loopVar < 6001215);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att13_lquantit = iatt13_lquantit[tid_lineitem1];
            att14_lextende = iatt14_lextende[tid_lineitem1];
            att15_ldiscoun = iatt15_ldiscoun[tid_lineitem1];
            att16_ltax = iatt16_ltax[tid_lineitem1];
            att17_lreturnf = iatt17_lreturnf[tid_lineitem1];
            att18_llinesta = iatt18_llinesta[tid_lineitem1];
            att19_lshipdat = iatt19_lshipdat[tid_lineitem1];
        }
        // -------- selection (opId: 2) --------
        if(active) {
            active = (att19_lshipdat <= 19980902);
        }
        // -------- map (opId: 3) --------
        if(active) {
            att25_charge = ((att14_lextende * ((float)1.0f - att15_ldiscoun)) * ((float)1.0f + att16_ltax));
        }
        // -------- map (opId: 4) --------
        if(active) {
            att26_discpric = (att14_lextende * ((float)1.0f - att15_ldiscoun));
        }
        // -------- aggregation (opId: 5) --------
        int bucket = 0;
        if(active) {
            uint64_t hash5 = 0;
            hash5 = 0;
            if(active) {
                hash5 = hash ( (hash5 + ((uint64_t)att17_lreturnf)));
            }
            if(active) {
                hash5 = hash ( (hash5 + ((uint64_t)att18_llinesta)));
            }
            apayl5 payl;
            payl.att17_lreturnf = att17_lreturnf;
            payl.att18_llinesta = att18_llinesta;
            int bucketFound = 0;
            int numLookups = 0;
            while(!(bucketFound)) {
                bucket = hashAggregateGetBucket ( aht5, 200, hash5, numLookups, &(payl));
                apayl5 probepayl = aht5[bucket].payload;
                bucketFound = 1;
                bucketFound &= ((payl.att17_lreturnf == probepayl.att17_lreturnf));
                bucketFound &= ((payl.att18_llinesta == probepayl.att18_llinesta));
            }
        }
        if(active) {
            atomicAdd(&(agg1[bucket]), ((float)att13_lquantit));
            atomicAdd(&(agg2[bucket]), ((float)att14_lextende));
            atomicAdd(&(agg3[bucket]), ((float)att26_discpric));
            atomicAdd(&(agg4[bucket]), ((float)att25_charge));
            atomicAdd(&(agg5[bucket]), ((float)att13_lquantit));
            atomicAdd(&(agg6[bucket]), ((float)att14_lextende));
            atomicAdd(&(agg7[bucket]), ((float)att15_ldiscoun));
            atomicAdd(&(agg8[bucket]), ((int)1));
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation5(
    agg_ht<apayl5>* aht5, float* agg1, float* agg2, float* agg3, float* agg4, float* agg5, float* agg6, float* agg7, int* agg8, int* nout_result, char* oatt17_lreturnf, char* oatt18_llinesta, float* oatt1_sumqty, float* oatt2_sumbasep, float* oatt3_sumdiscp, float* oatt5_avgqty, float* oatt6_avgprice, float* oatt7_avgdisc, int* oatt8_countord) {
    char att17_lreturnf;
    char att18_llinesta;
    float att1_sumqty;
    float att2_sumbasep;
    float att3_sumdiscp;
    float att4_sumcharg;
    float att5_avgqty;
    float att6_avgprice;
    float att7_avgdisc;
    int att8_countord;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_aggregation5 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation5 = loopVar;
        active = (loopVar < 200);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 5) --------
        if(active) {
            active &= ((aht5[tid_aggregation5].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl5 payl = aht5[tid_aggregation5].payload;
            att17_lreturnf = payl.att17_lreturnf;
            att18_llinesta = payl.att18_llinesta;
        }
        if(active) {
            att1_sumqty = agg1[tid_aggregation5];
            att2_sumbasep = agg2[tid_aggregation5];
            att3_sumdiscp = agg3[tid_aggregation5];
            att4_sumcharg = agg4[tid_aggregation5];
            att5_avgqty = agg5[tid_aggregation5];
            att6_avgprice = agg6[tid_aggregation5];
            att7_avgdisc = agg7[tid_aggregation5];
            att8_countord = agg8[tid_aggregation5];
            att5_avgqty = (att5_avgqty / ((float)att8_countord));
            att6_avgprice = (att6_avgprice / ((float)att8_countord));
            att7_avgdisc = (att7_avgdisc / ((float)att8_countord));
        }
        // -------- projection (no code) (opId: 6) --------
        // -------- materialize (opId: 7) --------
        int wp;
        int writeMask;
        int numProj;
        writeMask = __ballot_sync(ALL_LANES,active);
        numProj = __popc(writeMask);
        if((warplane == 0)) {
            wp = atomicAdd(nout_result, numProj);
        }
        wp = __shfl_sync(ALL_LANES,wp,0);
        wp = (wp + __popc((writeMask & prefixlanes)));
        if(active) {
            oatt17_lreturnf[wp] = att17_lreturnf;
            oatt18_llinesta[wp] = att18_llinesta;
            oatt1_sumqty[wp] = att1_sumqty;
            oatt2_sumbasep[wp] = att2_sumbasep;
            oatt3_sumdiscp[wp] = att3_sumdiscp;
            oatt5_avgqty[wp] = att5_avgqty;
            oatt6_avgprice[wp] = att6_avgprice;
            oatt7_avgdisc[wp] = att7_avgdisc;
            oatt8_countord[wp] = att8_countord;
        }
        loopVar += step;
    }

}

int main() {
    int* iatt13_lquantit;
    iatt13_lquantit = ( int*) map_memory_file ( "mmdb/lineitem_l_quantity" );
    float* iatt14_lextende;
    iatt14_lextende = ( float*) map_memory_file ( "mmdb/lineitem_l_extendedprice" );
    float* iatt15_ldiscoun;
    iatt15_ldiscoun = ( float*) map_memory_file ( "mmdb/lineitem_l_discount" );
    float* iatt16_ltax;
    iatt16_ltax = ( float*) map_memory_file ( "mmdb/lineitem_l_tax" );
    char* iatt17_lreturnf;
    iatt17_lreturnf = ( char*) map_memory_file ( "mmdb/lineitem_l_returnflag" );
    char* iatt18_llinesta;
    iatt18_llinesta = ( char*) map_memory_file ( "mmdb/lineitem_l_linestatus" );
    unsigned* iatt19_lshipdat;
    iatt19_lshipdat = ( unsigned*) map_memory_file ( "mmdb/lineitem_l_shipdate" );

    int nout_result;
    std::vector < char > oatt17_lreturnf(100);
    std::vector < char > oatt18_llinesta(100);
    std::vector < float > oatt1_sumqty(100);
    std::vector < float > oatt2_sumbasep(100);
    std::vector < float > oatt3_sumdiscp(100);
    std::vector < float > oatt5_avgqty(100);
    std::vector < float > oatt6_avgprice(100);
    std::vector < float > oatt7_avgdisc(100);
    std::vector < int > oatt8_countord(100);

    // wake up gpu
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in wake up gpu! " << cudaGetErrorString( err ) << std::endl;
            ERROR("wake up gpu")
        }
    }

    int* d_iatt13_lquantit;
    cudaMalloc((void**) &d_iatt13_lquantit, 6001215* sizeof(int) );
    float* d_iatt14_lextende;
    cudaMalloc((void**) &d_iatt14_lextende, 6001215* sizeof(float) );
    float* d_iatt15_ldiscoun;
    cudaMalloc((void**) &d_iatt15_ldiscoun, 6001215* sizeof(float) );
    float* d_iatt16_ltax;
    cudaMalloc((void**) &d_iatt16_ltax, 6001215* sizeof(float) );
    char* d_iatt17_lreturnf;
    cudaMalloc((void**) &d_iatt17_lreturnf, 6001215* sizeof(char) );
    char* d_iatt18_llinesta;
    cudaMalloc((void**) &d_iatt18_llinesta, 6001215* sizeof(char) );
    unsigned* d_iatt19_lshipdat;
    cudaMalloc((void**) &d_iatt19_lshipdat, 6001215* sizeof(unsigned) );
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    char* d_oatt17_lreturnf;
    cudaMalloc((void**) &d_oatt17_lreturnf, 100* sizeof(char) );
    char* d_oatt18_llinesta;
    cudaMalloc((void**) &d_oatt18_llinesta, 100* sizeof(char) );
    float* d_oatt1_sumqty;
    cudaMalloc((void**) &d_oatt1_sumqty, 100* sizeof(float) );
    float* d_oatt2_sumbasep;
    cudaMalloc((void**) &d_oatt2_sumbasep, 100* sizeof(float) );
    float* d_oatt3_sumdiscp;
    cudaMalloc((void**) &d_oatt3_sumdiscp, 100* sizeof(float) );
    float* d_oatt5_avgqty;
    cudaMalloc((void**) &d_oatt5_avgqty, 100* sizeof(float) );
    float* d_oatt6_avgprice;
    cudaMalloc((void**) &d_oatt6_avgprice, 100* sizeof(float) );
    float* d_oatt7_avgdisc;
    cudaMalloc((void**) &d_oatt7_avgdisc, 100* sizeof(float) );
    int* d_oatt8_countord;
    cudaMalloc((void**) &d_oatt8_countord, 100* sizeof(int) );
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda malloc! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda malloc")
        }
    }


    // show memory usage of GPU
    {   size_t free_byte ;
        size_t total_byte ;
        cudaError_t cuda_status = cudaMemGetInfo( &free_byte, &total_byte ) ;
        if ( cudaSuccess != cuda_status ) {
            printf("Error: cudaMemGetInfo fails, %s \n", cudaGetErrorString(cuda_status) );
            exit(1);
        }
        double free_db = (double)free_byte ;
        double total_db = (double)total_byte ;
        double used_db = total_db - free_db ;
        fprintf(stderr, "Memory %.1f / %.1f GB\n",
                used_db/(1024*1024*1024), total_db/(1024*1024*1024) );
        fflush(stdout);
    }

    agg_ht<apayl5>* d_aht5;
    cudaMalloc((void**) &d_aht5, 200* sizeof(agg_ht<apayl5>) );
    {
        int gridsize=920;
        int blocksize=128;
        initAggHT<<<gridsize, blocksize>>>(d_aht5, 200);
    }
    float* d_agg1;
    cudaMalloc((void**) &d_agg1, 200* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg1, 0.0f, 200);
    }
    float* d_agg2;
    cudaMalloc((void**) &d_agg2, 200* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg2, 0.0f, 200);
    }
    float* d_agg3;
    cudaMalloc((void**) &d_agg3, 200* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg3, 0.0f, 200);
    }
    float* d_agg4;
    cudaMalloc((void**) &d_agg4, 200* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg4, 0.0f, 200);
    }
    float* d_agg5;
    cudaMalloc((void**) &d_agg5, 200* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg5, 0.0f, 200);
    }
    float* d_agg6;
    cudaMalloc((void**) &d_agg6, 200* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg6, 0.0f, 200);
    }
    float* d_agg7;
    cudaMalloc((void**) &d_agg7, 200* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg7, 0.0f, 200);
    }
    int* d_agg8;
    cudaMalloc((void**) &d_agg8, 200* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg8, 0, 200);
    }
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_nout_result, 0, 1);
    }
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda mallocHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda mallocHT")
        }
    }


    // show memory usage of GPU
    {   size_t free_byte ;
        size_t total_byte ;
        cudaError_t cuda_status = cudaMemGetInfo( &free_byte, &total_byte ) ;
        if ( cudaSuccess != cuda_status ) {
            printf("Error: cudaMemGetInfo fails, %s \n", cudaGetErrorString(cuda_status) );
            exit(1);
        }
        double free_db = (double)free_byte ;
        double total_db = (double)total_byte ;
        double used_db = total_db - free_db ;
        fprintf(stderr, "Memory %.1f / %.1f GB\n",
                used_db/(1024*1024*1024), total_db/(1024*1024*1024) );
        fflush(stdout);
    }

    cudaMemcpy( d_iatt13_lquantit, iatt13_lquantit, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt14_lextende, iatt14_lextende, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt15_ldiscoun, iatt15_ldiscoun, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt16_ltax, iatt16_ltax, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt17_lreturnf, iatt17_lreturnf, 6001215 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt18_llinesta, iatt18_llinesta, 6001215 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt19_lshipdat, iatt19_lshipdat, 6001215 * sizeof(unsigned), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime0 = std::clock();
    std::clock_t start_krnl_lineitem11 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_lineitem1<<<gridsize, blocksize>>>(d_iatt13_lquantit, d_iatt14_lextende, d_iatt15_ldiscoun, d_iatt16_ltax, d_iatt17_lreturnf, d_iatt18_llinesta, d_iatt19_lshipdat, d_aht5, d_agg1, d_agg2, d_agg3, d_agg4, d_agg5, d_agg6, d_agg7, d_agg8);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem11 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem1! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem1")
        }
    }

    std::clock_t start_krnl_aggregation52 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation5<<<gridsize, blocksize>>>(d_aht5, d_agg1, d_agg2, d_agg3, d_agg4, d_agg5, d_agg6, d_agg7, d_agg8, d_nout_result, d_oatt17_lreturnf, d_oatt18_llinesta, d_oatt1_sumqty, d_oatt2_sumbasep, d_oatt3_sumdiscp, d_oatt5_avgqty, d_oatt6_avgprice, d_oatt7_avgdisc, d_oatt8_countord);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation52 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation5! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation5")
        }
    }

    std::clock_t stop_totalKernelTime0 = std::clock();
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt17_lreturnf.data(), d_oatt17_lreturnf, 100 * sizeof(char), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt18_llinesta.data(), d_oatt18_llinesta, 100 * sizeof(char), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt1_sumqty.data(), d_oatt1_sumqty, 100 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt2_sumbasep.data(), d_oatt2_sumbasep, 100 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt3_sumdiscp.data(), d_oatt3_sumdiscp, 100 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt5_avgqty.data(), d_oatt5_avgqty, 100 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt6_avgprice.data(), d_oatt6_avgprice, 100 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt7_avgdisc.data(), d_oatt7_avgdisc, 100 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt8_countord.data(), d_oatt8_countord, 100 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    cudaFree( d_iatt13_lquantit);
    cudaFree( d_iatt14_lextende);
    cudaFree( d_iatt15_ldiscoun);
    cudaFree( d_iatt16_ltax);
    cudaFree( d_iatt17_lreturnf);
    cudaFree( d_iatt18_llinesta);
    cudaFree( d_iatt19_lshipdat);
    cudaFree( d_aht5);
    cudaFree( d_agg1);
    cudaFree( d_agg2);
    cudaFree( d_agg3);
    cudaFree( d_agg4);
    cudaFree( d_agg5);
    cudaFree( d_agg6);
    cudaFree( d_agg7);
    cudaFree( d_agg8);
    cudaFree( d_nout_result);
    cudaFree( d_oatt17_lreturnf);
    cudaFree( d_oatt18_llinesta);
    cudaFree( d_oatt1_sumqty);
    cudaFree( d_oatt2_sumbasep);
    cudaFree( d_oatt3_sumdiscp);
    cudaFree( d_oatt5_avgqty);
    cudaFree( d_oatt6_avgprice);
    cudaFree( d_oatt7_avgdisc);
    cudaFree( d_oatt8_countord);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda free! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda free")
        }
    }

    std::clock_t start_finish3 = std::clock();
    printf("\nResult: %i tuples\n", nout_result);
    if((nout_result > 100)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("l_returnflag: ");
        printf("%c", oatt17_lreturnf[pv]);
        printf("  ");
        printf("l_linestatus: ");
        printf("%c", oatt18_llinesta[pv]);
        printf("  ");
        printf("sum_qty: ");
        printf("%15.2f", oatt1_sumqty[pv]);
        printf("  ");
        printf("sum_base_price: ");
        printf("%15.2f", oatt2_sumbasep[pv]);
        printf("  ");
        printf("sum_disc_price: ");
        printf("%15.2f", oatt3_sumdiscp[pv]);
        printf("  ");
        printf("avg_qty: ");
        printf("%15.2f", oatt5_avgqty[pv]);
        printf("  ");
        printf("avg_price: ");
        printf("%15.2f", oatt6_avgprice[pv]);
        printf("  ");
        printf("avg_disc: ");
        printf("%15.2f", oatt7_avgdisc[pv]);
        printf("  ");
        printf("count_order: ");
        printf("%8i", oatt8_countord[pv]);
        printf("  ");
        printf("\n");
    }
    if((nout_result > 10)) {
        printf("[...]\n");
    }
    printf("\n");
    std::clock_t stop_finish3 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem1", (stop_krnl_lineitem11 - start_krnl_lineitem11) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation5", (stop_krnl_aggregation52 - start_krnl_aggregation52) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish3 - start_finish3) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime0 - start_totalKernelTime0) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
