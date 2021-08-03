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
__global__ void krnl_lineitem1(
    int* iatt6_lquantit, float* iatt7_lextende, float* iatt8_ldiscoun, unsigned* iatt12_lshipdat, float* agg1) {
    int att6_lquantit;
    float att7_lextende;
    float att8_ldiscoun;
    unsigned att12_lshipdat;
    float att18_rev;

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
            att6_lquantit = iatt6_lquantit[tid_lineitem1];
            att7_lextende = iatt7_lextende[tid_lineitem1];
            att8_ldiscoun = iatt8_ldiscoun[tid_lineitem1];
            att12_lshipdat = iatt12_lshipdat[tid_lineitem1];
        }
        // -------- selection (opId: 2) --------
        if(active) {
            active = ((att12_lshipdat >= 19940101) && ((att12_lshipdat < 19950101) && ((att8_ldiscoun >= 0.05) && ((att8_ldiscoun <= 0.07) && (att6_lquantit < 24)))));
        }
        // -------- map (opId: 3) --------
        if(active) {
            att18_rev = (att7_lextende * att8_ldiscoun);
        }
        // -------- aggregation (opId: 4) --------
        int bucket = 0;
        if(active) {
            atomicAdd(&(agg1[bucket]), ((float)att18_rev));
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation4(
    float* agg1, int* nout_result, float* oatt1_revenue) {
    float att1_revenue;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_aggregation4 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation4 = loopVar;
        active = (loopVar < 1);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 4) --------
        if(active) {
            att1_revenue = agg1[tid_aggregation4];
        }
        // -------- materialize (opId: 5) --------
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
            oatt1_revenue[wp] = att1_revenue;
        }
        loopVar += step;
    }

}

int main() {
    int* iatt6_lquantit;
    iatt6_lquantit = ( int*) map_memory_file ( "mmdb/lineitem_l_quantity" );
    float* iatt7_lextende;
    iatt7_lextende = ( float*) map_memory_file ( "mmdb/lineitem_l_extendedprice" );
    float* iatt8_ldiscoun;
    iatt8_ldiscoun = ( float*) map_memory_file ( "mmdb/lineitem_l_discount" );
    unsigned* iatt12_lshipdat;
    iatt12_lshipdat = ( unsigned*) map_memory_file ( "mmdb/lineitem_l_shipdate" );

    int nout_result;
    std::vector < float > oatt1_revenue(1);

    // wake up gpu
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in wake up gpu! " << cudaGetErrorString( err ) << std::endl;
            ERROR("wake up gpu")
        }
    }

    int* d_iatt6_lquantit;
    cudaMalloc((void**) &d_iatt6_lquantit, 6001215* sizeof(int) );
    float* d_iatt7_lextende;
    cudaMalloc((void**) &d_iatt7_lextende, 6001215* sizeof(float) );
    float* d_iatt8_ldiscoun;
    cudaMalloc((void**) &d_iatt8_ldiscoun, 6001215* sizeof(float) );
    unsigned* d_iatt12_lshipdat;
    cudaMalloc((void**) &d_iatt12_lshipdat, 6001215* sizeof(unsigned) );
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    float* d_oatt1_revenue;
    cudaMalloc((void**) &d_oatt1_revenue, 1* sizeof(float) );
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

    float* d_agg1;
    cudaMalloc((void**) &d_agg1, 1* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg1, 0.0f, 1);
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

    cudaMemcpy( d_iatt6_lquantit, iatt6_lquantit, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt7_lextende, iatt7_lextende, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt8_ldiscoun, iatt8_ldiscoun, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt12_lshipdat, iatt12_lshipdat, 6001215 * sizeof(unsigned), cudaMemcpyHostToDevice);
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
        krnl_lineitem1<<<gridsize, blocksize>>>(d_iatt6_lquantit, d_iatt7_lextende, d_iatt8_ldiscoun, d_iatt12_lshipdat, d_agg1);
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

    std::clock_t start_krnl_aggregation42 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation4<<<gridsize, blocksize>>>(d_agg1, d_nout_result, d_oatt1_revenue);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation42 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation4! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation4")
        }
    }

    std::clock_t stop_totalKernelTime0 = std::clock();
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt1_revenue.data(), d_oatt1_revenue, 1 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    cudaFree( d_iatt6_lquantit);
    cudaFree( d_iatt7_lextende);
    cudaFree( d_iatt8_ldiscoun);
    cudaFree( d_iatt12_lshipdat);
    cudaFree( d_agg1);
    cudaFree( d_nout_result);
    cudaFree( d_oatt1_revenue);
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
    if((nout_result > 1)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("revenue: ");
        printf("%15.2f", oatt1_revenue[pv]);
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
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation4", (stop_krnl_aggregation42 - start_krnl_aggregation42) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish3 - start_finish3) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime0 - start_totalKernelTime0) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
