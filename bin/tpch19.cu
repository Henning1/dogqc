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
struct jpayl3 {
    int att2_ppartkey;
    str_t att5_pbrand;
    int att7_psize;
    str_t att8_pcontain;
};

__global__ void krnl_part1(
    int* iatt2_ppartkey, size_t* iatt5_pbrand_offset, char* iatt5_pbrand_char, int* iatt7_psize, size_t* iatt8_pcontain_offset, char* iatt8_pcontain_char, unique_ht<jpayl3>* jht3) {
    int att2_ppartkey;
    str_t att5_pbrand;
    int att7_psize;
    str_t att8_pcontain;

    int tid_part1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_part1 = loopVar;
        active = (loopVar < 200000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att2_ppartkey = iatt2_ppartkey[tid_part1];
            att5_pbrand = stringScan ( iatt5_pbrand_offset, iatt5_pbrand_char, tid_part1);
            att7_psize = iatt7_psize[tid_part1];
            att8_pcontain = stringScan ( iatt8_pcontain_offset, iatt8_pcontain_char, tid_part1);
        }
        // -------- hash join build (opId: 3) --------
        if(active) {
            jpayl3 payl3;
            payl3.att2_ppartkey = att2_ppartkey;
            payl3.att5_pbrand = att5_pbrand;
            payl3.att7_psize = att7_psize;
            payl3.att8_pcontain = att8_pcontain;
            uint64_t hash3;
            hash3 = 0;
            if(active) {
                hash3 = hash ( (hash3 + ((uint64_t)att2_ppartkey)));
            }
            hashBuildUnique ( jht3, 400000, hash3, &(payl3));
        }
        loopVar += step;
    }

}

__global__ void krnl_lineitem2(
    int* iatt12_lpartkey, int* iatt15_lquantit, float* iatt16_lextende, float* iatt17_ldiscoun, size_t* iatt24_lshipins_offset, char* iatt24_lshipins_char, size_t* iatt25_lshipmod_offset, char* iatt25_lshipmod_char, unique_ht<jpayl3>* jht3, float* agg1) {
    int att12_lpartkey;
    int att15_lquantit;
    float att16_lextende;
    float att17_ldiscoun;
    str_t att24_lshipins;
    str_t att25_lshipmod;
    int att2_ppartkey;
    str_t att5_pbrand;
    int att7_psize;
    str_t att8_pcontain;
    str_t c1 = stringConstant ( "Brand#12", 8);
    str_t c2 = stringConstant ( "SM PKG", 6);
    str_t c3 = stringConstant ( "SM PACK", 7);
    str_t c4 = stringConstant ( "SM BOX", 6);
    str_t c5 = stringConstant ( "SM CASE", 7);
    str_t c6 = stringConstant ( "AIR REG", 7);
    str_t c7 = stringConstant ( "AIR", 3);
    str_t c8 = stringConstant ( "DELIVER IN PERSON", 17);
    str_t c9 = stringConstant ( "Brand#23", 8);
    str_t c10 = stringConstant ( "MED PACK", 8);
    str_t c11 = stringConstant ( "MED PKG", 7);
    str_t c12 = stringConstant ( "MED BOX", 7);
    str_t c13 = stringConstant ( "MED BAG", 7);
    str_t c14 = stringConstant ( "AIR REG", 7);
    str_t c15 = stringConstant ( "AIR", 3);
    str_t c16 = stringConstant ( "DELIVER IN PERSON", 17);
    str_t c17 = stringConstant ( "Brand#34", 8);
    str_t c18 = stringConstant ( "LG PKG", 6);
    str_t c19 = stringConstant ( "LG PACK", 7);
    str_t c20 = stringConstant ( "LG BOX", 6);
    str_t c21 = stringConstant ( "LG CASE", 7);
    str_t c22 = stringConstant ( "AIR REG", 7);
    str_t c23 = stringConstant ( "AIR", 3);
    str_t c24 = stringConstant ( "DELIVER IN PERSON", 17);
    float att27_rev;

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
            att12_lpartkey = iatt12_lpartkey[tid_lineitem1];
            att15_lquantit = iatt15_lquantit[tid_lineitem1];
            att16_lextende = iatt16_lextende[tid_lineitem1];
            att17_ldiscoun = iatt17_ldiscoun[tid_lineitem1];
            att24_lshipins = stringScan ( iatt24_lshipins_offset, iatt24_lshipins_char, tid_lineitem1);
            att25_lshipmod = stringScan ( iatt25_lshipmod_offset, iatt25_lshipmod_char, tid_lineitem1);
        }
        // -------- hash join probe (opId: 3) --------
        uint64_t hash3 = 0;
        if(active) {
            hash3 = 0;
            if(active) {
                hash3 = hash ( (hash3 + ((uint64_t)att12_lpartkey)));
            }
        }
        jpayl3* probepayl3;
        int numLookups3 = 0;
        if(active) {
            active = hashProbeUnique ( jht3, 400000, hash3, numLookups3, &(probepayl3));
        }
        int bucketFound3 = 0;
        int probeActive3 = active;
        while((probeActive3 && !(bucketFound3))) {
            jpayl3 jprobepayl3 = *(probepayl3);
            att2_ppartkey = jprobepayl3.att2_ppartkey;
            att5_pbrand = jprobepayl3.att5_pbrand;
            att7_psize = jprobepayl3.att7_psize;
            att8_pcontain = jprobepayl3.att8_pcontain;
            bucketFound3 = 1;
            bucketFound3 &= ((att2_ppartkey == att12_lpartkey));
            if(!(bucketFound3)) {
                probeActive3 = hashProbeUnique ( jht3, 400000, hash3, numLookups3, &(probepayl3));
            }
        }
        active = bucketFound3;
        // -------- selection (opId: 4) --------
        if(active) {
            active = (((((((((stringEquals ( att5_pbrand, c1) && (stringEquals ( att8_pcontain, c2) || (stringEquals ( att8_pcontain, c3) || (stringEquals ( att8_pcontain, c4) || stringEquals ( att8_pcontain, c5))))) && (att15_lquantit >= 1.0f)) && (att15_lquantit <= 11.0f)) && (att7_psize >= 1)) && (att7_psize <= 5)) && (stringEquals ( att25_lshipmod, c6) || stringEquals ( att25_lshipmod, c7))) && stringEquals ( att24_lshipins, c8)) || (((((((stringEquals ( att5_pbrand, c9) && (stringEquals ( att8_pcontain, c10) || (stringEquals ( att8_pcontain, c11) || (stringEquals ( att8_pcontain, c12) || stringEquals ( att8_pcontain, c13))))) && (att15_lquantit >= 10.0f)) && (att15_lquantit <= 20.0f)) && (att7_psize >= 1)) && (att7_psize <= 10)) && (stringEquals ( att25_lshipmod, c14) || stringEquals ( att25_lshipmod, c15))) && stringEquals ( att24_lshipins, c16))) || (((((((stringEquals ( att5_pbrand, c17) && (stringEquals ( att8_pcontain, c18) || (stringEquals ( att8_pcontain, c19) || (stringEquals ( att8_pcontain, c20) || stringEquals ( att8_pcontain, c21))))) && (att15_lquantit >= 20.0f)) && (att15_lquantit <= 30.0f)) && (att7_psize >= 1)) && (att7_psize <= 15)) && (stringEquals ( att25_lshipmod, c22) || stringEquals ( att25_lshipmod, c23))) && stringEquals ( att24_lshipins, c24)));
        }
        // -------- map (opId: 5) --------
        if(active) {
            att27_rev = (att16_lextende * (1.0 - att17_ldiscoun));
        }
        // -------- aggregation (opId: 6) --------
        int bucket = 0;
        if(active) {
            atomicAdd(&(agg1[bucket]), ((float)att27_rev));
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation6(
    float* agg1, int* nout_result, float* oatt1_revenue) {
    float att1_revenue;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_aggregation6 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation6 = loopVar;
        active = (loopVar < 1);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 6) --------
        if(active) {
            att1_revenue = agg1[tid_aggregation6];
        }
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
            oatt1_revenue[wp] = att1_revenue;
        }
        loopVar += step;
    }

}

int main() {
    int* iatt2_ppartkey;
    iatt2_ppartkey = ( int*) map_memory_file ( "mmdb/part_p_partkey" );
    size_t* iatt5_pbrand_offset;
    iatt5_pbrand_offset = ( size_t*) map_memory_file ( "mmdb/part_p_brand_offset" );
    char* iatt5_pbrand_char;
    iatt5_pbrand_char = ( char*) map_memory_file ( "mmdb/part_p_brand_char" );
    int* iatt7_psize;
    iatt7_psize = ( int*) map_memory_file ( "mmdb/part_p_size" );
    size_t* iatt8_pcontain_offset;
    iatt8_pcontain_offset = ( size_t*) map_memory_file ( "mmdb/part_p_container_offset" );
    char* iatt8_pcontain_char;
    iatt8_pcontain_char = ( char*) map_memory_file ( "mmdb/part_p_container_char" );
    int* iatt12_lpartkey;
    iatt12_lpartkey = ( int*) map_memory_file ( "mmdb/lineitem_l_partkey" );
    int* iatt15_lquantit;
    iatt15_lquantit = ( int*) map_memory_file ( "mmdb/lineitem_l_quantity" );
    float* iatt16_lextende;
    iatt16_lextende = ( float*) map_memory_file ( "mmdb/lineitem_l_extendedprice" );
    float* iatt17_ldiscoun;
    iatt17_ldiscoun = ( float*) map_memory_file ( "mmdb/lineitem_l_discount" );
    size_t* iatt24_lshipins_offset;
    iatt24_lshipins_offset = ( size_t*) map_memory_file ( "mmdb/lineitem_l_shipinstruct_offset" );
    char* iatt24_lshipins_char;
    iatt24_lshipins_char = ( char*) map_memory_file ( "mmdb/lineitem_l_shipinstruct_char" );
    size_t* iatt25_lshipmod_offset;
    iatt25_lshipmod_offset = ( size_t*) map_memory_file ( "mmdb/lineitem_l_shipmode_offset" );
    char* iatt25_lshipmod_char;
    iatt25_lshipmod_char = ( char*) map_memory_file ( "mmdb/lineitem_l_shipmode_char" );

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

    int* d_iatt2_ppartkey;
    cudaMalloc((void**) &d_iatt2_ppartkey, 200000* sizeof(int) );
    size_t* d_iatt5_pbrand_offset;
    cudaMalloc((void**) &d_iatt5_pbrand_offset, (200000 + 1)* sizeof(size_t) );
    char* d_iatt5_pbrand_char;
    cudaMalloc((void**) &d_iatt5_pbrand_char, 1600009* sizeof(char) );
    int* d_iatt7_psize;
    cudaMalloc((void**) &d_iatt7_psize, 200000* sizeof(int) );
    size_t* d_iatt8_pcontain_offset;
    cudaMalloc((void**) &d_iatt8_pcontain_offset, (200000 + 1)* sizeof(size_t) );
    char* d_iatt8_pcontain_char;
    cudaMalloc((void**) &d_iatt8_pcontain_char, 1514980* sizeof(char) );
    int* d_iatt12_lpartkey;
    cudaMalloc((void**) &d_iatt12_lpartkey, 6001215* sizeof(int) );
    int* d_iatt15_lquantit;
    cudaMalloc((void**) &d_iatt15_lquantit, 6001215* sizeof(int) );
    float* d_iatt16_lextende;
    cudaMalloc((void**) &d_iatt16_lextende, 6001215* sizeof(float) );
    float* d_iatt17_ldiscoun;
    cudaMalloc((void**) &d_iatt17_ldiscoun, 6001215* sizeof(float) );
    size_t* d_iatt24_lshipins_offset;
    cudaMalloc((void**) &d_iatt24_lshipins_offset, (6001215 + 1)* sizeof(size_t) );
    char* d_iatt24_lshipins_char;
    cudaMalloc((void**) &d_iatt24_lshipins_char, 72006418* sizeof(char) );
    size_t* d_iatt25_lshipmod_offset;
    cudaMalloc((void**) &d_iatt25_lshipmod_offset, (6001215 + 1)* sizeof(size_t) );
    char* d_iatt25_lshipmod_char;
    cudaMalloc((void**) &d_iatt25_lshipmod_char, 25717043* sizeof(char) );
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

    unique_ht<jpayl3>* d_jht3;
    cudaMalloc((void**) &d_jht3, 400000* sizeof(unique_ht<jpayl3>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht3, 400000);
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

    cudaMemcpy( d_iatt2_ppartkey, iatt2_ppartkey, 200000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt5_pbrand_offset, iatt5_pbrand_offset, (200000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt5_pbrand_char, iatt5_pbrand_char, 1600009 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt7_psize, iatt7_psize, 200000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt8_pcontain_offset, iatt8_pcontain_offset, (200000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt8_pcontain_char, iatt8_pcontain_char, 1514980 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt12_lpartkey, iatt12_lpartkey, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt15_lquantit, iatt15_lquantit, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt16_lextende, iatt16_lextende, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt17_ldiscoun, iatt17_ldiscoun, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt24_lshipins_offset, iatt24_lshipins_offset, (6001215 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt24_lshipins_char, iatt24_lshipins_char, 72006418 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt25_lshipmod_offset, iatt25_lshipmod_offset, (6001215 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt25_lshipmod_char, iatt25_lshipmod_char, 25717043 * sizeof(char), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime161 = std::clock();
    std::clock_t start_krnl_part1162 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_part1<<<gridsize, blocksize>>>(d_iatt2_ppartkey, d_iatt5_pbrand_offset, d_iatt5_pbrand_char, d_iatt7_psize, d_iatt8_pcontain_offset, d_iatt8_pcontain_char, d_jht3);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_part1162 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_part1! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_part1")
        }
    }

    std::clock_t start_krnl_lineitem2163 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_lineitem2<<<gridsize, blocksize>>>(d_iatt12_lpartkey, d_iatt15_lquantit, d_iatt16_lextende, d_iatt17_ldiscoun, d_iatt24_lshipins_offset, d_iatt24_lshipins_char, d_iatt25_lshipmod_offset, d_iatt25_lshipmod_char, d_jht3, d_agg1);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem2163 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem2! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem2")
        }
    }

    std::clock_t start_krnl_aggregation6164 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation6<<<gridsize, blocksize>>>(d_agg1, d_nout_result, d_oatt1_revenue);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation6164 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation6! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation6")
        }
    }

    std::clock_t stop_totalKernelTime161 = std::clock();
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

    cudaFree( d_iatt2_ppartkey);
    cudaFree( d_iatt5_pbrand_offset);
    cudaFree( d_iatt5_pbrand_char);
    cudaFree( d_iatt7_psize);
    cudaFree( d_iatt8_pcontain_offset);
    cudaFree( d_iatt8_pcontain_char);
    cudaFree( d_jht3);
    cudaFree( d_iatt12_lpartkey);
    cudaFree( d_iatt15_lquantit);
    cudaFree( d_iatt16_lextende);
    cudaFree( d_iatt17_ldiscoun);
    cudaFree( d_iatt24_lshipins_offset);
    cudaFree( d_iatt24_lshipins_char);
    cudaFree( d_iatt25_lshipmod_offset);
    cudaFree( d_iatt25_lshipmod_char);
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

    std::clock_t start_finish165 = std::clock();
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
    std::clock_t stop_finish165 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_part1", (stop_krnl_part1162 - start_krnl_part1162) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem2", (stop_krnl_lineitem2163 - start_krnl_lineitem2163) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation6", (stop_krnl_aggregation6164 - start_krnl_aggregation6164) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish165 - start_finish165) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime161 - start_totalKernelTime161) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
