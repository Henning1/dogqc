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
    int att3_lpartkey;
    float att7_lextende;
    float att8_ldiscoun;
};

__global__ void krnl_lineitem1(
    int* iatt3_lpartkey, float* iatt7_lextende, float* iatt8_ldiscoun, multi_ht* jht3, jpayl3* jht3_payload) {
    int att3_lpartkey;
    float att7_lextende;
    float att8_ldiscoun;

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
            att3_lpartkey = iatt3_lpartkey[tid_lineitem1];
            att7_lextende = iatt7_lextende[tid_lineitem1];
            att8_ldiscoun = iatt8_ldiscoun[tid_lineitem1];
        }
        // -------- hash join build (opId: 3) --------
        if(active) {
            uint64_t hash3 = 0;
            if(active) {
                hash3 = 0;
                if(active) {
                    hash3 = hash ( (hash3 + ((uint64_t)att3_lpartkey)));
                }
            }
            hashCountMulti ( jht3, 12002430, hash3);
        }
        loopVar += step;
    }

}

__global__ void krnl_lineitem1_ins(
    int* iatt3_lpartkey, float* iatt7_lextende, float* iatt8_ldiscoun, multi_ht* jht3, jpayl3* jht3_payload, int* offs3) {
    int att3_lpartkey;
    float att7_lextende;
    float att8_ldiscoun;

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
            att3_lpartkey = iatt3_lpartkey[tid_lineitem1];
            att7_lextende = iatt7_lextende[tid_lineitem1];
            att8_ldiscoun = iatt8_ldiscoun[tid_lineitem1];
        }
        // -------- hash join build (opId: 3) --------
        if(active) {
            uint64_t hash3 = 0;
            if(active) {
                hash3 = 0;
                if(active) {
                    hash3 = hash ( (hash3 + ((uint64_t)att3_lpartkey)));
                }
            }
            jpayl3 payl;
            payl.att3_lpartkey = att3_lpartkey;
            payl.att7_lextende = att7_lextende;
            payl.att8_ldiscoun = att8_ldiscoun;
            hashInsertMulti ( jht3, jht3_payload, offs3, 12002430, hash3, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_part2(
    int* iatt18_ppartkey, multi_ht* jht3, jpayl3* jht3_payload, float* agg1) {
    int att18_ppartkey;
    unsigned warplane = (threadIdx.x % 32);
    int att3_lpartkey;
    float att7_lextende;
    float att8_ldiscoun;
    float att27_rev;

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
            att18_ppartkey = iatt18_ppartkey[tid_part1];
        }
        // -------- hash join probe (opId: 3) --------
        // -------- multiprobe multi broadcast (opId: 3) --------
        int matchEnd3 = 0;
        int matchEndBuf3 = 0;
        int matchOffset3 = 0;
        int matchOffsetBuf3 = 0;
        int probeActive3 = active;
        int att18_ppartkey_bcbuf3;
        uint64_t hash3 = 0;
        if(probeActive3) {
            hash3 = 0;
            if(active) {
                hash3 = hash ( (hash3 + ((uint64_t)att18_ppartkey)));
            }
            probeActive3 = hashProbeMulti ( jht3, 12002430, hash3, matchOffsetBuf3, matchEndBuf3);
        }
        unsigned activeProbes3 = __ballot_sync(ALL_LANES,probeActive3);
        int num3 = 0;
        num3 = (matchEndBuf3 - matchOffsetBuf3);
        unsigned wideProbes3 = __ballot_sync(ALL_LANES,(num3 >= 32));
        att18_ppartkey_bcbuf3 = att18_ppartkey;
        while((activeProbes3 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes3)) {
                if((wideProbes3 > 0)) {
                    tupleLane = (__ffs(wideProbes3) - 1);
                    wideProbes3 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes3) - 1);
                }
                num = __shfl_sync(ALL_LANES,num3,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset3 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes3 -= (1 << tupleLane);
            }
            matchOffset3 += __shfl_sync(ALL_LANES,matchOffsetBuf3,broadcastLane);
            matchEnd3 = __shfl_sync(ALL_LANES,matchEndBuf3,broadcastLane);
            att18_ppartkey = __shfl_sync(ALL_LANES,att18_ppartkey_bcbuf3,broadcastLane);
            probeActive3 = (matchOffset3 < matchEnd3);
            while(__any_sync(ALL_LANES,probeActive3)) {
                active = probeActive3;
                active = 0;
                jpayl3 payl;
                if(probeActive3) {
                    payl = jht3_payload[matchOffset3];
                    att3_lpartkey = payl.att3_lpartkey;
                    att7_lextende = payl.att7_lextende;
                    att8_ldiscoun = payl.att8_ldiscoun;
                    active = 1;
                    active &= ((att3_lpartkey == att18_ppartkey));
                    matchOffset3 += 32;
                    probeActive3 &= ((matchOffset3 < matchEnd3));
                }
                // -------- map (opId: 4) --------
                if(active) {
                    att27_rev = (att7_lextende * (1.0 - att8_ldiscoun));
                }
                // -------- aggregation (opId: 5) --------
                int bucket = 0;
                if(active) {
                    atomicAdd(&(agg1[bucket]), ((float)att27_rev));
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation5(
    float* agg1, int* nout_result, float* oatt1_revenue) {
    float att1_revenue;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_aggregation5 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation5 = loopVar;
        active = (loopVar < 1);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 5) --------
        if(active) {
            att1_revenue = agg1[tid_aggregation5];
        }
        // -------- materialize (opId: 6) --------
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
    int* iatt3_lpartkey;
    iatt3_lpartkey = ( int*) map_memory_file ( "mmdb/lineitem_l_partkey" );
    float* iatt7_lextende;
    iatt7_lextende = ( float*) map_memory_file ( "mmdb/lineitem_l_extendedprice" );
    float* iatt8_ldiscoun;
    iatt8_ldiscoun = ( float*) map_memory_file ( "mmdb/lineitem_l_discount" );
    int* iatt18_ppartkey;
    iatt18_ppartkey = ( int*) map_memory_file ( "mmdb/part_p_partkey" );

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

    int* d_iatt3_lpartkey;
    cudaMalloc((void**) &d_iatt3_lpartkey, 6001215* sizeof(int) );
    float* d_iatt7_lextende;
    cudaMalloc((void**) &d_iatt7_lextende, 6001215* sizeof(float) );
    float* d_iatt8_ldiscoun;
    cudaMalloc((void**) &d_iatt8_ldiscoun, 6001215* sizeof(float) );
    int* d_iatt18_ppartkey;
    cudaMalloc((void**) &d_iatt18_ppartkey, 200000* sizeof(int) );
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

    multi_ht* d_jht3;
    cudaMalloc((void**) &d_jht3, 12002430* sizeof(multi_ht) );
    jpayl3* d_jht3_payload;
    cudaMalloc((void**) &d_jht3_payload, 12002430* sizeof(jpayl3) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht3, 12002430);
    }
    int* d_offs3;
    cudaMalloc((void**) &d_offs3, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs3, 0, 1);
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

    cudaMemcpy( d_iatt3_lpartkey, iatt3_lpartkey, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt7_lextende, iatt7_lextende, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt8_ldiscoun, iatt8_ldiscoun, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt18_ppartkey, iatt18_ppartkey, 200000 * sizeof(int), cudaMemcpyHostToDevice);
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
        krnl_lineitem1<<<gridsize, blocksize>>>(d_iatt3_lpartkey, d_iatt7_lextende, d_iatt8_ldiscoun, d_jht3, d_jht3_payload);
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

    std::clock_t start_scanMultiHT2 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht3, 12002430, d_offs3);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT2 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_lineitem1_ins3 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_lineitem1_ins<<<gridsize, blocksize>>>(d_iatt3_lpartkey, d_iatt7_lextende, d_iatt8_ldiscoun, d_jht3, d_jht3_payload, d_offs3);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem1_ins3 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem1_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem1_ins")
        }
    }

    std::clock_t start_krnl_part24 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_part2<<<gridsize, blocksize>>>(d_iatt18_ppartkey, d_jht3, d_jht3_payload, d_agg1);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_part24 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_part2! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_part2")
        }
    }

    std::clock_t start_krnl_aggregation55 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation5<<<gridsize, blocksize>>>(d_agg1, d_nout_result, d_oatt1_revenue);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation55 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation5! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation5")
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

    cudaFree( d_iatt3_lpartkey);
    cudaFree( d_iatt7_lextende);
    cudaFree( d_iatt8_ldiscoun);
    cudaFree( d_jht3);
    cudaFree( d_jht3_payload);
    cudaFree( d_offs3);
    cudaFree( d_iatt18_ppartkey);
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

    std::clock_t start_finish6 = std::clock();
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
    std::clock_t stop_finish6 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem1", (stop_krnl_lineitem11 - start_krnl_lineitem11) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT2 - start_scanMultiHT2) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem1_ins", (stop_krnl_lineitem1_ins3 - start_krnl_lineitem1_ins3) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_part2", (stop_krnl_part24 - start_krnl_part24) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation5", (stop_krnl_aggregation55 - start_krnl_aggregation55) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish6 - start_finish6) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime0 - start_totalKernelTime0) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
