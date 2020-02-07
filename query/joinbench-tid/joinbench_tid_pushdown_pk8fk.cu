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
    int tid;
};

__global__ void krnl_r_build1(
    int* iatt1_rbuild, int* iatt2_rlinenum, multi_ht* jht3, jpayl3* jht3_payload) {
    int att1_rbuild;
    int att2_rlinenum;

    int tid_r_build1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    // -------- scan --------
    while(!(flushPipeline)) {
        tid_r_build1 = loopVar;
        active = (tid_r_build1 < 100000000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att1_rbuild = iatt1_rbuild[tid_r_build1];
            att2_rlinenum = iatt2_rlinenum[tid_r_build1];
        }
        // -------- hash join build --------
        if(active) {
            uint64_t hash3 = 0;
            if(active) {
                hash3 = 0;
                hash3 = hash ( (hash3 + ((uint64_t)att1_rbuild)));
            }
            hashCountMulti ( jht3, 200000000, hash3);
        }
        loopVar += step;
    }

}

__global__ void krnl_r_build1_ins(
    int* iatt1_rbuild, int* iatt2_rlinenum, multi_ht* jht3, jpayl3* jht3_payload, int* offs3) {
    int att1_rbuild;
    int att2_rlinenum;

    int tid_r_build1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    // -------- scan --------
    while(!(flushPipeline)) {
        tid_r_build1 = loopVar;
        active = (tid_r_build1 < 100000000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        // -------- hash join build --------
        if(active) {
            uint64_t hash3 = 0;
            if(active) {
                hash3 = 0;
                hash3 = hash ( (hash3 + ((uint64_t)iatt1_rbuild[tid_r_build1])));
            }
            jpayl3 payl;
            payl.tid = tid_r_build1;
            hashInsertMulti ( jht3, jht3_payload, offs3, 200000000, hash3, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_s_probe2(
    int* iatt3_sprobe, int* iatt4_slinenum, multi_ht* jht3, jpayl3* jht3_payload, int* nout_result, int* oatt1_rbuild, int* oatt2_rlinenum, int* oatt3_sprobe, int* oatt4_slinenum, int* iatt1_rbuild, int* iatt2_rlinenum) {
    int att3_sprobe;
    int att4_slinenum;
    unsigned warplane = (threadIdx.x % 32);
    int att1_rbuild;
    int att2_rlinenum;
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_s_probe1 = 0;
    int tid_r_build1;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    // -------- scan --------
    while(!(flushPipeline)) {
        tid_s_probe1 = loopVar;
      //active = (tid_s_probe1 < 3125000);
      //active = (tid_s_probe1 < 100000000);
        active = (tid_s_probe1 < 12500000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        // -------- hash join probe --------
        // -------- multiprobe multi broadcast --------
        int matchEnd3 = 0;
        int matchEndBuf3 = 0;
        int matchOffset3 = 0;
        int matchOffsetBuf3 = 0;
        int probeActive3 = active;

        //int att3_sprobe_bcbuf3;
        //int att4_slinenum_bcbuf3;
        int tid_s_probe1_bcbuf;

        uint64_t hash3 = 0;
        if(probeActive3) {
            hash3 = 0;
            hash3 = hash ( (hash3 + ((uint64_t)iatt3_sprobe[tid_s_probe1])));
            probeActive3 = hashProbeMulti ( jht3, 200000000, hash3, matchOffsetBuf3, matchEndBuf3);
        }
        unsigned activeProbes3 = __ballot_sync(ALL_LANES,probeActive3);
        int num3 = 0;
        num3 = (matchEndBuf3 - matchOffsetBuf3);
        unsigned wideProbes3 = __ballot_sync(ALL_LANES,(num3 >= 32));

        //att3_sprobe_bcbuf3 = att3_sprobe;
        //att4_slinenum_bcbuf3 = att4_slinenum;
        tid_s_probe1_bcbuf = tid_s_probe1;

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

            //att3_sprobe = __shfl_sync(ALL_LANES,att3_sprobe_bcbuf3,broadcastLane);
            //att4_slinenum = __shfl_sync(ALL_LANES,att4_slinenum_bcbuf3,broadcastLane);
            tid_s_probe1 = __shfl_sync(ALL_LANES,tid_s_probe1_bcbuf,broadcastLane);

            probeActive3 = (matchOffset3 < matchEnd3);
            while(__any_sync(ALL_LANES,probeActive3)) {
                active = 0;
                jpayl3 payl;
                if(probeActive3) {
                    payl = jht3_payload[matchOffset3];
                    tid_r_build1 = payl.tid;
                    active = 1;
                    active &= ((iatt1_rbuild[tid_r_build1] == iatt3_sprobe[tid_s_probe1]));
                    matchOffset3 += 32;
                    probeActive3 &= ((matchOffset3 < matchEnd3));
                }
                // -------- materialize --------
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
                    oatt1_rbuild[wp] = iatt1_rbuild[tid_r_build1];
                    oatt2_rlinenum[wp] = iatt2_rlinenum[tid_r_build1];
                    oatt3_sprobe[wp] = iatt3_sprobe[tid_s_probe1];
                    oatt4_slinenum[wp] = iatt4_slinenum[tid_s_probe1];
                }
            }
        }
        loopVar += step;
    }

}

int main() {
    std::clock_t start_import2 = std::clock();
    int* iatt1_rbuild;
    iatt1_rbuild = ( int*) map_memory_file ( "mmdb/r_build_r_build" );
    int* iatt2_rlinenum;
    iatt2_rlinenum = ( int*) map_memory_file ( "mmdb/r_build_r_linenumber" );
    int* iatt3_sprobe;
    iatt3_sprobe = ( int*) map_memory_file ( "mmdb/s_probe_s_probe" );
    int* iatt4_slinenum;
    iatt4_slinenum = ( int*) map_memory_file ( "mmdb/s_probe_s_linenumber" );
    std::clock_t stop_import2 = std::clock();

    std::clock_t start_declare3 = std::clock();
    int nout_result;
    std::vector < int > oatt1_rbuild(100000000);
    std::vector < int > oatt2_rlinenum(100000000);
    std::vector < int > oatt3_sprobe(100000000);
    std::vector < int > oatt4_slinenum(100000000);
    std::clock_t stop_declare3 = std::clock();

    std::clock_t start_wake_up_gpu4 = std::clock();
    // wake up gpu
    cudaDeviceSynchronize();
    std::clock_t stop_wake_up_gpu4 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in wake up gpu! " << cudaGetErrorString( err ) << std::endl;
            ERROR("wake up gpu")
        }
    }

    std::clock_t start_cuda_malloc5 = std::clock();
    int* d_iatt1_rbuild;
    cudaMalloc((void**) &d_iatt1_rbuild, 100000000* sizeof(int) );
    int* d_iatt2_rlinenum;
    cudaMalloc((void**) &d_iatt2_rlinenum, 100000000* sizeof(int) );
    int* d_iatt3_sprobe;
  //cudaMalloc((void**) &d_iatt3_sprobe, 3125000* sizeof(int) );
  //cudaMalloc((void**) &d_iatt3_sprobe, 100000000* sizeof(int) );
    cudaMalloc((void**) &d_iatt3_sprobe, 12500000* sizeof(int) );
    int* d_iatt4_slinenum;
  //cudaMalloc((void**) &d_iatt4_slinenum, 3125000* sizeof(int) );
  //cudaMalloc((void**) &d_iatt4_slinenum, 100000000* sizeof(int) );
    cudaMalloc((void**) &d_iatt4_slinenum, 12500000* sizeof(int) );
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    int* d_oatt1_rbuild;
    cudaMalloc((void**) &d_oatt1_rbuild, 100000000* sizeof(int) );
    int* d_oatt2_rlinenum;
    cudaMalloc((void**) &d_oatt2_rlinenum, 100000000* sizeof(int) );
    int* d_oatt3_sprobe;
    cudaMalloc((void**) &d_oatt3_sprobe, 100000000* sizeof(int) );
    int* d_oatt4_slinenum;
    cudaMalloc((void**) &d_oatt4_slinenum, 100000000* sizeof(int) );
    cudaDeviceSynchronize();
    std::clock_t stop_cuda_malloc5 = std::clock();
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

    std::clock_t start_cuda_mallocHT6 = std::clock();
    multi_ht* d_jht3;
    cudaMalloc((void**) &d_jht3, 200000000* sizeof(multi_ht) );
    jpayl3* d_jht3_payload;
    cudaMalloc((void**) &d_jht3_payload, 200000000* sizeof(jpayl3) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht3, 200000000);
    }
    int* d_offs3;
    cudaMalloc((void**) &d_offs3, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs3, 0, 1);
    }
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_nout_result, 0, 1);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_cuda_mallocHT6 = std::clock();
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

    std::clock_t start_cuda_memcpy_in7 = std::clock();
    cudaMemcpy( d_iatt1_rbuild, iatt1_rbuild, 100000000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt2_rlinenum, iatt2_rlinenum, 100000000 * sizeof(int), cudaMemcpyHostToDevice);
  //cudaMemcpy( d_iatt3_sprobe, iatt3_sprobe, 3125000 * sizeof(int), cudaMemcpyHostToDevice);
  //cudaMemcpy( d_iatt3_sprobe, iatt3_sprobe, 100000000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt3_sprobe, iatt3_sprobe, 12500000 * sizeof(int), cudaMemcpyHostToDevice);
  //cudaMemcpy( d_iatt4_slinenum, iatt4_slinenum, 3125000 * sizeof(int), cudaMemcpyHostToDevice);
  //cudaMemcpy( d_iatt4_slinenum, iatt4_slinenum, 100000000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt4_slinenum, iatt4_slinenum, 12500000 * sizeof(int), cudaMemcpyHostToDevice);
    //cudaMemcpy( d_iatt3_sprobe, iatt3_sprobe, 100000000 * sizeof(int), cudaMemcpyHostToDevice);
    //cudaMemcpy( d_iatt4_slinenum, iatt4_slinenum, 100000000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    std::clock_t stop_cuda_memcpy_in7 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime8 = std::clock();
    std::clock_t start_krnl_r_build1_920_1289 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_r_build1<<<gridsize, blocksize>>>(d_iatt1_rbuild, d_iatt2_rlinenum, d_jht3, d_jht3_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_r_build1_920_1289 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_r_build1 920 128! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_r_build1 920 128")
        }
    }

    std::clock_t start_scanMultiHT_920_12810 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht3, 200000000, d_offs3);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT_920_12810 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT 920 128! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT 920 128")
        }
    }

    std::clock_t start_krnl_r_build1_ins_920_12811 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_r_build1_ins<<<gridsize, blocksize>>>(d_iatt1_rbuild, d_iatt2_rlinenum, d_jht3, d_jht3_payload, d_offs3);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_r_build1_ins_920_12811 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_r_build1_ins 920 128! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_r_build1_ins 920 128")
        }
    }

    std::clock_t start_krnl_s_probe2_920_12812 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_s_probe2<<<gridsize, blocksize>>>(d_iatt3_sprobe, d_iatt4_slinenum, d_jht3, d_jht3_payload, d_nout_result, d_oatt1_rbuild, d_oatt2_rlinenum, d_oatt3_sprobe, d_oatt4_slinenum,d_iatt1_rbuild, d_iatt2_rlinenum);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_s_probe2_920_12812 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_s_probe2 920 128! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_s_probe2 920 128")
        }
    }

    std::clock_t stop_totalKernelTime8 = std::clock();
    std::clock_t start_cuda_memcpy_out13 = std::clock();
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt1_rbuild.data(), d_oatt1_rbuild, 100000000 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt2_rlinenum.data(), d_oatt2_rlinenum, 100000000 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt3_sprobe.data(), d_oatt3_sprobe, 100000000 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt4_slinenum.data(), d_oatt4_slinenum, 100000000 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    std::clock_t stop_cuda_memcpy_out13 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    std::clock_t start_cuda_free14 = std::clock();
    cudaFree( d_iatt1_rbuild);
    cudaFree( d_iatt2_rlinenum);
    cudaFree( d_jht3);
    cudaFree( d_jht3_payload);
    cudaFree( d_offs3);
    cudaFree( d_iatt3_sprobe);
    cudaFree( d_iatt4_slinenum);
    cudaFree( d_nout_result);
    cudaFree( d_oatt1_rbuild);
    cudaFree( d_oatt2_rlinenum);
    cudaFree( d_oatt3_sprobe);
    cudaFree( d_oatt4_slinenum);
    cudaDeviceSynchronize();
    std::clock_t stop_cuda_free14 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda free! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda free")
        }
    }

    std::clock_t start_finish15 = std::clock();
    printf("\nResult: %i tuples\n", nout_result);
    if((nout_result > 100000000)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("r_build: ");
        printf("%8i", oatt1_rbuild[pv]);
        printf("  ");
        printf("r_linenumber: ");
        printf("%8i", oatt2_rlinenum[pv]);
        printf("  ");
        printf("s_probe: ");
        printf("%8i", oatt3_sprobe[pv]);
        printf("  ");
        printf("s_linenumber: ");
        printf("%8i", oatt4_slinenum[pv]);
        printf("  ");
        printf("\n");
    }
    if((nout_result > 10)) {
        printf("[...]\n");
    }
    printf("\n");
    //FILE* outFile;
    //outFile = fopen("queryresult.csv", "w");
    //fprintf(outFile, "r_build, ");
    //fprintf(outFile, "r_linenumber, ");
    //fprintf(outFile, "s_probe, ");
    //fprintf(outFile, "s_linenumber, ");
    //fprintf(outFile, "\n");
    //for ( int pv = 0; (pv < nout_result); pv += 1) {
    //    fprintf(outFile, "%8i  ", oatt1_rbuild[pv]);
    //    fprintf(outFile, "%8i  ", oatt2_rlinenum[pv]);
    //    fprintf(outFile, "%8i  ", oatt3_sprobe[pv]);
    //    fprintf(outFile, "%8i  ", oatt4_slinenum[pv]);
    //    fprintf(outFile, "\n");
    //}
    std::clock_t stop_finish15 = std::clock();

    std::cout << "import: " << (stop_import2 - start_import2) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "declare: " << (stop_declare3 - start_declare3) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "wake up gpu: " << (stop_wake_up_gpu4 - start_wake_up_gpu4) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "cuda malloc: " << (stop_cuda_malloc5 - start_cuda_malloc5) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "cuda mallocHT: " << (stop_cuda_mallocHT6 - start_cuda_mallocHT6) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "cuda memcpy in: " << (stop_cuda_memcpy_in7 - start_cuda_memcpy_in7) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "krnl_r_build1 920 128: " << (stop_krnl_r_build1_920_1289 - start_krnl_r_build1_920_1289) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "scanMultiHT 920 128: " << (stop_scanMultiHT_920_12810 - start_scanMultiHT_920_12810) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "krnl_r_build1_ins 920 128: " << (stop_krnl_r_build1_ins_920_12811 - start_krnl_r_build1_ins_920_12811) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "krnl_s_probe2 920 128: " << (stop_krnl_s_probe2_920_12812 - start_krnl_s_probe2_920_12812) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "cuda memcpy out: " << (stop_cuda_memcpy_out13 - start_cuda_memcpy_out13) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "cuda free: " << (stop_cuda_free14 - start_cuda_free14) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "finish: " << (stop_finish15 - start_finish15) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
    std::cout << "totalKernelTime: " << (stop_totalKernelTime8 - start_totalKernelTime8) / (double) (CLOCKS_PER_SEC / 1000) << " ms" << std::endl;
}
