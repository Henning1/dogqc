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
struct jpayl12 {
    int att4_ssuppkey;
    int att7_snationk;
};
struct jpayl6 {
    int att11_nnationk;
    str_t att12_nname;
    int att15_nnationk;
    str_t att16_nname;
};
struct jpayl8 {
    int att11_nnationk;
    str_t att12_nname;
    str_t att16_nname;
    int att19_ccustkey;
};
struct jpayl11 {
    int att11_nnationk;
    str_t att12_nname;
    str_t att16_nname;
    int att27_oorderke;
};
struct apayl15 {
    str_t att12_nname;
    str_t att16_nname;
    unsigned att53_lyear;
};

__global__ void krnl_supplier1(
    int* iatt4_ssuppkey, int* iatt7_snationk, unique_ht<jpayl12>* jht12) {
    int att4_ssuppkey;
    int att7_snationk;

    int tid_supplier1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_supplier1 = loopVar;
        active = (loopVar < 10000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att4_ssuppkey = iatt4_ssuppkey[tid_supplier1];
            att7_snationk = iatt7_snationk[tid_supplier1];
        }
        // -------- hash join build (opId: 12) --------
        if(active) {
            jpayl12 payl12;
            payl12.att4_ssuppkey = att4_ssuppkey;
            payl12.att7_snationk = att7_snationk;
            uint64_t hash12;
            hash12 = 0;
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att7_snationk)));
            }
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att4_ssuppkey)));
            }
            hashBuildUnique ( jht12, 20000, hash12, &(payl12));
        }
        loopVar += step;
    }

}

__global__ void krnl_nation2(
    int* iatt11_nnationk, size_t* iatt12_nname_offset, char* iatt12_nname_char, int* nout_inner4, int* itm_inner4_n_nationkey, str_t* itm_inner4_n_name) {
    int att11_nnationk;
    str_t att12_nname;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_nation1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_nation1 = loopVar;
        active = (loopVar < 25);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att11_nnationk = iatt11_nnationk[tid_nation1];
            att12_nname = stringScan ( iatt12_nname_offset, iatt12_nname_char, tid_nation1);
        }
        // -------- nested join: materialize inner  (opId: 4) --------
        int wp;
        int writeMask;
        int numProj;
        writeMask = __ballot_sync(ALL_LANES,active);
        numProj = __popc(writeMask);
        if((warplane == 0)) {
            wp = atomicAdd(nout_inner4, numProj);
        }
        wp = __shfl_sync(ALL_LANES,wp,0);
        wp = (wp + __popc((writeMask & prefixlanes)));
        if(active) {
            itm_inner4_n_nationkey[wp] = att11_nnationk;
            itm_inner4_n_name[wp] = att12_nname;
        }
        loopVar += step;
    }

}

__global__ void krnl_nation23(
    int* iatt15_nnationk, size_t* iatt16_nname_offset, char* iatt16_nname_char, int* nout_inner4, int* itm_inner4_n_nationkey, str_t* itm_inner4_n_name, unique_ht<jpayl6>* jht6) {
    int att15_nnationk;
    str_t att16_nname;
    int att11_nnationk;
    str_t att12_nname;
    str_t c1 = stringConstant ( "GERMANY", 7);
    str_t c2 = stringConstant ( "FRANCE", 6);
    str_t c3 = stringConstant ( "FRANCE", 6);
    str_t c4 = stringConstant ( "GERMANY", 7);

    int tid_nation2 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_nation2 = loopVar;
        active = (loopVar < 25);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att15_nnationk = iatt15_nnationk[tid_nation2];
            att16_nname = stringScan ( iatt16_nname_offset, iatt16_nname_char, tid_nation2);
        }
        // -------- nested join: loop inner  (opId: 4) --------
        int outerActive4 = active;
        for ( int tid_inner40 = 0; (tid_inner40 < *(nout_inner4)); (tid_inner40++)) {
            active = outerActive4;
            if(active) {
                att11_nnationk = itm_inner4_n_nationkey[tid_inner40];
                att12_nname = itm_inner4_n_name[tid_inner40];
            }
            if(active) {
                active = ((stringEquals ( att12_nname, c1) && stringEquals ( att16_nname, c2)) || (stringEquals ( att12_nname, c3) && stringEquals ( att16_nname, c4)));
            }
            // -------- hash join build (opId: 6) --------
            if(active) {
                jpayl6 payl6;
                payl6.att11_nnationk = att11_nnationk;
                payl6.att12_nname = att12_nname;
                payl6.att15_nnationk = att15_nnationk;
                payl6.att16_nname = att16_nname;
                uint64_t hash6;
                hash6 = 0;
                if(active) {
                    hash6 = hash ( (hash6 + ((uint64_t)att15_nnationk)));
                }
                hashBuildUnique ( jht6, 1250, hash6, &(payl6));
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_customer5(
    int* iatt19_ccustkey, int* iatt22_cnationk, unique_ht<jpayl6>* jht6, unique_ht<jpayl8>* jht8) {
    int att19_ccustkey;
    int att22_cnationk;
    int att11_nnationk;
    str_t att12_nname;
    int att15_nnationk;
    str_t att16_nname;

    int tid_customer1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_customer1 = loopVar;
        active = (loopVar < 150000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att19_ccustkey = iatt19_ccustkey[tid_customer1];
            att22_cnationk = iatt22_cnationk[tid_customer1];
        }
        // -------- hash join probe (opId: 6) --------
        uint64_t hash6 = 0;
        if(active) {
            hash6 = 0;
            if(active) {
                hash6 = hash ( (hash6 + ((uint64_t)att22_cnationk)));
            }
        }
        jpayl6* probepayl6;
        int numLookups6 = 0;
        if(active) {
            active = hashProbeUnique ( jht6, 1250, hash6, numLookups6, &(probepayl6));
        }
        int bucketFound6 = 0;
        int probeActive6 = active;
        while((probeActive6 && !(bucketFound6))) {
            jpayl6 jprobepayl6 = *(probepayl6);
            att11_nnationk = jprobepayl6.att11_nnationk;
            att12_nname = jprobepayl6.att12_nname;
            att15_nnationk = jprobepayl6.att15_nnationk;
            att16_nname = jprobepayl6.att16_nname;
            bucketFound6 = 1;
            bucketFound6 &= ((att15_nnationk == att22_cnationk));
            if(!(bucketFound6)) {
                probeActive6 = hashProbeUnique ( jht6, 1250, hash6, numLookups6, &(probepayl6));
            }
        }
        active = bucketFound6;
        // -------- hash join build (opId: 8) --------
        if(active) {
            jpayl8 payl8;
            payl8.att11_nnationk = att11_nnationk;
            payl8.att12_nname = att12_nname;
            payl8.att16_nname = att16_nname;
            payl8.att19_ccustkey = att19_ccustkey;
            uint64_t hash8;
            hash8 = 0;
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att19_ccustkey)));
            }
            hashBuildUnique ( jht8, 30000, hash8, &(payl8));
        }
        loopVar += step;
    }

}

__global__ void krnl_orders7(
    int* iatt27_oorderke, int* iatt28_ocustkey, unique_ht<jpayl8>* jht8, unique_ht<jpayl11>* jht11) {
    int att27_oorderke;
    int att28_ocustkey;
    int att11_nnationk;
    str_t att12_nname;
    str_t att16_nname;
    int att19_ccustkey;

    int tid_orders1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_orders1 = loopVar;
        active = (loopVar < 1500000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att27_oorderke = iatt27_oorderke[tid_orders1];
            att28_ocustkey = iatt28_ocustkey[tid_orders1];
        }
        // -------- hash join probe (opId: 8) --------
        uint64_t hash8 = 0;
        if(active) {
            hash8 = 0;
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att28_ocustkey)));
            }
        }
        jpayl8* probepayl8;
        int numLookups8 = 0;
        if(active) {
            active = hashProbeUnique ( jht8, 30000, hash8, numLookups8, &(probepayl8));
        }
        int bucketFound8 = 0;
        int probeActive8 = active;
        while((probeActive8 && !(bucketFound8))) {
            jpayl8 jprobepayl8 = *(probepayl8);
            att11_nnationk = jprobepayl8.att11_nnationk;
            att12_nname = jprobepayl8.att12_nname;
            att16_nname = jprobepayl8.att16_nname;
            att19_ccustkey = jprobepayl8.att19_ccustkey;
            bucketFound8 = 1;
            bucketFound8 &= ((att19_ccustkey == att28_ocustkey));
            if(!(bucketFound8)) {
                probeActive8 = hashProbeUnique ( jht8, 30000, hash8, numLookups8, &(probepayl8));
            }
        }
        active = bucketFound8;
        // -------- hash join build (opId: 11) --------
        if(active) {
            jpayl11 payl11;
            payl11.att11_nnationk = att11_nnationk;
            payl11.att12_nname = att12_nname;
            payl11.att16_nname = att16_nname;
            payl11.att27_oorderke = att27_oorderke;
            uint64_t hash11;
            hash11 = 0;
            if(active) {
                hash11 = hash ( (hash11 + ((uint64_t)att27_oorderke)));
            }
            hashBuildUnique ( jht11, 300000, hash11, &(payl11));
        }
        loopVar += step;
    }

}

__global__ void krnl_lineitem9(
    int* iatt36_lorderke, int* iatt38_lsuppkey, float* iatt41_lextende, float* iatt42_ldiscoun, unsigned* iatt46_lshipdat, unique_ht<jpayl11>* jht11, unique_ht<jpayl12>* jht12, agg_ht<apayl15>* aht15, float* agg1, float* agg2, int* agg3) {
    int att36_lorderke;
    int att38_lsuppkey;
    float att41_lextende;
    float att42_ldiscoun;
    unsigned att46_lshipdat;
    int att11_nnationk;
    str_t att12_nname;
    str_t att16_nname;
    int att27_oorderke;
    int att4_ssuppkey;
    int att7_snationk;
    float att52_volume;
    unsigned att53_lyear;

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
            att36_lorderke = iatt36_lorderke[tid_lineitem1];
            att38_lsuppkey = iatt38_lsuppkey[tid_lineitem1];
            att41_lextende = iatt41_lextende[tid_lineitem1];
            att42_ldiscoun = iatt42_ldiscoun[tid_lineitem1];
            att46_lshipdat = iatt46_lshipdat[tid_lineitem1];
        }
        // -------- selection (opId: 10) --------
        if(active) {
            active = ((att46_lshipdat >= 19950101) && (att46_lshipdat <= 19961231));
        }
        // -------- hash join probe (opId: 11) --------
        uint64_t hash11 = 0;
        if(active) {
            hash11 = 0;
            if(active) {
                hash11 = hash ( (hash11 + ((uint64_t)att36_lorderke)));
            }
        }
        jpayl11* probepayl11;
        int numLookups11 = 0;
        if(active) {
            active = hashProbeUnique ( jht11, 300000, hash11, numLookups11, &(probepayl11));
        }
        int bucketFound11 = 0;
        int probeActive11 = active;
        while((probeActive11 && !(bucketFound11))) {
            jpayl11 jprobepayl11 = *(probepayl11);
            att11_nnationk = jprobepayl11.att11_nnationk;
            att12_nname = jprobepayl11.att12_nname;
            att16_nname = jprobepayl11.att16_nname;
            att27_oorderke = jprobepayl11.att27_oorderke;
            bucketFound11 = 1;
            bucketFound11 &= ((att27_oorderke == att36_lorderke));
            if(!(bucketFound11)) {
                probeActive11 = hashProbeUnique ( jht11, 300000, hash11, numLookups11, &(probepayl11));
            }
        }
        active = bucketFound11;
        // -------- hash join probe (opId: 12) --------
        uint64_t hash12 = 0;
        if(active) {
            hash12 = 0;
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att11_nnationk)));
            }
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att38_lsuppkey)));
            }
        }
        jpayl12* probepayl12;
        int numLookups12 = 0;
        if(active) {
            active = hashProbeUnique ( jht12, 20000, hash12, numLookups12, &(probepayl12));
        }
        int bucketFound12 = 0;
        int probeActive12 = active;
        while((probeActive12 && !(bucketFound12))) {
            jpayl12 jprobepayl12 = *(probepayl12);
            att4_ssuppkey = jprobepayl12.att4_ssuppkey;
            att7_snationk = jprobepayl12.att7_snationk;
            bucketFound12 = 1;
            bucketFound12 &= ((att7_snationk == att11_nnationk));
            bucketFound12 &= ((att4_ssuppkey == att38_lsuppkey));
            if(!(bucketFound12)) {
                probeActive12 = hashProbeUnique ( jht12, 20000, hash12, numLookups12, &(probepayl12));
            }
        }
        active = bucketFound12;
        // -------- map (opId: 13) --------
        if(active) {
            att52_volume = (att41_lextende * (1 - att42_ldiscoun));
        }
        // -------- map (opId: 14) --------
        if(active) {
            att53_lyear = (att46_lshipdat / 10000);
        }
        // -------- aggregation (opId: 15) --------
        int bucket = 0;
        if(active) {
            uint64_t hash15 = 0;
            hash15 = 0;
            hash15 = hash ( (hash15 + stringHash ( att12_nname)));
            hash15 = hash ( (hash15 + stringHash ( att16_nname)));
            if(active) {
                hash15 = hash ( (hash15 + ((uint64_t)att53_lyear)));
            }
            apayl15 payl;
            payl.att12_nname = att12_nname;
            payl.att16_nname = att16_nname;
            payl.att53_lyear = att53_lyear;
            int bucketFound = 0;
            int numLookups = 0;
            while(!(bucketFound)) {
                bucket = hashAggregateGetBucket ( aht15, 24004, hash15, numLookups, &(payl));
                apayl15 probepayl = aht15[bucket].payload;
                bucketFound = 1;
                bucketFound &= (stringEquals ( payl.att12_nname, probepayl.att12_nname));
                bucketFound &= (stringEquals ( payl.att16_nname, probepayl.att16_nname));
                bucketFound &= ((payl.att53_lyear == probepayl.att53_lyear));
            }
        }
        if(active) {
            atomicAdd(&(agg1[bucket]), ((float)att52_volume));
            atomicAdd(&(agg2[bucket]), ((float)att52_volume));
            atomicAdd(&(agg3[bucket]), ((int)1));
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation15(
    agg_ht<apayl15>* aht15, float* agg1, float* agg2, int* agg3, int* nout_result, str_offs* oatt12_nname_offset, char* iatt12_nname_char, str_offs* oatt16_nname_offset, char* iatt16_nname_char, unsigned* oatt53_lyear, float* oatt1_sumvolum, float* oatt2_avgvolum) {
    str_t att12_nname;
    str_t att16_nname;
    unsigned att53_lyear;
    float att1_sumvolum;
    float att2_avgvolum;
    int att3_countagg;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_aggregation15 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation15 = loopVar;
        active = (loopVar < 24004);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 15) --------
        if(active) {
            active &= ((aht15[tid_aggregation15].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl15 payl = aht15[tid_aggregation15].payload;
            att12_nname = payl.att12_nname;
            att16_nname = payl.att16_nname;
            att53_lyear = payl.att53_lyear;
        }
        if(active) {
            att1_sumvolum = agg1[tid_aggregation15];
            att2_avgvolum = agg2[tid_aggregation15];
            att3_countagg = agg3[tid_aggregation15];
            att2_avgvolum = (att2_avgvolum / ((float)att3_countagg));
        }
        // -------- projection (no code) (opId: 16) --------
        // -------- materialize (opId: 17) --------
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
            oatt12_nname_offset[wp] = toStringOffset ( iatt12_nname_char, att12_nname);
            oatt16_nname_offset[wp] = toStringOffset ( iatt16_nname_char, att16_nname);
            oatt53_lyear[wp] = att53_lyear;
            oatt1_sumvolum[wp] = att1_sumvolum;
            oatt2_avgvolum[wp] = att2_avgvolum;
        }
        loopVar += step;
    }

}

int main() {
    int* iatt4_ssuppkey;
    iatt4_ssuppkey = ( int*) map_memory_file ( "mmdb/supplier_s_suppkey" );
    int* iatt7_snationk;
    iatt7_snationk = ( int*) map_memory_file ( "mmdb/supplier_s_nationkey" );
    int* iatt11_nnationk;
    iatt11_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    size_t* iatt12_nname_offset;
    iatt12_nname_offset = ( size_t*) map_memory_file ( "mmdb/nation_n_name_offset" );
    char* iatt12_nname_char;
    iatt12_nname_char = ( char*) map_memory_file ( "mmdb/nation_n_name_char" );
    int* iatt15_nnationk;
    iatt15_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    size_t* iatt16_nname_offset;
    iatt16_nname_offset = ( size_t*) map_memory_file ( "mmdb/nation_n_name_offset" );
    char* iatt16_nname_char;
    iatt16_nname_char = ( char*) map_memory_file ( "mmdb/nation_n_name_char" );
    int* iatt19_ccustkey;
    iatt19_ccustkey = ( int*) map_memory_file ( "mmdb/customer_c_custkey" );
    int* iatt22_cnationk;
    iatt22_cnationk = ( int*) map_memory_file ( "mmdb/customer_c_nationkey" );
    int* iatt27_oorderke;
    iatt27_oorderke = ( int*) map_memory_file ( "mmdb/orders_o_orderkey" );
    int* iatt28_ocustkey;
    iatt28_ocustkey = ( int*) map_memory_file ( "mmdb/orders_o_custkey" );
    int* iatt36_lorderke;
    iatt36_lorderke = ( int*) map_memory_file ( "mmdb/lineitem_l_orderkey" );
    int* iatt38_lsuppkey;
    iatt38_lsuppkey = ( int*) map_memory_file ( "mmdb/lineitem_l_suppkey" );
    float* iatt41_lextende;
    iatt41_lextende = ( float*) map_memory_file ( "mmdb/lineitem_l_extendedprice" );
    float* iatt42_ldiscoun;
    iatt42_ldiscoun = ( float*) map_memory_file ( "mmdb/lineitem_l_discount" );
    unsigned* iatt46_lshipdat;
    iatt46_lshipdat = ( unsigned*) map_memory_file ( "mmdb/lineitem_l_shipdate" );

    int nout_inner4;
    int nout_result;
    std::vector < str_offs > oatt12_nname_offset(12002);
    std::vector < str_offs > oatt16_nname_offset(12002);
    std::vector < unsigned > oatt53_lyear(12002);
    std::vector < float > oatt1_sumvolum(12002);
    std::vector < float > oatt2_avgvolum(12002);

    // wake up gpu
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in wake up gpu! " << cudaGetErrorString( err ) << std::endl;
            ERROR("wake up gpu")
        }
    }

    int* d_iatt4_ssuppkey;
    cudaMalloc((void**) &d_iatt4_ssuppkey, 10000* sizeof(int) );
    int* d_iatt7_snationk;
    cudaMalloc((void**) &d_iatt7_snationk, 10000* sizeof(int) );
    int* d_iatt11_nnationk;
    cudaMalloc((void**) &d_iatt11_nnationk, 25* sizeof(int) );
    size_t* d_iatt12_nname_offset;
    cudaMalloc((void**) &d_iatt12_nname_offset, (25 + 1)* sizeof(size_t) );
    char* d_iatt12_nname_char;
    cudaMalloc((void**) &d_iatt12_nname_char, 186* sizeof(char) );
    int* d_nout_inner4;
    cudaMalloc((void**) &d_nout_inner4, 1* sizeof(int) );
    int* d_itm_inner4_n_nationkey;
    cudaMalloc((void**) &d_itm_inner4_n_nationkey, 25* sizeof(int) );
    str_t* d_itm_inner4_n_name;
    cudaMalloc((void**) &d_itm_inner4_n_name, 25* sizeof(str_t) );
    int* d_iatt15_nnationk;
    d_iatt15_nnationk = d_iatt11_nnationk;
    size_t* d_iatt16_nname_offset;
    d_iatt16_nname_offset = d_iatt12_nname_offset;
    char* d_iatt16_nname_char;
    d_iatt16_nname_char = d_iatt12_nname_char;
    int* d_iatt19_ccustkey;
    cudaMalloc((void**) &d_iatt19_ccustkey, 150000* sizeof(int) );
    int* d_iatt22_cnationk;
    cudaMalloc((void**) &d_iatt22_cnationk, 150000* sizeof(int) );
    int* d_iatt27_oorderke;
    cudaMalloc((void**) &d_iatt27_oorderke, 1500000* sizeof(int) );
    int* d_iatt28_ocustkey;
    cudaMalloc((void**) &d_iatt28_ocustkey, 1500000* sizeof(int) );
    int* d_iatt36_lorderke;
    cudaMalloc((void**) &d_iatt36_lorderke, 6001215* sizeof(int) );
    int* d_iatt38_lsuppkey;
    cudaMalloc((void**) &d_iatt38_lsuppkey, 6001215* sizeof(int) );
    float* d_iatt41_lextende;
    cudaMalloc((void**) &d_iatt41_lextende, 6001215* sizeof(float) );
    float* d_iatt42_ldiscoun;
    cudaMalloc((void**) &d_iatt42_ldiscoun, 6001215* sizeof(float) );
    unsigned* d_iatt46_lshipdat;
    cudaMalloc((void**) &d_iatt46_lshipdat, 6001215* sizeof(unsigned) );
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    str_offs* d_oatt12_nname_offset;
    cudaMalloc((void**) &d_oatt12_nname_offset, 12002* sizeof(str_offs) );
    str_offs* d_oatt16_nname_offset;
    cudaMalloc((void**) &d_oatt16_nname_offset, 12002* sizeof(str_offs) );
    unsigned* d_oatt53_lyear;
    cudaMalloc((void**) &d_oatt53_lyear, 12002* sizeof(unsigned) );
    float* d_oatt1_sumvolum;
    cudaMalloc((void**) &d_oatt1_sumvolum, 12002* sizeof(float) );
    float* d_oatt2_avgvolum;
    cudaMalloc((void**) &d_oatt2_avgvolum, 12002* sizeof(float) );
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

    unique_ht<jpayl12>* d_jht12;
    cudaMalloc((void**) &d_jht12, 20000* sizeof(unique_ht<jpayl12>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht12, 20000);
    }
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_nout_inner4, 0, 1);
    }
    unique_ht<jpayl6>* d_jht6;
    cudaMalloc((void**) &d_jht6, 1250* sizeof(unique_ht<jpayl6>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht6, 1250);
    }
    unique_ht<jpayl8>* d_jht8;
    cudaMalloc((void**) &d_jht8, 30000* sizeof(unique_ht<jpayl8>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht8, 30000);
    }
    unique_ht<jpayl11>* d_jht11;
    cudaMalloc((void**) &d_jht11, 300000* sizeof(unique_ht<jpayl11>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht11, 300000);
    }
    agg_ht<apayl15>* d_aht15;
    cudaMalloc((void**) &d_aht15, 24004* sizeof(agg_ht<apayl15>) );
    {
        int gridsize=920;
        int blocksize=128;
        initAggHT<<<gridsize, blocksize>>>(d_aht15, 24004);
    }
    float* d_agg1;
    cudaMalloc((void**) &d_agg1, 24004* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg1, 0.0f, 24004);
    }
    float* d_agg2;
    cudaMalloc((void**) &d_agg2, 24004* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg2, 0.0f, 24004);
    }
    int* d_agg3;
    cudaMalloc((void**) &d_agg3, 24004* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg3, 0, 24004);
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

    cudaMemcpy( d_iatt4_ssuppkey, iatt4_ssuppkey, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt7_snationk, iatt7_snationk, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt11_nnationk, iatt11_nnationk, 25 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt12_nname_offset, iatt12_nname_offset, (25 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt12_nname_char, iatt12_nname_char, 186 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt19_ccustkey, iatt19_ccustkey, 150000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt22_cnationk, iatt22_cnationk, 150000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt27_oorderke, iatt27_oorderke, 1500000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt28_ocustkey, iatt28_ocustkey, 1500000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt36_lorderke, iatt36_lorderke, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt38_lsuppkey, iatt38_lsuppkey, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt41_lextende, iatt41_lextende, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt42_ldiscoun, iatt42_ldiscoun, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt46_lshipdat, iatt46_lshipdat, 6001215 * sizeof(unsigned), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime47 = std::clock();
    std::clock_t start_krnl_supplier148 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_supplier1<<<gridsize, blocksize>>>(d_iatt4_ssuppkey, d_iatt7_snationk, d_jht12);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_supplier148 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_supplier1! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_supplier1")
        }
    }

    std::clock_t start_krnl_nation249 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation2<<<gridsize, blocksize>>>(d_iatt11_nnationk, d_iatt12_nname_offset, d_iatt12_nname_char, d_nout_inner4, d_itm_inner4_n_nationkey, d_itm_inner4_n_name);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation249 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation2! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation2")
        }
    }

    std::clock_t start_krnl_nation2350 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation23<<<gridsize, blocksize>>>(d_iatt15_nnationk, d_iatt16_nname_offset, d_iatt16_nname_char, d_nout_inner4, d_itm_inner4_n_nationkey, d_itm_inner4_n_name, d_jht6);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation2350 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation23! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation23")
        }
    }

    std::clock_t start_krnl_customer551 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_customer5<<<gridsize, blocksize>>>(d_iatt19_ccustkey, d_iatt22_cnationk, d_jht6, d_jht8);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_customer551 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_customer5! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_customer5")
        }
    }

    std::clock_t start_krnl_orders752 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_orders7<<<gridsize, blocksize>>>(d_iatt27_oorderke, d_iatt28_ocustkey, d_jht8, d_jht11);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_orders752 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_orders7! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_orders7")
        }
    }

    std::clock_t start_krnl_lineitem953 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_lineitem9<<<gridsize, blocksize>>>(d_iatt36_lorderke, d_iatt38_lsuppkey, d_iatt41_lextende, d_iatt42_ldiscoun, d_iatt46_lshipdat, d_jht11, d_jht12, d_aht15, d_agg1, d_agg2, d_agg3);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem953 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem9! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem9")
        }
    }

    std::clock_t start_krnl_aggregation1554 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation15<<<gridsize, blocksize>>>(d_aht15, d_agg1, d_agg2, d_agg3, d_nout_result, d_oatt12_nname_offset, d_iatt12_nname_char, d_oatt16_nname_offset, d_iatt16_nname_char, d_oatt53_lyear, d_oatt1_sumvolum, d_oatt2_avgvolum);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation1554 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation15! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation15")
        }
    }

    std::clock_t stop_totalKernelTime47 = std::clock();
    cudaMemcpy( &nout_inner4, d_nout_inner4, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt12_nname_offset.data(), d_oatt12_nname_offset, 12002 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt16_nname_offset.data(), d_oatt16_nname_offset, 12002 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt53_lyear.data(), d_oatt53_lyear, 12002 * sizeof(unsigned), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt1_sumvolum.data(), d_oatt1_sumvolum, 12002 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt2_avgvolum.data(), d_oatt2_avgvolum, 12002 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    cudaFree( d_iatt4_ssuppkey);
    cudaFree( d_iatt7_snationk);
    cudaFree( d_jht12);
    cudaFree( d_iatt11_nnationk);
    cudaFree( d_iatt12_nname_offset);
    cudaFree( d_iatt12_nname_char);
    cudaFree( d_nout_inner4);
    cudaFree( d_itm_inner4_n_nationkey);
    cudaFree( d_itm_inner4_n_name);
    cudaFree( d_jht6);
    cudaFree( d_iatt19_ccustkey);
    cudaFree( d_iatt22_cnationk);
    cudaFree( d_jht8);
    cudaFree( d_iatt27_oorderke);
    cudaFree( d_iatt28_ocustkey);
    cudaFree( d_jht11);
    cudaFree( d_iatt36_lorderke);
    cudaFree( d_iatt38_lsuppkey);
    cudaFree( d_iatt41_lextende);
    cudaFree( d_iatt42_ldiscoun);
    cudaFree( d_iatt46_lshipdat);
    cudaFree( d_aht15);
    cudaFree( d_agg1);
    cudaFree( d_agg2);
    cudaFree( d_agg3);
    cudaFree( d_nout_result);
    cudaFree( d_oatt12_nname_offset);
    cudaFree( d_oatt16_nname_offset);
    cudaFree( d_oatt53_lyear);
    cudaFree( d_oatt1_sumvolum);
    cudaFree( d_oatt2_avgvolum);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda free! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda free")
        }
    }

    std::clock_t start_finish55 = std::clock();
    printf("\nResult: %i tuples\n", nout_result);
    if((nout_result > 12002)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("n_name: ");
        stringPrint ( iatt12_nname_char, oatt12_nname_offset[pv]);
        printf("  ");
        printf("n_name: ");
        stringPrint ( iatt16_nname_char, oatt16_nname_offset[pv]);
        printf("  ");
        printf("l_year: ");
        printf("%10i", oatt53_lyear[pv]);
        printf("  ");
        printf("sum_volume: ");
        printf("%15.2f", oatt1_sumvolum[pv]);
        printf("  ");
        printf("avg_volume: ");
        printf("%15.2f", oatt2_avgvolum[pv]);
        printf("  ");
        printf("\n");
    }
    if((nout_result > 10)) {
        printf("[...]\n");
    }
    printf("\n");
    std::clock_t stop_finish55 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_supplier1", (stop_krnl_supplier148 - start_krnl_supplier148) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation2", (stop_krnl_nation249 - start_krnl_nation249) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation23", (stop_krnl_nation2350 - start_krnl_nation2350) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_customer5", (stop_krnl_customer551 - start_krnl_customer551) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_orders7", (stop_krnl_orders752 - start_krnl_orders752) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem9", (stop_krnl_lineitem953 - start_krnl_lineitem953) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation15", (stop_krnl_aggregation1554 - start_krnl_aggregation1554) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish55 - start_finish55) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime47 - start_totalKernelTime47) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
