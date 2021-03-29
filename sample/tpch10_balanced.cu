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
struct jpayl6 {
    int att2_nnationk;
    str_t att3_nname;
};
struct jpayl5 {
    int att6_oorderke;
    int att7_ocustkey;
};
struct jpayl9 {
    str_t att3_nname;
    int att6_oorderke;
    int att15_ccustkey;
    str_t att16_cname;
    str_t att17_caddress;
    str_t att19_cphone;
    float att20_cacctbal;
    str_t att22_ccomment;
};
struct apayl11 {
    int att15_ccustkey;
    str_t att16_cname;
    float att20_cacctbal;
    str_t att19_cphone;
    str_t att3_nname;
    str_t att17_caddress;
    str_t att22_ccomment;
};

__global__ void krnl_nation1(
    int* iatt2_nnationk, size_t* iatt3_nname_offset, char* iatt3_nname_char, unique_ht<jpayl6>* jht6) {
    int att2_nnationk;
    str_t att3_nname;

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
            att2_nnationk = iatt2_nnationk[tid_nation1];
            att3_nname = stringScan ( iatt3_nname_offset, iatt3_nname_char, tid_nation1);
        }
        // -------- hash join build (opId: 6) --------
        if(active) {
            jpayl6 payl6;
            payl6.att2_nnationk = att2_nnationk;
            payl6.att3_nname = att3_nname;
            uint64_t hash6;
            hash6 = 0;
            if(active) {
                hash6 = hash ( (hash6 + ((uint64_t)att2_nnationk)));
            }
            hashBuildUnique ( jht6, 50, hash6, &(payl6));
        }
        loopVar += step;
    }

}

__global__ void krnl_orders2(
    int* iatt6_oorderke, int* iatt7_ocustkey, unsigned* iatt10_oorderda, multi_ht* jht5, jpayl5* jht5_payload) {
    int att6_oorderke;
    int att7_ocustkey;
    unsigned att10_oorderda;

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
            att6_oorderke = iatt6_oorderke[tid_orders1];
            att7_ocustkey = iatt7_ocustkey[tid_orders1];
            att10_oorderda = iatt10_oorderda[tid_orders1];
        }
        // -------- selection (opId: 3) --------
        if(active) {
            active = ((att10_oorderda >= 19931001) && (att10_oorderda < 19940101));
        }
        // -------- hash join build (opId: 5) --------
        if(active) {
            uint64_t hash5 = 0;
            if(active) {
                hash5 = 0;
                if(active) {
                    hash5 = hash ( (hash5 + ((uint64_t)att7_ocustkey)));
                }
            }
            hashCountMulti ( jht5, 150000, hash5);
        }
        loopVar += step;
    }

}

__global__ void krnl_orders2_ins(
    int* iatt6_oorderke, int* iatt7_ocustkey, unsigned* iatt10_oorderda, multi_ht* jht5, jpayl5* jht5_payload, int* offs5) {
    int att6_oorderke;
    int att7_ocustkey;
    unsigned att10_oorderda;

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
            att6_oorderke = iatt6_oorderke[tid_orders1];
            att7_ocustkey = iatt7_ocustkey[tid_orders1];
            att10_oorderda = iatt10_oorderda[tid_orders1];
        }
        // -------- selection (opId: 3) --------
        if(active) {
            active = ((att10_oorderda >= 19931001) && (att10_oorderda < 19940101));
        }
        // -------- hash join build (opId: 5) --------
        if(active) {
            uint64_t hash5 = 0;
            if(active) {
                hash5 = 0;
                if(active) {
                    hash5 = hash ( (hash5 + ((uint64_t)att7_ocustkey)));
                }
            }
            jpayl5 payl;
            payl.att6_oorderke = att6_oorderke;
            payl.att7_ocustkey = att7_ocustkey;
            hashInsertMulti ( jht5, jht5_payload, offs5, 150000, hash5, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_customer4(
    int* iatt15_ccustkey, size_t* iatt16_cname_offset, char* iatt16_cname_char, size_t* iatt17_caddress_offset, char* iatt17_caddress_char, int* iatt18_cnationk, size_t* iatt19_cphone_offset, char* iatt19_cphone_char, float* iatt20_cacctbal, size_t* iatt22_ccomment_offset, char* iatt22_ccomment_char, multi_ht* jht5, jpayl5* jht5_payload, unique_ht<jpayl6>* jht6, unique_ht<jpayl9>* jht9) {
    int att15_ccustkey;
    str_t att16_cname;
    str_t att17_caddress;
    int att18_cnationk;
    str_t att19_cphone;
    float att20_cacctbal;
    str_t att22_ccomment;
    int att6_oorderke;
    int att7_ocustkey;
    int att2_nnationk;
    str_t att3_nname;

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
            att15_ccustkey = iatt15_ccustkey[tid_customer1];
            att16_cname = stringScan ( iatt16_cname_offset, iatt16_cname_char, tid_customer1);
            att17_caddress = stringScan ( iatt17_caddress_offset, iatt17_caddress_char, tid_customer1);
            att18_cnationk = iatt18_cnationk[tid_customer1];
            att19_cphone = stringScan ( iatt19_cphone_offset, iatt19_cphone_char, tid_customer1);
            att20_cacctbal = iatt20_cacctbal[tid_customer1];
            att22_ccomment = stringScan ( iatt22_ccomment_offset, iatt22_ccomment_char, tid_customer1);
        }
        // -------- hash join probe (opId: 5) --------
        int matchEnd5 = 0;
        int matchOffset5 = 0;
        int matchStep5 = 1;
        int matchFound5 = 0;
        int probeActive5 = active;
        uint64_t hash5 = 0;
        if(probeActive5) {
            hash5 = 0;
            if(active) {
                hash5 = hash ( (hash5 + ((uint64_t)att15_ccustkey)));
            }
            probeActive5 = hashProbeMulti ( jht5, 150000, hash5, matchOffset5, matchEnd5);
        }
        active = probeActive5;
        while(__any_sync(ALL_LANES,active)) {
            probeActive5 = active;
            jpayl5 payl;
            if(probeActive5) {
                payl = jht5_payload[matchOffset5];
                att6_oorderke = payl.att6_oorderke;
                att7_ocustkey = payl.att7_ocustkey;
                active &= ((att7_ocustkey == att15_ccustkey));
                matchFound5 += active;
            }
            // -------- hash join probe (opId: 6) --------
            uint64_t hash6 = 0;
            if(active) {
                hash6 = 0;
                if(active) {
                    hash6 = hash ( (hash6 + ((uint64_t)att18_cnationk)));
                }
            }
            jpayl6* probepayl6;
            int numLookups6 = 0;
            if(active) {
                active = hashProbeUnique ( jht6, 50, hash6, numLookups6, &(probepayl6));
            }
            int bucketFound6 = 0;
            int probeActive6 = active;
            while((probeActive6 && !(bucketFound6))) {
                jpayl6 jprobepayl6 = *(probepayl6);
                att2_nnationk = jprobepayl6.att2_nnationk;
                att3_nname = jprobepayl6.att3_nname;
                bucketFound6 = 1;
                bucketFound6 &= ((att2_nnationk == att18_cnationk));
                if(!(bucketFound6)) {
                    probeActive6 = hashProbeUnique ( jht6, 50, hash6, numLookups6, &(probepayl6));
                }
            }
            active = bucketFound6;
            // -------- hash join build (opId: 9) --------
            if(active) {
                jpayl9 payl9;
                payl9.att3_nname = att3_nname;
                payl9.att6_oorderke = att6_oorderke;
                payl9.att15_ccustkey = att15_ccustkey;
                payl9.att16_cname = att16_cname;
                payl9.att17_caddress = att17_caddress;
                payl9.att19_cphone = att19_cphone;
                payl9.att20_cacctbal = att20_cacctbal;
                payl9.att22_ccomment = att22_ccomment;
                uint64_t hash9;
                hash9 = 0;
                if(active) {
                    hash9 = hash ( (hash9 + ((uint64_t)att6_oorderke)));
                }
                hashBuildUnique ( jht9, 300000, hash9, &(payl9));
            }
            matchOffset5 += matchStep5;
            probeActive5 &= ((matchOffset5 < matchEnd5));
            active = probeActive5;
        }
        loopVar += step;
    }

}

__global__ void krnl_lineitem7(
    int* iatt23_lorderke, float* iatt28_lextende, float* iatt29_ldiscoun, char* iatt31_lreturnf, unique_ht<jpayl9>* jht9, agg_ht<apayl11>* aht11, float* agg1) {
    int att23_lorderke;
    float att28_lextende;
    float att29_ldiscoun;
    char att31_lreturnf;
    str_t att3_nname;
    int att6_oorderke;
    int att15_ccustkey;
    str_t att16_cname;
    str_t att17_caddress;
    str_t att19_cphone;
    float att20_cacctbal;
    str_t att22_ccomment;
    int buffercount100009_ = 0;
    unsigned warpid = (threadIdx.x / 32);
    int bufferBase100009_ = (warpid * 32);
    // shared memory variables for divergence buffers
    __shared__ str_t att3_nname_dvgnce_buf_100009_[32];
    __shared__ int att15_ccustkey_dvgnce_buf_100009_[32];
    __shared__ str_t att16_cname_dvgnce_buf_100009_[32];
    __shared__ str_t att17_caddress_dvgnce_buf_100009_[32];
    __shared__ str_t att19_cphone_dvgnce_buf_100009_[32];
    __shared__ float att20_cacctbal_dvgnce_buf_100009_[32];
    __shared__ str_t att22_ccomment_dvgnce_buf_100009_[32];
    __shared__ float att28_lextende_dvgnce_buf_100009_[32];
    __shared__ float att29_ldiscoun_dvgnce_buf_100009_[32];
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));
    float att39_rev;

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
            att23_lorderke = iatt23_lorderke[tid_lineitem1];
            att28_lextende = iatt28_lextende[tid_lineitem1];
            att29_ldiscoun = iatt29_ldiscoun[tid_lineitem1];
            att31_lreturnf = iatt31_lreturnf[tid_lineitem1];
        }
        // -------- selection (opId: 8) --------
        if(active) {
            active = (att31_lreturnf == 'R');
        }
        // -------- hash join probe (opId: 9) --------
        uint64_t hash9 = 0;
        if(active) {
            hash9 = 0;
            if(active) {
                hash9 = hash ( (hash9 + ((uint64_t)att23_lorderke)));
            }
        }
        jpayl9* probepayl9;
        int numLookups9 = 0;
        if(active) {
            active = hashProbeUnique ( jht9, 300000, hash9, numLookups9, &(probepayl9));
        }
        int bucketFound9 = 0;
        int probeActive9 = active;
        while((probeActive9 && !(bucketFound9))) {
            jpayl9 jprobepayl9 = *(probepayl9);
            att3_nname = jprobepayl9.att3_nname;
            att6_oorderke = jprobepayl9.att6_oorderke;
            att15_ccustkey = jprobepayl9.att15_ccustkey;
            att16_cname = jprobepayl9.att16_cname;
            att17_caddress = jprobepayl9.att17_caddress;
            att19_cphone = jprobepayl9.att19_cphone;
            att20_cacctbal = jprobepayl9.att20_cacctbal;
            att22_ccomment = jprobepayl9.att22_ccomment;
            bucketFound9 = 1;
            bucketFound9 &= ((att6_oorderke == att23_lorderke));
            if(!(bucketFound9)) {
                probeActive9 = hashProbeUnique ( jht9, 300000, hash9, numLookups9, &(probepayl9));
            }
        }
        active = bucketFound9;
        // -------- divergence buffer (opId: 100009) --------
        // ensures that the thread activity in each warp (32 threads) lies above a given threshold
        // depending on the buffer count inactive lanes are either refilled or flushed to the buffer
        int activemask100009_ = __ballot_sync(ALL_LANES,active);
        int numactive100009_ = __popc(activemask100009_);
        int scan100009_;
        int remaining100009_;
        int bufIdx100009_;
        int minTuplesInFlight100009_ = (flushPipeline) ? (0) : (28);
        while(((buffercount100009_ + numactive100009_) > minTuplesInFlight100009_)) {
            // refill inactive lanes from shared memory buffer
            if(((numactive100009_ < 28) && buffercount100009_)) {
                remaining100009_ = max(((buffercount100009_ + numactive100009_) - 32), 0);
                // prefix scan of inactive lanes
                scan100009_ = __popc((~(activemask100009_) & prefixlanes));
                // gather buffered data (tids, datastructure state, computed values)
                if((!(active) && (scan100009_ < buffercount100009_))) {
                    bufIdx100009_ = (remaining100009_ + (scan100009_ + bufferBase100009_));
                    att3_nname = att3_nname_dvgnce_buf_100009_[bufIdx100009_];
                    att15_ccustkey = att15_ccustkey_dvgnce_buf_100009_[bufIdx100009_];
                    att16_cname = att16_cname_dvgnce_buf_100009_[bufIdx100009_];
                    att17_caddress = att17_caddress_dvgnce_buf_100009_[bufIdx100009_];
                    att19_cphone = att19_cphone_dvgnce_buf_100009_[bufIdx100009_];
                    att20_cacctbal = att20_cacctbal_dvgnce_buf_100009_[bufIdx100009_];
                    att22_ccomment = att22_ccomment_dvgnce_buf_100009_[bufIdx100009_];
                    att28_lextende = att28_lextende_dvgnce_buf_100009_[bufIdx100009_];
                    att29_ldiscoun = att29_ldiscoun_dvgnce_buf_100009_[bufIdx100009_];
                    active = 1;
                }
                // decrement buffer count
                buffercount100009_ = remaining100009_;
            }
            // -------- map (opId: 10) --------
            if(active) {
                att39_rev = (att28_lextende * ((float)1.0f - att29_ldiscoun));
            }
            // -------- aggregation (opId: 11) --------
            int bucket = 0;
            if(active) {
                uint64_t hash11 = 0;
                hash11 = 0;
                if(active) {
                    hash11 = hash ( (hash11 + ((uint64_t)att15_ccustkey)));
                }
                hash11 = hash ( (hash11 + stringHash ( att16_cname)));
                if(active) {
                    hash11 = hash ( (hash11 + ((uint64_t)att20_cacctbal)));
                }
                hash11 = hash ( (hash11 + stringHash ( att19_cphone)));
                hash11 = hash ( (hash11 + stringHash ( att3_nname)));
                hash11 = hash ( (hash11 + stringHash ( att17_caddress)));
                hash11 = hash ( (hash11 + stringHash ( att22_ccomment)));
                apayl11 payl;
                payl.att15_ccustkey = att15_ccustkey;
                payl.att16_cname = att16_cname;
                payl.att20_cacctbal = att20_cacctbal;
                payl.att19_cphone = att19_cphone;
                payl.att3_nname = att3_nname;
                payl.att17_caddress = att17_caddress;
                payl.att22_ccomment = att22_ccomment;
                int bucketFound = 0;
                int numLookups = 0;
                while(!(bucketFound)) {
                    bucket = hashAggregateGetBucket ( aht11, 138748, hash11, numLookups, &(payl));
                    apayl11 probepayl = aht11[bucket].payload;
                    bucketFound = 1;
                    bucketFound &= ((payl.att15_ccustkey == probepayl.att15_ccustkey));
                    bucketFound &= (stringEquals ( payl.att16_cname, probepayl.att16_cname));
                    bucketFound &= ((payl.att20_cacctbal == probepayl.att20_cacctbal));
                    bucketFound &= (stringEquals ( payl.att19_cphone, probepayl.att19_cphone));
                    bucketFound &= (stringEquals ( payl.att3_nname, probepayl.att3_nname));
                    bucketFound &= (stringEquals ( payl.att17_caddress, probepayl.att17_caddress));
                    bucketFound &= (stringEquals ( payl.att22_ccomment, probepayl.att22_ccomment));
                }
            }
            if(active) {
                atomicAdd(&(agg1[bucket]), ((float)att39_rev));
            }
            active = 0;
            activemask100009_ = __ballot_sync(ALL_LANES,active);
            numactive100009_ = __popc(activemask100009_);
        }
        // flush to divergence buffer
        if((numactive100009_ > 0)) {
            // warp prefix scan of remaining active lanes
            scan100009_ = (__popc((activemask100009_ & prefixlanes)) + buffercount100009_);
            // write to buffer
            bufIdx100009_ = (bufferBase100009_ + scan100009_);
            if(active) {
                att3_nname_dvgnce_buf_100009_[bufIdx100009_] = att3_nname;
                att15_ccustkey_dvgnce_buf_100009_[bufIdx100009_] = att15_ccustkey;
                att16_cname_dvgnce_buf_100009_[bufIdx100009_] = att16_cname;
                att17_caddress_dvgnce_buf_100009_[bufIdx100009_] = att17_caddress;
                att19_cphone_dvgnce_buf_100009_[bufIdx100009_] = att19_cphone;
                att20_cacctbal_dvgnce_buf_100009_[bufIdx100009_] = att20_cacctbal;
                att22_ccomment_dvgnce_buf_100009_[bufIdx100009_] = att22_ccomment;
                att28_lextende_dvgnce_buf_100009_[bufIdx100009_] = att28_lextende;
                att29_ldiscoun_dvgnce_buf_100009_[bufIdx100009_] = att29_ldiscoun;
            }
            __syncwarp();
            buffercount100009_ += numactive100009_;
            active = 0;
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation11(
    agg_ht<apayl11>* aht11, float* agg1, int* nout_result, int* oatt15_ccustkey, str_offs* oatt16_cname_offset, char* iatt16_cname_char, float* oatt20_cacctbal, str_offs* oatt19_cphone_offset, char* iatt19_cphone_char, str_offs* oatt3_nname_offset, char* iatt3_nname_char, str_offs* oatt17_caddress_offset, char* iatt17_caddress_char, str_offs* oatt22_ccomment_offset, char* iatt22_ccomment_char, float* oatt1_revenue) {
    int att15_ccustkey;
    str_t att16_cname;
    float att20_cacctbal;
    str_t att19_cphone;
    str_t att3_nname;
    str_t att17_caddress;
    str_t att22_ccomment;
    float att1_revenue;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_aggregation11 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation11 = loopVar;
        active = (loopVar < 138748);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 11) --------
        if(active) {
            active &= ((aht11[tid_aggregation11].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl11 payl = aht11[tid_aggregation11].payload;
            att15_ccustkey = payl.att15_ccustkey;
            att16_cname = payl.att16_cname;
            att20_cacctbal = payl.att20_cacctbal;
            att19_cphone = payl.att19_cphone;
            att3_nname = payl.att3_nname;
            att17_caddress = payl.att17_caddress;
            att22_ccomment = payl.att22_ccomment;
        }
        if(active) {
            att1_revenue = agg1[tid_aggregation11];
        }
        // -------- materialize (opId: 12) --------
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
            oatt15_ccustkey[wp] = att15_ccustkey;
            oatt16_cname_offset[wp] = toStringOffset ( iatt16_cname_char, att16_cname);
            oatt20_cacctbal[wp] = att20_cacctbal;
            oatt19_cphone_offset[wp] = toStringOffset ( iatt19_cphone_char, att19_cphone);
            oatt3_nname_offset[wp] = toStringOffset ( iatt3_nname_char, att3_nname);
            oatt17_caddress_offset[wp] = toStringOffset ( iatt17_caddress_char, att17_caddress);
            oatt22_ccomment_offset[wp] = toStringOffset ( iatt22_ccomment_char, att22_ccomment);
            oatt1_revenue[wp] = att1_revenue;
        }
        loopVar += step;
    }

}

int main() {
    int* iatt2_nnationk;
    iatt2_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    size_t* iatt3_nname_offset;
    iatt3_nname_offset = ( size_t*) map_memory_file ( "mmdb/nation_n_name_offset" );
    char* iatt3_nname_char;
    iatt3_nname_char = ( char*) map_memory_file ( "mmdb/nation_n_name_char" );
    int* iatt6_oorderke;
    iatt6_oorderke = ( int*) map_memory_file ( "mmdb/orders_o_orderkey" );
    int* iatt7_ocustkey;
    iatt7_ocustkey = ( int*) map_memory_file ( "mmdb/orders_o_custkey" );
    unsigned* iatt10_oorderda;
    iatt10_oorderda = ( unsigned*) map_memory_file ( "mmdb/orders_o_orderdate" );
    int* iatt15_ccustkey;
    iatt15_ccustkey = ( int*) map_memory_file ( "mmdb/customer_c_custkey" );
    size_t* iatt16_cname_offset;
    iatt16_cname_offset = ( size_t*) map_memory_file ( "mmdb/customer_c_name_offset" );
    char* iatt16_cname_char;
    iatt16_cname_char = ( char*) map_memory_file ( "mmdb/customer_c_name_char" );
    size_t* iatt17_caddress_offset;
    iatt17_caddress_offset = ( size_t*) map_memory_file ( "mmdb/customer_c_address_offset" );
    char* iatt17_caddress_char;
    iatt17_caddress_char = ( char*) map_memory_file ( "mmdb/customer_c_address_char" );
    int* iatt18_cnationk;
    iatt18_cnationk = ( int*) map_memory_file ( "mmdb/customer_c_nationkey" );
    size_t* iatt19_cphone_offset;
    iatt19_cphone_offset = ( size_t*) map_memory_file ( "mmdb/customer_c_phone_offset" );
    char* iatt19_cphone_char;
    iatt19_cphone_char = ( char*) map_memory_file ( "mmdb/customer_c_phone_char" );
    float* iatt20_cacctbal;
    iatt20_cacctbal = ( float*) map_memory_file ( "mmdb/customer_c_acctbal" );
    size_t* iatt22_ccomment_offset;
    iatt22_ccomment_offset = ( size_t*) map_memory_file ( "mmdb/customer_c_comment_offset" );
    char* iatt22_ccomment_char;
    iatt22_ccomment_char = ( char*) map_memory_file ( "mmdb/customer_c_comment_char" );
    int* iatt23_lorderke;
    iatt23_lorderke = ( int*) map_memory_file ( "mmdb/lineitem_l_orderkey" );
    float* iatt28_lextende;
    iatt28_lextende = ( float*) map_memory_file ( "mmdb/lineitem_l_extendedprice" );
    float* iatt29_ldiscoun;
    iatt29_ldiscoun = ( float*) map_memory_file ( "mmdb/lineitem_l_discount" );
    char* iatt31_lreturnf;
    iatt31_lreturnf = ( char*) map_memory_file ( "mmdb/lineitem_l_returnflag" );

    int nout_result;
    std::vector < int > oatt15_ccustkey(69374);
    std::vector < str_offs > oatt16_cname_offset(69374);
    std::vector < float > oatt20_cacctbal(69374);
    std::vector < str_offs > oatt19_cphone_offset(69374);
    std::vector < str_offs > oatt3_nname_offset(69374);
    std::vector < str_offs > oatt17_caddress_offset(69374);
    std::vector < str_offs > oatt22_ccomment_offset(69374);
    std::vector < float > oatt1_revenue(69374);

    // wake up gpu
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in wake up gpu! " << cudaGetErrorString( err ) << std::endl;
            ERROR("wake up gpu")
        }
    }

    int* d_iatt2_nnationk;
    cudaMalloc((void**) &d_iatt2_nnationk, 25* sizeof(int) );
    size_t* d_iatt3_nname_offset;
    cudaMalloc((void**) &d_iatt3_nname_offset, (25 + 1)* sizeof(size_t) );
    char* d_iatt3_nname_char;
    cudaMalloc((void**) &d_iatt3_nname_char, 186* sizeof(char) );
    int* d_iatt6_oorderke;
    cudaMalloc((void**) &d_iatt6_oorderke, 1500000* sizeof(int) );
    int* d_iatt7_ocustkey;
    cudaMalloc((void**) &d_iatt7_ocustkey, 1500000* sizeof(int) );
    unsigned* d_iatt10_oorderda;
    cudaMalloc((void**) &d_iatt10_oorderda, 1500000* sizeof(unsigned) );
    int* d_iatt15_ccustkey;
    cudaMalloc((void**) &d_iatt15_ccustkey, 150000* sizeof(int) );
    size_t* d_iatt16_cname_offset;
    cudaMalloc((void**) &d_iatt16_cname_offset, (150000 + 1)* sizeof(size_t) );
    char* d_iatt16_cname_char;
    cudaMalloc((void**) &d_iatt16_cname_char, 2700009* sizeof(char) );
    size_t* d_iatt17_caddress_offset;
    cudaMalloc((void**) &d_iatt17_caddress_offset, (150000 + 1)* sizeof(size_t) );
    char* d_iatt17_caddress_char;
    cudaMalloc((void**) &d_iatt17_caddress_char, 3753296* sizeof(char) );
    int* d_iatt18_cnationk;
    cudaMalloc((void**) &d_iatt18_cnationk, 150000* sizeof(int) );
    size_t* d_iatt19_cphone_offset;
    cudaMalloc((void**) &d_iatt19_cphone_offset, (150000 + 1)* sizeof(size_t) );
    char* d_iatt19_cphone_char;
    cudaMalloc((void**) &d_iatt19_cphone_char, 2250009* sizeof(char) );
    float* d_iatt20_cacctbal;
    cudaMalloc((void**) &d_iatt20_cacctbal, 150000* sizeof(float) );
    size_t* d_iatt22_ccomment_offset;
    cudaMalloc((void**) &d_iatt22_ccomment_offset, (150000 + 1)* sizeof(size_t) );
    char* d_iatt22_ccomment_char;
    cudaMalloc((void**) &d_iatt22_ccomment_char, 10836339* sizeof(char) );
    int* d_iatt23_lorderke;
    cudaMalloc((void**) &d_iatt23_lorderke, 6001215* sizeof(int) );
    float* d_iatt28_lextende;
    cudaMalloc((void**) &d_iatt28_lextende, 6001215* sizeof(float) );
    float* d_iatt29_ldiscoun;
    cudaMalloc((void**) &d_iatt29_ldiscoun, 6001215* sizeof(float) );
    char* d_iatt31_lreturnf;
    cudaMalloc((void**) &d_iatt31_lreturnf, 6001215* sizeof(char) );
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    int* d_oatt15_ccustkey;
    cudaMalloc((void**) &d_oatt15_ccustkey, 69374* sizeof(int) );
    str_offs* d_oatt16_cname_offset;
    cudaMalloc((void**) &d_oatt16_cname_offset, 69374* sizeof(str_offs) );
    float* d_oatt20_cacctbal;
    cudaMalloc((void**) &d_oatt20_cacctbal, 69374* sizeof(float) );
    str_offs* d_oatt19_cphone_offset;
    cudaMalloc((void**) &d_oatt19_cphone_offset, 69374* sizeof(str_offs) );
    str_offs* d_oatt3_nname_offset;
    cudaMalloc((void**) &d_oatt3_nname_offset, 69374* sizeof(str_offs) );
    str_offs* d_oatt17_caddress_offset;
    cudaMalloc((void**) &d_oatt17_caddress_offset, 69374* sizeof(str_offs) );
    str_offs* d_oatt22_ccomment_offset;
    cudaMalloc((void**) &d_oatt22_ccomment_offset, 69374* sizeof(str_offs) );
    float* d_oatt1_revenue;
    cudaMalloc((void**) &d_oatt1_revenue, 69374* sizeof(float) );
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

    unique_ht<jpayl6>* d_jht6;
    cudaMalloc((void**) &d_jht6, 50* sizeof(unique_ht<jpayl6>) );
    {
        int gridsize=100;
        int blocksize=32;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht6, 50);
    }
    multi_ht* d_jht5;
    cudaMalloc((void**) &d_jht5, 150000* sizeof(multi_ht) );
    jpayl5* d_jht5_payload;
    cudaMalloc((void**) &d_jht5_payload, 150000* sizeof(jpayl5) );
    {
        int gridsize=100;
        int blocksize=32;
        initMultiHT<<<gridsize, blocksize>>>(d_jht5, 150000);
    }
    int* d_offs5;
    cudaMalloc((void**) &d_offs5, 1* sizeof(int) );
    {
        int gridsize=100;
        int blocksize=32;
        initArray<<<gridsize, blocksize>>>(d_offs5, 0, 1);
    }
    unique_ht<jpayl9>* d_jht9;
    cudaMalloc((void**) &d_jht9, 300000* sizeof(unique_ht<jpayl9>) );
    {
        int gridsize=100;
        int blocksize=32;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht9, 300000);
    }
    agg_ht<apayl11>* d_aht11;
    cudaMalloc((void**) &d_aht11, 138748* sizeof(agg_ht<apayl11>) );
    {
        int gridsize=100;
        int blocksize=32;
        initAggHT<<<gridsize, blocksize>>>(d_aht11, 138748);
    }
    float* d_agg1;
    cudaMalloc((void**) &d_agg1, 138748* sizeof(float) );
    {
        int gridsize=100;
        int blocksize=32;
        initArray<<<gridsize, blocksize>>>(d_agg1, 0.0f, 138748);
    }
    {
        int gridsize=100;
        int blocksize=32;
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

    cudaMemcpy( d_iatt2_nnationk, iatt2_nnationk, 25 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt3_nname_offset, iatt3_nname_offset, (25 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt3_nname_char, iatt3_nname_char, 186 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt6_oorderke, iatt6_oorderke, 1500000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt7_ocustkey, iatt7_ocustkey, 1500000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt10_oorderda, iatt10_oorderda, 1500000 * sizeof(unsigned), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt15_ccustkey, iatt15_ccustkey, 150000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt16_cname_offset, iatt16_cname_offset, (150000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt16_cname_char, iatt16_cname_char, 2700009 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt17_caddress_offset, iatt17_caddress_offset, (150000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt17_caddress_char, iatt17_caddress_char, 3753296 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt18_cnationk, iatt18_cnationk, 150000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt19_cphone_offset, iatt19_cphone_offset, (150000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt19_cphone_char, iatt19_cphone_char, 2250009 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt20_cacctbal, iatt20_cacctbal, 150000 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt22_ccomment_offset, iatt22_ccomment_offset, (150000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt22_ccomment_char, iatt22_ccomment_char, 10836339 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt23_lorderke, iatt23_lorderke, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt28_lextende, iatt28_lextende, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt29_ldiscoun, iatt29_ldiscoun, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt31_lreturnf, iatt31_lreturnf, 6001215 * sizeof(char), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime0 = std::clock();
    std::clock_t start_krnl_nation11 = std::clock();
    {
        int gridsize=100;
        int blocksize=32;
        krnl_nation1<<<gridsize, blocksize>>>(d_iatt2_nnationk, d_iatt3_nname_offset, d_iatt3_nname_char, d_jht6);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation11 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation1! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation1")
        }
    }

    std::clock_t start_krnl_orders22 = std::clock();
    {
        int gridsize=100;
        int blocksize=32;
        krnl_orders2<<<gridsize, blocksize>>>(d_iatt6_oorderke, d_iatt7_ocustkey, d_iatt10_oorderda, d_jht5, d_jht5_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_orders22 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_orders2! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_orders2")
        }
    }

    std::clock_t start_scanMultiHT3 = std::clock();
    {
        int gridsize=100;
        int blocksize=32;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht5, 150000, d_offs5);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT3 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_orders2_ins4 = std::clock();
    {
        int gridsize=100;
        int blocksize=32;
        krnl_orders2_ins<<<gridsize, blocksize>>>(d_iatt6_oorderke, d_iatt7_ocustkey, d_iatt10_oorderda, d_jht5, d_jht5_payload, d_offs5);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_orders2_ins4 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_orders2_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_orders2_ins")
        }
    }

    std::clock_t start_krnl_customer45 = std::clock();
    {
        int gridsize=100;
        int blocksize=32;
        krnl_customer4<<<gridsize, blocksize>>>(d_iatt15_ccustkey, d_iatt16_cname_offset, d_iatt16_cname_char, d_iatt17_caddress_offset, d_iatt17_caddress_char, d_iatt18_cnationk, d_iatt19_cphone_offset, d_iatt19_cphone_char, d_iatt20_cacctbal, d_iatt22_ccomment_offset, d_iatt22_ccomment_char, d_jht5, d_jht5_payload, d_jht6, d_jht9);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_customer45 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_customer4! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_customer4")
        }
    }

    std::clock_t start_krnl_lineitem76 = std::clock();
    {
        int gridsize=100;
        int blocksize=32;
        krnl_lineitem7<<<gridsize, blocksize>>>(d_iatt23_lorderke, d_iatt28_lextende, d_iatt29_ldiscoun, d_iatt31_lreturnf, d_jht9, d_aht11, d_agg1);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem76 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem7! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem7")
        }
    }

    std::clock_t start_krnl_aggregation117 = std::clock();
    {
        int gridsize=100;
        int blocksize=32;
        krnl_aggregation11<<<gridsize, blocksize>>>(d_aht11, d_agg1, d_nout_result, d_oatt15_ccustkey, d_oatt16_cname_offset, d_iatt16_cname_char, d_oatt20_cacctbal, d_oatt19_cphone_offset, d_iatt19_cphone_char, d_oatt3_nname_offset, d_iatt3_nname_char, d_oatt17_caddress_offset, d_iatt17_caddress_char, d_oatt22_ccomment_offset, d_iatt22_ccomment_char, d_oatt1_revenue);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation117 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation11! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation11")
        }
    }

    std::clock_t stop_totalKernelTime0 = std::clock();
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt15_ccustkey.data(), d_oatt15_ccustkey, 69374 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt16_cname_offset.data(), d_oatt16_cname_offset, 69374 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt20_cacctbal.data(), d_oatt20_cacctbal, 69374 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt19_cphone_offset.data(), d_oatt19_cphone_offset, 69374 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt3_nname_offset.data(), d_oatt3_nname_offset, 69374 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt17_caddress_offset.data(), d_oatt17_caddress_offset, 69374 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt22_ccomment_offset.data(), d_oatt22_ccomment_offset, 69374 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt1_revenue.data(), d_oatt1_revenue, 69374 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    cudaFree( d_iatt2_nnationk);
    cudaFree( d_iatt3_nname_offset);
    cudaFree( d_iatt3_nname_char);
    cudaFree( d_jht6);
    cudaFree( d_iatt6_oorderke);
    cudaFree( d_iatt7_ocustkey);
    cudaFree( d_iatt10_oorderda);
    cudaFree( d_jht5);
    cudaFree( d_jht5_payload);
    cudaFree( d_offs5);
    cudaFree( d_iatt15_ccustkey);
    cudaFree( d_iatt16_cname_offset);
    cudaFree( d_iatt16_cname_char);
    cudaFree( d_iatt17_caddress_offset);
    cudaFree( d_iatt17_caddress_char);
    cudaFree( d_iatt18_cnationk);
    cudaFree( d_iatt19_cphone_offset);
    cudaFree( d_iatt19_cphone_char);
    cudaFree( d_iatt20_cacctbal);
    cudaFree( d_iatt22_ccomment_offset);
    cudaFree( d_iatt22_ccomment_char);
    cudaFree( d_jht9);
    cudaFree( d_iatt23_lorderke);
    cudaFree( d_iatt28_lextende);
    cudaFree( d_iatt29_ldiscoun);
    cudaFree( d_iatt31_lreturnf);
    cudaFree( d_aht11);
    cudaFree( d_agg1);
    cudaFree( d_nout_result);
    cudaFree( d_oatt15_ccustkey);
    cudaFree( d_oatt16_cname_offset);
    cudaFree( d_oatt20_cacctbal);
    cudaFree( d_oatt19_cphone_offset);
    cudaFree( d_oatt3_nname_offset);
    cudaFree( d_oatt17_caddress_offset);
    cudaFree( d_oatt22_ccomment_offset);
    cudaFree( d_oatt1_revenue);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda free! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda free")
        }
    }

    std::clock_t start_finish8 = std::clock();
    printf("\nResult: %i tuples\n", nout_result);
    if((nout_result > 69374)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("c_custkey: ");
        printf("%8i", oatt15_ccustkey[pv]);
        printf("  ");
        printf("c_name: ");
        stringPrint ( iatt16_cname_char, oatt16_cname_offset[pv]);
        printf("  ");
        printf("c_acctbal: ");
        printf("%15.2f", oatt20_cacctbal[pv]);
        printf("  ");
        printf("c_phone: ");
        stringPrint ( iatt19_cphone_char, oatt19_cphone_offset[pv]);
        printf("  ");
        printf("n_name: ");
        stringPrint ( iatt3_nname_char, oatt3_nname_offset[pv]);
        printf("  ");
        printf("c_address: ");
        stringPrint ( iatt17_caddress_char, oatt17_caddress_offset[pv]);
        printf("  ");
        printf("c_comment: ");
        stringPrint ( iatt22_ccomment_char, oatt22_ccomment_offset[pv]);
        printf("  ");
        printf("revenue: ");
        printf("%15.2f", oatt1_revenue[pv]);
        printf("  ");
        printf("\n");
    }
    if((nout_result > 10)) {
        printf("[...]\n");
    }
    printf("\n");
    std::clock_t stop_finish8 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_nation1", (stop_krnl_nation11 - start_krnl_nation11) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_orders2", (stop_krnl_orders22 - start_krnl_orders22) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT3 - start_scanMultiHT3) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_orders2_ins", (stop_krnl_orders2_ins4 - start_krnl_orders2_ins4) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_customer4", (stop_krnl_customer45 - start_krnl_customer45) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem7 LR9", (stop_krnl_lineitem76 - start_krnl_lineitem76) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation11", (stop_krnl_aggregation117 - start_krnl_aggregation117) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish8 - start_finish8) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime0 - start_totalKernelTime0) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
