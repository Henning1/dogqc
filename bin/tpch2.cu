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
struct jpayl4 {
    int att2_rregionk;
};
struct jpayl6 {
    int att5_nnationk;
    str_t att6_nname;
};
struct jpayl25 {
    str_t att6_nname;
    int att9_ssuppkey;
    str_t att10_sname;
    str_t att11_saddress;
    str_t att13_sphone;
    float att14_sacctbal;
    str_t att15_scomment;
};
struct jpayl10 {
    int att16_rregionk;
};
struct jpayl12 {
    int att19_nnationk;
};
struct jpayl17 {
    int att23_ssuppkey;
};
struct jpayl16 {
    int att30_ppartkey;
};
struct apayl18 {
    int att39_pspartke;
};
struct jpayl21 {
    int att39_pspartke;
    float att1_minsuppl;
};
struct jpayl23 {
    int att39_pspartke;
    float att1_minsuppl;
    int att44_ppartkey;
    str_t att46_pmfgr;
};

__global__ void krnl_region1(
    int* iatt2_rregionk, size_t* iatt3_rname_offset, char* iatt3_rname_char, unique_ht<jpayl4>* jht4) {
    int att2_rregionk;
    str_t att3_rname;
    str_t c1 = stringConstant ( "EUROPE", 6);

    int tid_region1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_region1 = loopVar;
        active = (loopVar < 5);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att2_rregionk = iatt2_rregionk[tid_region1];
            att3_rname = stringScan ( iatt3_rname_offset, iatt3_rname_char, tid_region1);
        }
        // -------- selection (opId: 2) --------
        if(active) {
            active = stringEquals ( att3_rname, c1);
        }
        // -------- hash join build (opId: 4) --------
        if(active) {
            jpayl4 payl4;
            payl4.att2_rregionk = att2_rregionk;
            uint64_t hash4;
            hash4 = 0;
            if(active) {
                hash4 = hash ( (hash4 + ((uint64_t)att2_rregionk)));
            }
            hashBuildUnique ( jht4, 10, hash4, &(payl4));
        }
        loopVar += step;
    }

}

__global__ void krnl_nation3(
    int* iatt5_nnationk, size_t* iatt6_nname_offset, char* iatt6_nname_char, int* iatt7_nregionk, unique_ht<jpayl4>* jht4, multi_ht* jht6, jpayl6* jht6_payload) {
    int att5_nnationk;
    str_t att6_nname;
    int att7_nregionk;
    int att2_rregionk;

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
            att5_nnationk = iatt5_nnationk[tid_nation1];
            att6_nname = stringScan ( iatt6_nname_offset, iatt6_nname_char, tid_nation1);
            att7_nregionk = iatt7_nregionk[tid_nation1];
        }
        // -------- hash join probe (opId: 4) --------
        uint64_t hash4 = 0;
        if(active) {
            hash4 = 0;
            if(active) {
                hash4 = hash ( (hash4 + ((uint64_t)att7_nregionk)));
            }
        }
        jpayl4* probepayl4;
        int numLookups4 = 0;
        if(active) {
            active = hashProbeUnique ( jht4, 10, hash4, numLookups4, &(probepayl4));
        }
        int bucketFound4 = 0;
        int probeActive4 = active;
        while((probeActive4 && !(bucketFound4))) {
            jpayl4 jprobepayl4 = *(probepayl4);
            att2_rregionk = jprobepayl4.att2_rregionk;
            bucketFound4 = 1;
            bucketFound4 &= ((att2_rregionk == att7_nregionk));
            if(!(bucketFound4)) {
                probeActive4 = hashProbeUnique ( jht4, 10, hash4, numLookups4, &(probepayl4));
            }
        }
        active = bucketFound4;
        // -------- hash join build (opId: 6) --------
        if(active) {
            uint64_t hash6 = 0;
            if(active) {
                hash6 = 0;
                if(active) {
                    hash6 = hash ( (hash6 + ((uint64_t)att5_nnationk)));
                }
            }
            hashCountMulti ( jht6, 50, hash6);
        }
        loopVar += step;
    }

}

__global__ void krnl_nation3_ins(
    int* iatt5_nnationk, size_t* iatt6_nname_offset, char* iatt6_nname_char, int* iatt7_nregionk, unique_ht<jpayl4>* jht4, multi_ht* jht6, jpayl6* jht6_payload, int* offs6) {
    int att5_nnationk;
    str_t att6_nname;
    int att7_nregionk;
    int att2_rregionk;

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
            att5_nnationk = iatt5_nnationk[tid_nation1];
            att6_nname = stringScan ( iatt6_nname_offset, iatt6_nname_char, tid_nation1);
            att7_nregionk = iatt7_nregionk[tid_nation1];
        }
        // -------- hash join probe (opId: 4) --------
        uint64_t hash4 = 0;
        if(active) {
            hash4 = 0;
            if(active) {
                hash4 = hash ( (hash4 + ((uint64_t)att7_nregionk)));
            }
        }
        jpayl4* probepayl4;
        int numLookups4 = 0;
        if(active) {
            active = hashProbeUnique ( jht4, 10, hash4, numLookups4, &(probepayl4));
        }
        int bucketFound4 = 0;
        int probeActive4 = active;
        while((probeActive4 && !(bucketFound4))) {
            jpayl4 jprobepayl4 = *(probepayl4);
            att2_rregionk = jprobepayl4.att2_rregionk;
            bucketFound4 = 1;
            bucketFound4 &= ((att2_rregionk == att7_nregionk));
            if(!(bucketFound4)) {
                probeActive4 = hashProbeUnique ( jht4, 10, hash4, numLookups4, &(probepayl4));
            }
        }
        active = bucketFound4;
        // -------- hash join build (opId: 6) --------
        if(active) {
            uint64_t hash6 = 0;
            if(active) {
                hash6 = 0;
                if(active) {
                    hash6 = hash ( (hash6 + ((uint64_t)att5_nnationk)));
                }
            }
            jpayl6 payl;
            payl.att5_nnationk = att5_nnationk;
            payl.att6_nname = att6_nname;
            hashInsertMulti ( jht6, jht6_payload, offs6, 50, hash6, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_supplier5(
    int* iatt9_ssuppkey, size_t* iatt10_sname_offset, char* iatt10_sname_char, size_t* iatt11_saddress_offset, char* iatt11_saddress_char, int* iatt12_snationk, size_t* iatt13_sphone_offset, char* iatt13_sphone_char, float* iatt14_sacctbal, size_t* iatt15_scomment_offset, char* iatt15_scomment_char, multi_ht* jht6, jpayl6* jht6_payload, unique_ht<jpayl25>* jht25) {
    int att9_ssuppkey;
    str_t att10_sname;
    str_t att11_saddress;
    int att12_snationk;
    str_t att13_sphone;
    float att14_sacctbal;
    str_t att15_scomment;
    unsigned warplane = (threadIdx.x % 32);
    int att5_nnationk;
    str_t att6_nname;

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
            att9_ssuppkey = iatt9_ssuppkey[tid_supplier1];
            att10_sname = stringScan ( iatt10_sname_offset, iatt10_sname_char, tid_supplier1);
            att11_saddress = stringScan ( iatt11_saddress_offset, iatt11_saddress_char, tid_supplier1);
            att12_snationk = iatt12_snationk[tid_supplier1];
            att13_sphone = stringScan ( iatt13_sphone_offset, iatt13_sphone_char, tid_supplier1);
            att14_sacctbal = iatt14_sacctbal[tid_supplier1];
            att15_scomment = stringScan ( iatt15_scomment_offset, iatt15_scomment_char, tid_supplier1);
        }
        // -------- hash join probe (opId: 6) --------
        // -------- multiprobe multi broadcast (opId: 6) --------
        int matchEnd6 = 0;
        int matchEndBuf6 = 0;
        int matchOffset6 = 0;
        int matchOffsetBuf6 = 0;
        int probeActive6 = active;
        int att9_ssuppkey_bcbuf6;
        str_t att10_sname_bcbuf6;
        str_t att11_saddress_bcbuf6;
        int att12_snationk_bcbuf6;
        str_t att13_sphone_bcbuf6;
        float att14_sacctbal_bcbuf6;
        str_t att15_scomment_bcbuf6;
        uint64_t hash6 = 0;
        if(probeActive6) {
            hash6 = 0;
            if(active) {
                hash6 = hash ( (hash6 + ((uint64_t)att12_snationk)));
            }
            probeActive6 = hashProbeMulti ( jht6, 50, hash6, matchOffsetBuf6, matchEndBuf6);
        }
        unsigned activeProbes6 = __ballot_sync(ALL_LANES,probeActive6);
        int num6 = 0;
        num6 = (matchEndBuf6 - matchOffsetBuf6);
        unsigned wideProbes6 = __ballot_sync(ALL_LANES,(num6 >= 32));
        att9_ssuppkey_bcbuf6 = att9_ssuppkey;
        att10_sname_bcbuf6 = att10_sname;
        att11_saddress_bcbuf6 = att11_saddress;
        att12_snationk_bcbuf6 = att12_snationk;
        att13_sphone_bcbuf6 = att13_sphone;
        att14_sacctbal_bcbuf6 = att14_sacctbal;
        att15_scomment_bcbuf6 = att15_scomment;
        while((activeProbes6 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes6)) {
                if((wideProbes6 > 0)) {
                    tupleLane = (__ffs(wideProbes6) - 1);
                    wideProbes6 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes6) - 1);
                }
                num = __shfl_sync(ALL_LANES,num6,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset6 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes6 -= (1 << tupleLane);
            }
            matchOffset6 += __shfl_sync(ALL_LANES,matchOffsetBuf6,broadcastLane);
            matchEnd6 = __shfl_sync(ALL_LANES,matchEndBuf6,broadcastLane);
            att9_ssuppkey = __shfl_sync(ALL_LANES,att9_ssuppkey_bcbuf6,broadcastLane);
            att10_sname = __shfl_sync(ALL_LANES,att10_sname_bcbuf6,broadcastLane);
            att11_saddress = __shfl_sync(ALL_LANES,att11_saddress_bcbuf6,broadcastLane);
            att12_snationk = __shfl_sync(ALL_LANES,att12_snationk_bcbuf6,broadcastLane);
            att13_sphone = __shfl_sync(ALL_LANES,att13_sphone_bcbuf6,broadcastLane);
            att14_sacctbal = __shfl_sync(ALL_LANES,att14_sacctbal_bcbuf6,broadcastLane);
            att15_scomment = __shfl_sync(ALL_LANES,att15_scomment_bcbuf6,broadcastLane);
            probeActive6 = (matchOffset6 < matchEnd6);
            while(__any_sync(ALL_LANES,probeActive6)) {
                active = probeActive6;
                active = 0;
                jpayl6 payl;
                if(probeActive6) {
                    payl = jht6_payload[matchOffset6];
                    att5_nnationk = payl.att5_nnationk;
                    att6_nname = payl.att6_nname;
                    active = 1;
                    active &= ((att5_nnationk == att12_snationk));
                    matchOffset6 += 32;
                    probeActive6 &= ((matchOffset6 < matchEnd6));
                }
                // -------- hash join build (opId: 25) --------
                if(active) {
                    jpayl25 payl25;
                    payl25.att6_nname = att6_nname;
                    payl25.att9_ssuppkey = att9_ssuppkey;
                    payl25.att10_sname = att10_sname;
                    payl25.att11_saddress = att11_saddress;
                    payl25.att13_sphone = att13_sphone;
                    payl25.att14_sacctbal = att14_sacctbal;
                    payl25.att15_scomment = att15_scomment;
                    uint64_t hash25;
                    hash25 = 0;
                    if(active) {
                        hash25 = hash ( (hash25 + ((uint64_t)att9_ssuppkey)));
                    }
                    hashBuildUnique ( jht25, 20000, hash25, &(payl25));
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_region27(
    int* iatt16_rregionk, size_t* iatt17_rname_offset, char* iatt17_rname_char, unique_ht<jpayl10>* jht10) {
    int att16_rregionk;
    str_t att17_rname;
    str_t c2 = stringConstant ( "EUROPE", 6);

    int tid_region2 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_region2 = loopVar;
        active = (loopVar < 5);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att16_rregionk = iatt16_rregionk[tid_region2];
            att17_rname = stringScan ( iatt17_rname_offset, iatt17_rname_char, tid_region2);
        }
        // -------- selection (opId: 8) --------
        if(active) {
            active = stringEquals ( att17_rname, c2);
        }
        // -------- hash join build (opId: 10) --------
        if(active) {
            jpayl10 payl10;
            payl10.att16_rregionk = att16_rregionk;
            uint64_t hash10;
            hash10 = 0;
            if(active) {
                hash10 = hash ( (hash10 + ((uint64_t)att16_rregionk)));
            }
            hashBuildUnique ( jht10, 10, hash10, &(payl10));
        }
        loopVar += step;
    }

}

__global__ void krnl_nation29(
    int* iatt19_nnationk, int* iatt21_nregionk, unique_ht<jpayl10>* jht10, unique_ht<jpayl12>* jht12) {
    int att19_nnationk;
    int att21_nregionk;
    int att16_rregionk;

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
            att19_nnationk = iatt19_nnationk[tid_nation2];
            att21_nregionk = iatt21_nregionk[tid_nation2];
        }
        // -------- hash join probe (opId: 10) --------
        uint64_t hash10 = 0;
        if(active) {
            hash10 = 0;
            if(active) {
                hash10 = hash ( (hash10 + ((uint64_t)att21_nregionk)));
            }
        }
        jpayl10* probepayl10;
        int numLookups10 = 0;
        if(active) {
            active = hashProbeUnique ( jht10, 10, hash10, numLookups10, &(probepayl10));
        }
        int bucketFound10 = 0;
        int probeActive10 = active;
        while((probeActive10 && !(bucketFound10))) {
            jpayl10 jprobepayl10 = *(probepayl10);
            att16_rregionk = jprobepayl10.att16_rregionk;
            bucketFound10 = 1;
            bucketFound10 &= ((att16_rregionk == att21_nregionk));
            if(!(bucketFound10)) {
                probeActive10 = hashProbeUnique ( jht10, 10, hash10, numLookups10, &(probepayl10));
            }
        }
        active = bucketFound10;
        // -------- hash join build (opId: 12) --------
        if(active) {
            jpayl12 payl12;
            payl12.att19_nnationk = att19_nnationk;
            uint64_t hash12;
            hash12 = 0;
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att19_nnationk)));
            }
            hashBuildUnique ( jht12, 50, hash12, &(payl12));
        }
        loopVar += step;
    }

}

__global__ void krnl_supplier211(
    int* iatt23_ssuppkey, int* iatt26_snationk, unique_ht<jpayl12>* jht12, unique_ht<jpayl17>* jht17) {
    int att23_ssuppkey;
    int att26_snationk;
    int att19_nnationk;

    int tid_supplier2 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_supplier2 = loopVar;
        active = (loopVar < 10000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att23_ssuppkey = iatt23_ssuppkey[tid_supplier2];
            att26_snationk = iatt26_snationk[tid_supplier2];
        }
        // -------- hash join probe (opId: 12) --------
        uint64_t hash12 = 0;
        if(active) {
            hash12 = 0;
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att26_snationk)));
            }
        }
        jpayl12* probepayl12;
        int numLookups12 = 0;
        if(active) {
            active = hashProbeUnique ( jht12, 50, hash12, numLookups12, &(probepayl12));
        }
        int bucketFound12 = 0;
        int probeActive12 = active;
        while((probeActive12 && !(bucketFound12))) {
            jpayl12 jprobepayl12 = *(probepayl12);
            att19_nnationk = jprobepayl12.att19_nnationk;
            bucketFound12 = 1;
            bucketFound12 &= ((att19_nnationk == att26_snationk));
            if(!(bucketFound12)) {
                probeActive12 = hashProbeUnique ( jht12, 50, hash12, numLookups12, &(probepayl12));
            }
        }
        active = bucketFound12;
        // -------- hash join build (opId: 17) --------
        if(active) {
            jpayl17 payl17;
            payl17.att23_ssuppkey = att23_ssuppkey;
            uint64_t hash17;
            hash17 = 0;
            if(active) {
                hash17 = hash ( (hash17 + ((uint64_t)att23_ssuppkey)));
            }
            hashBuildUnique ( jht17, 20000, hash17, &(payl17));
        }
        loopVar += step;
    }

}

__global__ void krnl_part13(
    int* iatt30_ppartkey, size_t* iatt34_ptype_offset, char* iatt34_ptype_char, int* iatt35_psize, agg_ht<jpayl16>* jht16) {
    int att30_ppartkey;
    str_t att34_ptype;
    int att35_psize;
    str_t c3 = stringConstant ( "%BRASS", 6);

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
            att30_ppartkey = iatt30_ppartkey[tid_part1];
            att34_ptype = stringScan ( iatt34_ptype_offset, iatt34_ptype_char, tid_part1);
            att35_psize = iatt35_psize[tid_part1];
        }
        // -------- selection (opId: 14) --------
        if(active) {
            active = (stringLikeCheck ( att34_ptype, c3) && (att35_psize == 15));
        }
        // -------- hash join build (opId: 16) --------
        if(active) {
            uint64_t hash16;
            hash16 = 0;
            if(active) {
                hash16 = hash ( (hash16 + ((uint64_t)att30_ppartkey)));
            }
            int bucket = 0;
            jpayl16 payl16;
            payl16.att30_ppartkey = att30_ppartkey;
            int bucketFound = 0;
            int numLookups = 0;
            while(!(bucketFound)) {
                bucket = hashAggregateGetBucket ( jht16, 400000, hash16, numLookups, &(payl16));
                jpayl16 probepayl = jht16[bucket].payload;
                bucketFound = 1;
                bucketFound &= ((payl16.att30_ppartkey == probepayl.att30_ppartkey));
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_partsupp15(
    int* iatt39_pspartke, int* iatt40_pssuppke, float* iatt42_pssupply, agg_ht<jpayl16>* jht16, unique_ht<jpayl17>* jht17, agg_ht<apayl18>* aht18, float* agg1) {
    int att39_pspartke;
    int att40_pssuppke;
    float att42_pssupply;
    int att30_ppartkey;
    int att23_ssuppkey;

    int tid_partsupp1 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_partsupp1 = loopVar;
        active = (loopVar < 800000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att39_pspartke = iatt39_pspartke[tid_partsupp1];
            att40_pssuppke = iatt40_pssuppke[tid_partsupp1];
            att42_pssupply = iatt42_pssupply[tid_partsupp1];
        }
        // -------- hash join probe (opId: 16) --------
        if(active) {
            uint64_t hash16 = 0;
            hash16 = 0;
            if(active) {
                hash16 = hash ( (hash16 + ((uint64_t)att39_pspartke)));
            }
            int numLookups16 = 0;
            int location16 = 0;
            int filterMatch16 = 0;
            int activeProbe16 = 1;
            while((!(filterMatch16) && activeProbe16)) {
                activeProbe16 = hashAggregateFindBucket ( jht16, 400000, hash16, numLookups16, location16);
                if(activeProbe16) {
                    jpayl16 probepayl = jht16[location16].payload;
                    att30_ppartkey = probepayl.att30_ppartkey;
                    filterMatch16 = 1;
                    filterMatch16 &= ((att30_ppartkey == att39_pspartke));
                }
            }
            active &= (filterMatch16);
        }
        // -------- hash join probe (opId: 17) --------
        uint64_t hash17 = 0;
        if(active) {
            hash17 = 0;
            if(active) {
                hash17 = hash ( (hash17 + ((uint64_t)att40_pssuppke)));
            }
        }
        jpayl17* probepayl17;
        int numLookups17 = 0;
        if(active) {
            active = hashProbeUnique ( jht17, 20000, hash17, numLookups17, &(probepayl17));
        }
        int bucketFound17 = 0;
        int probeActive17 = active;
        while((probeActive17 && !(bucketFound17))) {
            jpayl17 jprobepayl17 = *(probepayl17);
            att23_ssuppkey = jprobepayl17.att23_ssuppkey;
            bucketFound17 = 1;
            bucketFound17 &= ((att23_ssuppkey == att40_pssuppke));
            if(!(bucketFound17)) {
                probeActive17 = hashProbeUnique ( jht17, 20000, hash17, numLookups17, &(probepayl17));
            }
        }
        active = bucketFound17;
        // -------- aggregation (opId: 18) --------
        int bucket = 0;
        if(active) {
            uint64_t hash18 = 0;
            hash18 = 0;
            if(active) {
                hash18 = hash ( (hash18 + ((uint64_t)att39_pspartke)));
            }
            apayl18 payl;
            payl.att39_pspartke = att39_pspartke;
            int bucketFound = 0;
            int numLookups = 0;
            while(!(bucketFound)) {
                bucket = hashAggregateGetBucket ( aht18, 1600000, hash18, numLookups, &(payl));
                apayl18 probepayl = aht18[bucket].payload;
                bucketFound = 1;
                bucketFound &= ((payl.att39_pspartke == probepayl.att39_pspartke));
            }
        }
        if(active) {
            atomicMin(&(agg1[bucket]), ((float)att42_pssupply));
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation18(
    agg_ht<apayl18>* aht18, float* agg1, multi_ht* jht21, jpayl21* jht21_payload) {
    int att39_pspartke;
    float att1_minsuppl;

    int tid_aggregation18 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation18 = loopVar;
        active = (loopVar < 1600000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 18) --------
        if(active) {
            active &= ((aht18[tid_aggregation18].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl18 payl = aht18[tid_aggregation18].payload;
            att39_pspartke = payl.att39_pspartke;
        }
        if(active) {
            att1_minsuppl = agg1[tid_aggregation18];
        }
        // -------- hash join build (opId: 21) --------
        if(active) {
            uint64_t hash21 = 0;
            if(active) {
                hash21 = 0;
                if(active) {
                    hash21 = hash ( (hash21 + ((uint64_t)att39_pspartke)));
                }
            }
            hashCountMulti ( jht21, 1600000, hash21);
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation18_ins(
    agg_ht<apayl18>* aht18, float* agg1, multi_ht* jht21, jpayl21* jht21_payload, int* offs21) {
    int att39_pspartke;
    float att1_minsuppl;

    int tid_aggregation18 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation18 = loopVar;
        active = (loopVar < 1600000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 18) --------
        if(active) {
            active &= ((aht18[tid_aggregation18].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl18 payl = aht18[tid_aggregation18].payload;
            att39_pspartke = payl.att39_pspartke;
        }
        if(active) {
            att1_minsuppl = agg1[tid_aggregation18];
        }
        // -------- hash join build (opId: 21) --------
        if(active) {
            uint64_t hash21 = 0;
            if(active) {
                hash21 = 0;
                if(active) {
                    hash21 = hash ( (hash21 + ((uint64_t)att39_pspartke)));
                }
            }
            jpayl21 payl;
            payl.att39_pspartke = att39_pspartke;
            payl.att1_minsuppl = att1_minsuppl;
            hashInsertMulti ( jht21, jht21_payload, offs21, 1600000, hash21, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_part219(
    int* iatt44_ppartkey, size_t* iatt46_pmfgr_offset, char* iatt46_pmfgr_char, size_t* iatt48_ptype_offset, char* iatt48_ptype_char, int* iatt49_psize, multi_ht* jht21, jpayl21* jht21_payload, multi_ht* jht23, jpayl23* jht23_payload) {
    int att44_ppartkey;
    str_t att46_pmfgr;
    str_t att48_ptype;
    int att49_psize;
    str_t c4 = stringConstant ( "%BRASS", 6);
    unsigned warplane = (threadIdx.x % 32);
    int att39_pspartke;
    float att1_minsuppl;

    int tid_part2 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_part2 = loopVar;
        active = (loopVar < 200000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att44_ppartkey = iatt44_ppartkey[tid_part2];
            att46_pmfgr = stringScan ( iatt46_pmfgr_offset, iatt46_pmfgr_char, tid_part2);
            att48_ptype = stringScan ( iatt48_ptype_offset, iatt48_ptype_char, tid_part2);
            att49_psize = iatt49_psize[tid_part2];
        }
        // -------- selection (opId: 20) --------
        if(active) {
            active = (stringLikeCheck ( att48_ptype, c4) && (att49_psize == 15));
        }
        // -------- hash join probe (opId: 21) --------
        // -------- multiprobe multi broadcast (opId: 21) --------
        int matchEnd21 = 0;
        int matchEndBuf21 = 0;
        int matchOffset21 = 0;
        int matchOffsetBuf21 = 0;
        int probeActive21 = active;
        int att44_ppartkey_bcbuf21;
        str_t att46_pmfgr_bcbuf21;
        uint64_t hash21 = 0;
        if(probeActive21) {
            hash21 = 0;
            if(active) {
                hash21 = hash ( (hash21 + ((uint64_t)att44_ppartkey)));
            }
            probeActive21 = hashProbeMulti ( jht21, 1600000, hash21, matchOffsetBuf21, matchEndBuf21);
        }
        unsigned activeProbes21 = __ballot_sync(ALL_LANES,probeActive21);
        int num21 = 0;
        num21 = (matchEndBuf21 - matchOffsetBuf21);
        unsigned wideProbes21 = __ballot_sync(ALL_LANES,(num21 >= 32));
        att44_ppartkey_bcbuf21 = att44_ppartkey;
        att46_pmfgr_bcbuf21 = att46_pmfgr;
        while((activeProbes21 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes21)) {
                if((wideProbes21 > 0)) {
                    tupleLane = (__ffs(wideProbes21) - 1);
                    wideProbes21 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes21) - 1);
                }
                num = __shfl_sync(ALL_LANES,num21,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset21 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes21 -= (1 << tupleLane);
            }
            matchOffset21 += __shfl_sync(ALL_LANES,matchOffsetBuf21,broadcastLane);
            matchEnd21 = __shfl_sync(ALL_LANES,matchEndBuf21,broadcastLane);
            att44_ppartkey = __shfl_sync(ALL_LANES,att44_ppartkey_bcbuf21,broadcastLane);
            att46_pmfgr = __shfl_sync(ALL_LANES,att46_pmfgr_bcbuf21,broadcastLane);
            probeActive21 = (matchOffset21 < matchEnd21);
            while(__any_sync(ALL_LANES,probeActive21)) {
                active = probeActive21;
                active = 0;
                jpayl21 payl;
                if(probeActive21) {
                    payl = jht21_payload[matchOffset21];
                    att39_pspartke = payl.att39_pspartke;
                    att1_minsuppl = payl.att1_minsuppl;
                    active = 1;
                    active &= ((att39_pspartke == att44_ppartkey));
                    matchOffset21 += 32;
                    probeActive21 &= ((matchOffset21 < matchEnd21));
                }
                // -------- hash join build (opId: 23) --------
                if(active) {
                    uint64_t hash23 = 0;
                    if(active) {
                        hash23 = 0;
                        if(active) {
                            hash23 = hash ( (hash23 + ((uint64_t)att39_pspartke)));
                        }
                    }
                    hashCountMulti ( jht23, 1600000, hash23);
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_part219_ins(
    int* iatt44_ppartkey, size_t* iatt46_pmfgr_offset, char* iatt46_pmfgr_char, size_t* iatt48_ptype_offset, char* iatt48_ptype_char, int* iatt49_psize, multi_ht* jht21, jpayl21* jht21_payload, multi_ht* jht23, jpayl23* jht23_payload, int* offs23) {
    int att44_ppartkey;
    str_t att46_pmfgr;
    str_t att48_ptype;
    int att49_psize;
    str_t c4 = stringConstant ( "%BRASS", 6);
    unsigned warplane = (threadIdx.x % 32);
    int att39_pspartke;
    float att1_minsuppl;

    int tid_part2 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_part2 = loopVar;
        active = (loopVar < 200000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att44_ppartkey = iatt44_ppartkey[tid_part2];
            att46_pmfgr = stringScan ( iatt46_pmfgr_offset, iatt46_pmfgr_char, tid_part2);
            att48_ptype = stringScan ( iatt48_ptype_offset, iatt48_ptype_char, tid_part2);
            att49_psize = iatt49_psize[tid_part2];
        }
        // -------- selection (opId: 20) --------
        if(active) {
            active = (stringLikeCheck ( att48_ptype, c4) && (att49_psize == 15));
        }
        // -------- hash join probe (opId: 21) --------
        // -------- multiprobe multi broadcast (opId: 21) --------
        int matchEnd21 = 0;
        int matchEndBuf21 = 0;
        int matchOffset21 = 0;
        int matchOffsetBuf21 = 0;
        int probeActive21 = active;
        int att44_ppartkey_bcbuf21;
        str_t att46_pmfgr_bcbuf21;
        uint64_t hash21 = 0;
        if(probeActive21) {
            hash21 = 0;
            if(active) {
                hash21 = hash ( (hash21 + ((uint64_t)att44_ppartkey)));
            }
            probeActive21 = hashProbeMulti ( jht21, 1600000, hash21, matchOffsetBuf21, matchEndBuf21);
        }
        unsigned activeProbes21 = __ballot_sync(ALL_LANES,probeActive21);
        int num21 = 0;
        num21 = (matchEndBuf21 - matchOffsetBuf21);
        unsigned wideProbes21 = __ballot_sync(ALL_LANES,(num21 >= 32));
        att44_ppartkey_bcbuf21 = att44_ppartkey;
        att46_pmfgr_bcbuf21 = att46_pmfgr;
        while((activeProbes21 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes21)) {
                if((wideProbes21 > 0)) {
                    tupleLane = (__ffs(wideProbes21) - 1);
                    wideProbes21 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes21) - 1);
                }
                num = __shfl_sync(ALL_LANES,num21,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset21 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes21 -= (1 << tupleLane);
            }
            matchOffset21 += __shfl_sync(ALL_LANES,matchOffsetBuf21,broadcastLane);
            matchEnd21 = __shfl_sync(ALL_LANES,matchEndBuf21,broadcastLane);
            att44_ppartkey = __shfl_sync(ALL_LANES,att44_ppartkey_bcbuf21,broadcastLane);
            att46_pmfgr = __shfl_sync(ALL_LANES,att46_pmfgr_bcbuf21,broadcastLane);
            probeActive21 = (matchOffset21 < matchEnd21);
            while(__any_sync(ALL_LANES,probeActive21)) {
                active = probeActive21;
                active = 0;
                jpayl21 payl;
                if(probeActive21) {
                    payl = jht21_payload[matchOffset21];
                    att39_pspartke = payl.att39_pspartke;
                    att1_minsuppl = payl.att1_minsuppl;
                    active = 1;
                    active &= ((att39_pspartke == att44_ppartkey));
                    matchOffset21 += 32;
                    probeActive21 &= ((matchOffset21 < matchEnd21));
                }
                // -------- hash join build (opId: 23) --------
                if(active) {
                    uint64_t hash23 = 0;
                    if(active) {
                        hash23 = 0;
                        if(active) {
                            hash23 = hash ( (hash23 + ((uint64_t)att39_pspartke)));
                        }
                    }
                    jpayl23 payl;
                    payl.att39_pspartke = att39_pspartke;
                    payl.att1_minsuppl = att1_minsuppl;
                    payl.att44_ppartkey = att44_ppartkey;
                    payl.att46_pmfgr = att46_pmfgr;
                    hashInsertMulti ( jht23, jht23_payload, offs23, 1600000, hash23, &(payl));
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_partsupp222(
    int* iatt53_pspartke, int* iatt54_pssuppke, float* iatt56_pssupply, multi_ht* jht23, jpayl23* jht23_payload, unique_ht<jpayl25>* jht25, int* nout_result, float* oatt14_sacctbal, str_offs* oatt10_sname_offset, char* iatt10_sname_char, str_offs* oatt6_nname_offset, char* iatt6_nname_char, int* oatt44_ppartkey, str_offs* oatt46_pmfgr_offset, char* iatt46_pmfgr_char, str_offs* oatt11_saddress_offset, char* iatt11_saddress_char, str_offs* oatt13_sphone_offset, char* iatt13_sphone_char, str_offs* oatt15_scomment_offset, char* iatt15_scomment_char) {
    int att53_pspartke;
    int att54_pssuppke;
    float att56_pssupply;
    unsigned warplane = (threadIdx.x % 32);
    int att39_pspartke;
    float att1_minsuppl;
    int att44_ppartkey;
    str_t att46_pmfgr;
    str_t att6_nname;
    int att9_ssuppkey;
    str_t att10_sname;
    str_t att11_saddress;
    str_t att13_sphone;
    float att14_sacctbal;
    str_t att15_scomment;
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_partsupp2 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_partsupp2 = loopVar;
        active = (loopVar < 800000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
            att53_pspartke = iatt53_pspartke[tid_partsupp2];
            att54_pssuppke = iatt54_pssuppke[tid_partsupp2];
            att56_pssupply = iatt56_pssupply[tid_partsupp2];
        }
        // -------- hash join probe (opId: 23) --------
        // -------- multiprobe multi broadcast (opId: 23) --------
        int matchEnd23 = 0;
        int matchEndBuf23 = 0;
        int matchOffset23 = 0;
        int matchOffsetBuf23 = 0;
        int probeActive23 = active;
        int att53_pspartke_bcbuf23;
        int att54_pssuppke_bcbuf23;
        float att56_pssupply_bcbuf23;
        uint64_t hash23 = 0;
        if(probeActive23) {
            hash23 = 0;
            if(active) {
                hash23 = hash ( (hash23 + ((uint64_t)att53_pspartke)));
            }
            probeActive23 = hashProbeMulti ( jht23, 1600000, hash23, matchOffsetBuf23, matchEndBuf23);
        }
        unsigned activeProbes23 = __ballot_sync(ALL_LANES,probeActive23);
        int num23 = 0;
        num23 = (matchEndBuf23 - matchOffsetBuf23);
        unsigned wideProbes23 = __ballot_sync(ALL_LANES,(num23 >= 32));
        att53_pspartke_bcbuf23 = att53_pspartke;
        att54_pssuppke_bcbuf23 = att54_pssuppke;
        att56_pssupply_bcbuf23 = att56_pssupply;
        while((activeProbes23 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes23)) {
                if((wideProbes23 > 0)) {
                    tupleLane = (__ffs(wideProbes23) - 1);
                    wideProbes23 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes23) - 1);
                }
                num = __shfl_sync(ALL_LANES,num23,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset23 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes23 -= (1 << tupleLane);
            }
            matchOffset23 += __shfl_sync(ALL_LANES,matchOffsetBuf23,broadcastLane);
            matchEnd23 = __shfl_sync(ALL_LANES,matchEndBuf23,broadcastLane);
            att53_pspartke = __shfl_sync(ALL_LANES,att53_pspartke_bcbuf23,broadcastLane);
            att54_pssuppke = __shfl_sync(ALL_LANES,att54_pssuppke_bcbuf23,broadcastLane);
            att56_pssupply = __shfl_sync(ALL_LANES,att56_pssupply_bcbuf23,broadcastLane);
            probeActive23 = (matchOffset23 < matchEnd23);
            while(__any_sync(ALL_LANES,probeActive23)) {
                active = probeActive23;
                active = 0;
                jpayl23 payl;
                if(probeActive23) {
                    payl = jht23_payload[matchOffset23];
                    att39_pspartke = payl.att39_pspartke;
                    att1_minsuppl = payl.att1_minsuppl;
                    att44_ppartkey = payl.att44_ppartkey;
                    att46_pmfgr = payl.att46_pmfgr;
                    active = 1;
                    active &= ((att39_pspartke == att53_pspartke));
                    matchOffset23 += 32;
                    probeActive23 &= ((matchOffset23 < matchEnd23));
                }
                // -------- selection (opId: 24) --------
                if(active) {
                    active = (att1_minsuppl == att56_pssupply);
                }
                // -------- hash join probe (opId: 25) --------
                uint64_t hash25 = 0;
                if(active) {
                    hash25 = 0;
                    if(active) {
                        hash25 = hash ( (hash25 + ((uint64_t)att54_pssuppke)));
                    }
                }
                jpayl25* probepayl25;
                int numLookups25 = 0;
                if(active) {
                    active = hashProbeUnique ( jht25, 20000, hash25, numLookups25, &(probepayl25));
                }
                int bucketFound25 = 0;
                int probeActive25 = active;
                while((probeActive25 && !(bucketFound25))) {
                    jpayl25 jprobepayl25 = *(probepayl25);
                    att6_nname = jprobepayl25.att6_nname;
                    att9_ssuppkey = jprobepayl25.att9_ssuppkey;
                    att10_sname = jprobepayl25.att10_sname;
                    att11_saddress = jprobepayl25.att11_saddress;
                    att13_sphone = jprobepayl25.att13_sphone;
                    att14_sacctbal = jprobepayl25.att14_sacctbal;
                    att15_scomment = jprobepayl25.att15_scomment;
                    bucketFound25 = 1;
                    bucketFound25 &= ((att9_ssuppkey == att54_pssuppke));
                    if(!(bucketFound25)) {
                        probeActive25 = hashProbeUnique ( jht25, 20000, hash25, numLookups25, &(probepayl25));
                    }
                }
                active = bucketFound25;
                // -------- projection (no code) (opId: 26) --------
                // -------- materialize (opId: 27) --------
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
                    oatt14_sacctbal[wp] = att14_sacctbal;
                    oatt10_sname_offset[wp] = toStringOffset ( iatt10_sname_char, att10_sname);
                    oatt6_nname_offset[wp] = toStringOffset ( iatt6_nname_char, att6_nname);
                    oatt44_ppartkey[wp] = att44_ppartkey;
                    oatt46_pmfgr_offset[wp] = toStringOffset ( iatt46_pmfgr_char, att46_pmfgr);
                    oatt11_saddress_offset[wp] = toStringOffset ( iatt11_saddress_char, att11_saddress);
                    oatt13_sphone_offset[wp] = toStringOffset ( iatt13_sphone_char, att13_sphone);
                    oatt15_scomment_offset[wp] = toStringOffset ( iatt15_scomment_char, att15_scomment);
                }
            }
        }
        loopVar += step;
    }

}

int main() {
    int* iatt2_rregionk;
    iatt2_rregionk = ( int*) map_memory_file ( "mmdb/region_r_regionkey" );
    size_t* iatt3_rname_offset;
    iatt3_rname_offset = ( size_t*) map_memory_file ( "mmdb/region_r_name_offset" );
    char* iatt3_rname_char;
    iatt3_rname_char = ( char*) map_memory_file ( "mmdb/region_r_name_char" );
    int* iatt5_nnationk;
    iatt5_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    size_t* iatt6_nname_offset;
    iatt6_nname_offset = ( size_t*) map_memory_file ( "mmdb/nation_n_name_offset" );
    char* iatt6_nname_char;
    iatt6_nname_char = ( char*) map_memory_file ( "mmdb/nation_n_name_char" );
    int* iatt7_nregionk;
    iatt7_nregionk = ( int*) map_memory_file ( "mmdb/nation_n_regionkey" );
    int* iatt9_ssuppkey;
    iatt9_ssuppkey = ( int*) map_memory_file ( "mmdb/supplier_s_suppkey" );
    size_t* iatt10_sname_offset;
    iatt10_sname_offset = ( size_t*) map_memory_file ( "mmdb/supplier_s_name_offset" );
    char* iatt10_sname_char;
    iatt10_sname_char = ( char*) map_memory_file ( "mmdb/supplier_s_name_char" );
    size_t* iatt11_saddress_offset;
    iatt11_saddress_offset = ( size_t*) map_memory_file ( "mmdb/supplier_s_address_offset" );
    char* iatt11_saddress_char;
    iatt11_saddress_char = ( char*) map_memory_file ( "mmdb/supplier_s_address_char" );
    int* iatt12_snationk;
    iatt12_snationk = ( int*) map_memory_file ( "mmdb/supplier_s_nationkey" );
    size_t* iatt13_sphone_offset;
    iatt13_sphone_offset = ( size_t*) map_memory_file ( "mmdb/supplier_s_phone_offset" );
    char* iatt13_sphone_char;
    iatt13_sphone_char = ( char*) map_memory_file ( "mmdb/supplier_s_phone_char" );
    float* iatt14_sacctbal;
    iatt14_sacctbal = ( float*) map_memory_file ( "mmdb/supplier_s_acctbal" );
    size_t* iatt15_scomment_offset;
    iatt15_scomment_offset = ( size_t*) map_memory_file ( "mmdb/supplier_s_comment_offset" );
    char* iatt15_scomment_char;
    iatt15_scomment_char = ( char*) map_memory_file ( "mmdb/supplier_s_comment_char" );
    int* iatt16_rregionk;
    iatt16_rregionk = ( int*) map_memory_file ( "mmdb/region_r_regionkey" );
    size_t* iatt17_rname_offset;
    iatt17_rname_offset = ( size_t*) map_memory_file ( "mmdb/region_r_name_offset" );
    char* iatt17_rname_char;
    iatt17_rname_char = ( char*) map_memory_file ( "mmdb/region_r_name_char" );
    int* iatt19_nnationk;
    iatt19_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    int* iatt21_nregionk;
    iatt21_nregionk = ( int*) map_memory_file ( "mmdb/nation_n_regionkey" );
    int* iatt23_ssuppkey;
    iatt23_ssuppkey = ( int*) map_memory_file ( "mmdb/supplier_s_suppkey" );
    int* iatt26_snationk;
    iatt26_snationk = ( int*) map_memory_file ( "mmdb/supplier_s_nationkey" );
    int* iatt30_ppartkey;
    iatt30_ppartkey = ( int*) map_memory_file ( "mmdb/part_p_partkey" );
    size_t* iatt34_ptype_offset;
    iatt34_ptype_offset = ( size_t*) map_memory_file ( "mmdb/part_p_type_offset" );
    char* iatt34_ptype_char;
    iatt34_ptype_char = ( char*) map_memory_file ( "mmdb/part_p_type_char" );
    int* iatt35_psize;
    iatt35_psize = ( int*) map_memory_file ( "mmdb/part_p_size" );
    int* iatt39_pspartke;
    iatt39_pspartke = ( int*) map_memory_file ( "mmdb/partsupp_ps_partkey" );
    int* iatt40_pssuppke;
    iatt40_pssuppke = ( int*) map_memory_file ( "mmdb/partsupp_ps_suppkey" );
    float* iatt42_pssupply;
    iatt42_pssupply = ( float*) map_memory_file ( "mmdb/partsupp_ps_supplycost" );
    int* iatt44_ppartkey;
    iatt44_ppartkey = ( int*) map_memory_file ( "mmdb/part_p_partkey" );
    size_t* iatt46_pmfgr_offset;
    iatt46_pmfgr_offset = ( size_t*) map_memory_file ( "mmdb/part_p_mfgr_offset" );
    char* iatt46_pmfgr_char;
    iatt46_pmfgr_char = ( char*) map_memory_file ( "mmdb/part_p_mfgr_char" );
    size_t* iatt48_ptype_offset;
    iatt48_ptype_offset = ( size_t*) map_memory_file ( "mmdb/part_p_type_offset" );
    char* iatt48_ptype_char;
    iatt48_ptype_char = ( char*) map_memory_file ( "mmdb/part_p_type_char" );
    int* iatt49_psize;
    iatt49_psize = ( int*) map_memory_file ( "mmdb/part_p_size" );
    int* iatt53_pspartke;
    iatt53_pspartke = ( int*) map_memory_file ( "mmdb/partsupp_ps_partkey" );
    int* iatt54_pssuppke;
    iatt54_pssuppke = ( int*) map_memory_file ( "mmdb/partsupp_ps_suppkey" );
    float* iatt56_pssupply;
    iatt56_pssupply = ( float*) map_memory_file ( "mmdb/partsupp_ps_supplycost" );

    int nout_result;
    std::vector < float > oatt14_sacctbal(800000);
    std::vector < str_offs > oatt10_sname_offset(800000);
    std::vector < str_offs > oatt6_nname_offset(800000);
    std::vector < int > oatt44_ppartkey(800000);
    std::vector < str_offs > oatt46_pmfgr_offset(800000);
    std::vector < str_offs > oatt11_saddress_offset(800000);
    std::vector < str_offs > oatt13_sphone_offset(800000);
    std::vector < str_offs > oatt15_scomment_offset(800000);

    // wake up gpu
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in wake up gpu! " << cudaGetErrorString( err ) << std::endl;
            ERROR("wake up gpu")
        }
    }

    int* d_iatt2_rregionk;
    cudaMalloc((void**) &d_iatt2_rregionk, 5* sizeof(int) );
    size_t* d_iatt3_rname_offset;
    cudaMalloc((void**) &d_iatt3_rname_offset, (5 + 1)* sizeof(size_t) );
    char* d_iatt3_rname_char;
    cudaMalloc((void**) &d_iatt3_rname_char, 43* sizeof(char) );
    int* d_iatt5_nnationk;
    cudaMalloc((void**) &d_iatt5_nnationk, 25* sizeof(int) );
    size_t* d_iatt6_nname_offset;
    cudaMalloc((void**) &d_iatt6_nname_offset, (25 + 1)* sizeof(size_t) );
    char* d_iatt6_nname_char;
    cudaMalloc((void**) &d_iatt6_nname_char, 186* sizeof(char) );
    int* d_iatt7_nregionk;
    cudaMalloc((void**) &d_iatt7_nregionk, 25* sizeof(int) );
    int* d_iatt9_ssuppkey;
    cudaMalloc((void**) &d_iatt9_ssuppkey, 10000* sizeof(int) );
    size_t* d_iatt10_sname_offset;
    cudaMalloc((void**) &d_iatt10_sname_offset, (10000 + 1)* sizeof(size_t) );
    char* d_iatt10_sname_char;
    cudaMalloc((void**) &d_iatt10_sname_char, 180009* sizeof(char) );
    size_t* d_iatt11_saddress_offset;
    cudaMalloc((void**) &d_iatt11_saddress_offset, (10000 + 1)* sizeof(size_t) );
    char* d_iatt11_saddress_char;
    cudaMalloc((void**) &d_iatt11_saddress_char, 249461* sizeof(char) );
    int* d_iatt12_snationk;
    cudaMalloc((void**) &d_iatt12_snationk, 10000* sizeof(int) );
    size_t* d_iatt13_sphone_offset;
    cudaMalloc((void**) &d_iatt13_sphone_offset, (10000 + 1)* sizeof(size_t) );
    char* d_iatt13_sphone_char;
    cudaMalloc((void**) &d_iatt13_sphone_char, 150009* sizeof(char) );
    float* d_iatt14_sacctbal;
    cudaMalloc((void**) &d_iatt14_sacctbal, 10000* sizeof(float) );
    size_t* d_iatt15_scomment_offset;
    cudaMalloc((void**) &d_iatt15_scomment_offset, (10000 + 1)* sizeof(size_t) );
    char* d_iatt15_scomment_char;
    cudaMalloc((void**) &d_iatt15_scomment_char, 623073* sizeof(char) );
    int* d_iatt16_rregionk;
    d_iatt16_rregionk = d_iatt2_rregionk;
    size_t* d_iatt17_rname_offset;
    d_iatt17_rname_offset = d_iatt3_rname_offset;
    char* d_iatt17_rname_char;
    d_iatt17_rname_char = d_iatt3_rname_char;
    int* d_iatt19_nnationk;
    d_iatt19_nnationk = d_iatt5_nnationk;
    int* d_iatt21_nregionk;
    d_iatt21_nregionk = d_iatt7_nregionk;
    int* d_iatt23_ssuppkey;
    d_iatt23_ssuppkey = d_iatt9_ssuppkey;
    int* d_iatt26_snationk;
    d_iatt26_snationk = d_iatt12_snationk;
    int* d_iatt30_ppartkey;
    cudaMalloc((void**) &d_iatt30_ppartkey, 200000* sizeof(int) );
    size_t* d_iatt34_ptype_offset;
    cudaMalloc((void**) &d_iatt34_ptype_offset, (200000 + 1)* sizeof(size_t) );
    char* d_iatt34_ptype_char;
    cudaMalloc((void**) &d_iatt34_ptype_char, 4119955* sizeof(char) );
    int* d_iatt35_psize;
    cudaMalloc((void**) &d_iatt35_psize, 200000* sizeof(int) );
    int* d_iatt39_pspartke;
    cudaMalloc((void**) &d_iatt39_pspartke, 800000* sizeof(int) );
    int* d_iatt40_pssuppke;
    cudaMalloc((void**) &d_iatt40_pssuppke, 800000* sizeof(int) );
    float* d_iatt42_pssupply;
    cudaMalloc((void**) &d_iatt42_pssupply, 800000* sizeof(float) );
    int* d_iatt44_ppartkey;
    d_iatt44_ppartkey = d_iatt30_ppartkey;
    size_t* d_iatt46_pmfgr_offset;
    cudaMalloc((void**) &d_iatt46_pmfgr_offset, (200000 + 1)* sizeof(size_t) );
    char* d_iatt46_pmfgr_char;
    cudaMalloc((void**) &d_iatt46_pmfgr_char, 2800009* sizeof(char) );
    size_t* d_iatt48_ptype_offset;
    d_iatt48_ptype_offset = d_iatt34_ptype_offset;
    char* d_iatt48_ptype_char;
    d_iatt48_ptype_char = d_iatt34_ptype_char;
    int* d_iatt49_psize;
    d_iatt49_psize = d_iatt35_psize;
    int* d_iatt53_pspartke;
    d_iatt53_pspartke = d_iatt39_pspartke;
    int* d_iatt54_pssuppke;
    d_iatt54_pssuppke = d_iatt40_pssuppke;
    float* d_iatt56_pssupply;
    d_iatt56_pssupply = d_iatt42_pssupply;
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    float* d_oatt14_sacctbal;
    cudaMalloc((void**) &d_oatt14_sacctbal, 800000* sizeof(float) );
    str_offs* d_oatt10_sname_offset;
    cudaMalloc((void**) &d_oatt10_sname_offset, 800000* sizeof(str_offs) );
    str_offs* d_oatt6_nname_offset;
    cudaMalloc((void**) &d_oatt6_nname_offset, 800000* sizeof(str_offs) );
    int* d_oatt44_ppartkey;
    cudaMalloc((void**) &d_oatt44_ppartkey, 800000* sizeof(int) );
    str_offs* d_oatt46_pmfgr_offset;
    cudaMalloc((void**) &d_oatt46_pmfgr_offset, 800000* sizeof(str_offs) );
    str_offs* d_oatt11_saddress_offset;
    cudaMalloc((void**) &d_oatt11_saddress_offset, 800000* sizeof(str_offs) );
    str_offs* d_oatt13_sphone_offset;
    cudaMalloc((void**) &d_oatt13_sphone_offset, 800000* sizeof(str_offs) );
    str_offs* d_oatt15_scomment_offset;
    cudaMalloc((void**) &d_oatt15_scomment_offset, 800000* sizeof(str_offs) );
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

    unique_ht<jpayl4>* d_jht4;
    cudaMalloc((void**) &d_jht4, 10* sizeof(unique_ht<jpayl4>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht4, 10);
    }
    multi_ht* d_jht6;
    cudaMalloc((void**) &d_jht6, 50* sizeof(multi_ht) );
    jpayl6* d_jht6_payload;
    cudaMalloc((void**) &d_jht6_payload, 50* sizeof(jpayl6) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht6, 50);
    }
    int* d_offs6;
    cudaMalloc((void**) &d_offs6, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs6, 0, 1);
    }
    unique_ht<jpayl25>* d_jht25;
    cudaMalloc((void**) &d_jht25, 20000* sizeof(unique_ht<jpayl25>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht25, 20000);
    }
    unique_ht<jpayl10>* d_jht10;
    cudaMalloc((void**) &d_jht10, 10* sizeof(unique_ht<jpayl10>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht10, 10);
    }
    unique_ht<jpayl12>* d_jht12;
    cudaMalloc((void**) &d_jht12, 50* sizeof(unique_ht<jpayl12>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht12, 50);
    }
    unique_ht<jpayl17>* d_jht17;
    cudaMalloc((void**) &d_jht17, 20000* sizeof(unique_ht<jpayl17>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht17, 20000);
    }
    agg_ht<jpayl16>* d_jht16;
    cudaMalloc((void**) &d_jht16, 400000* sizeof(agg_ht<jpayl16>) );
    {
        int gridsize=920;
        int blocksize=128;
        initAggHT<<<gridsize, blocksize>>>(d_jht16, 400000);
    }
    agg_ht<apayl18>* d_aht18;
    cudaMalloc((void**) &d_aht18, 1600000* sizeof(agg_ht<apayl18>) );
    {
        int gridsize=920;
        int blocksize=128;
        initAggHT<<<gridsize, blocksize>>>(d_aht18, 1600000);
    }
    float* d_agg1;
    cudaMalloc((void**) &d_agg1, 1600000* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg1, FLT_MAX, 1600000);
    }
    multi_ht* d_jht21;
    cudaMalloc((void**) &d_jht21, 1600000* sizeof(multi_ht) );
    jpayl21* d_jht21_payload;
    cudaMalloc((void**) &d_jht21_payload, 1600000* sizeof(jpayl21) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht21, 1600000);
    }
    int* d_offs21;
    cudaMalloc((void**) &d_offs21, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs21, 0, 1);
    }
    multi_ht* d_jht23;
    cudaMalloc((void**) &d_jht23, 1600000* sizeof(multi_ht) );
    jpayl23* d_jht23_payload;
    cudaMalloc((void**) &d_jht23_payload, 1600000* sizeof(jpayl23) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht23, 1600000);
    }
    int* d_offs23;
    cudaMalloc((void**) &d_offs23, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs23, 0, 1);
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

    cudaMemcpy( d_iatt2_rregionk, iatt2_rregionk, 5 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt3_rname_offset, iatt3_rname_offset, (5 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt3_rname_char, iatt3_rname_char, 43 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt5_nnationk, iatt5_nnationk, 25 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt6_nname_offset, iatt6_nname_offset, (25 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt6_nname_char, iatt6_nname_char, 186 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt7_nregionk, iatt7_nregionk, 25 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt9_ssuppkey, iatt9_ssuppkey, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt10_sname_offset, iatt10_sname_offset, (10000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt10_sname_char, iatt10_sname_char, 180009 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt11_saddress_offset, iatt11_saddress_offset, (10000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt11_saddress_char, iatt11_saddress_char, 249461 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt12_snationk, iatt12_snationk, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt13_sphone_offset, iatt13_sphone_offset, (10000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt13_sphone_char, iatt13_sphone_char, 150009 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt14_sacctbal, iatt14_sacctbal, 10000 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt15_scomment_offset, iatt15_scomment_offset, (10000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt15_scomment_char, iatt15_scomment_char, 623073 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt30_ppartkey, iatt30_ppartkey, 200000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt34_ptype_offset, iatt34_ptype_offset, (200000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt34_ptype_char, iatt34_ptype_char, 4119955 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt35_psize, iatt35_psize, 200000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt39_pspartke, iatt39_pspartke, 800000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt40_pssuppke, iatt40_pssuppke, 800000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt42_pssupply, iatt42_pssupply, 800000 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt46_pmfgr_offset, iatt46_pmfgr_offset, (200000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt46_pmfgr_char, iatt46_pmfgr_char, 2800009 * sizeof(char), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime4 = std::clock();
    std::clock_t start_krnl_region15 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_region1<<<gridsize, blocksize>>>(d_iatt2_rregionk, d_iatt3_rname_offset, d_iatt3_rname_char, d_jht4);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_region15 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_region1! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_region1")
        }
    }

    std::clock_t start_krnl_nation36 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation3<<<gridsize, blocksize>>>(d_iatt5_nnationk, d_iatt6_nname_offset, d_iatt6_nname_char, d_iatt7_nregionk, d_jht4, d_jht6, d_jht6_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation36 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation3! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation3")
        }
    }

    std::clock_t start_scanMultiHT7 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht6, 50, d_offs6);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT7 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_nation3_ins8 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation3_ins<<<gridsize, blocksize>>>(d_iatt5_nnationk, d_iatt6_nname_offset, d_iatt6_nname_char, d_iatt7_nregionk, d_jht4, d_jht6, d_jht6_payload, d_offs6);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation3_ins8 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation3_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation3_ins")
        }
    }

    std::clock_t start_krnl_supplier59 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_supplier5<<<gridsize, blocksize>>>(d_iatt9_ssuppkey, d_iatt10_sname_offset, d_iatt10_sname_char, d_iatt11_saddress_offset, d_iatt11_saddress_char, d_iatt12_snationk, d_iatt13_sphone_offset, d_iatt13_sphone_char, d_iatt14_sacctbal, d_iatt15_scomment_offset, d_iatt15_scomment_char, d_jht6, d_jht6_payload, d_jht25);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_supplier59 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_supplier5! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_supplier5")
        }
    }

    std::clock_t start_krnl_region2710 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_region27<<<gridsize, blocksize>>>(d_iatt16_rregionk, d_iatt17_rname_offset, d_iatt17_rname_char, d_jht10);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_region2710 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_region27! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_region27")
        }
    }

    std::clock_t start_krnl_nation2911 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation29<<<gridsize, blocksize>>>(d_iatt19_nnationk, d_iatt21_nregionk, d_jht10, d_jht12);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation2911 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation29! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation29")
        }
    }

    std::clock_t start_krnl_supplier21112 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_supplier211<<<gridsize, blocksize>>>(d_iatt23_ssuppkey, d_iatt26_snationk, d_jht12, d_jht17);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_supplier21112 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_supplier211! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_supplier211")
        }
    }

    std::clock_t start_krnl_part1313 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_part13<<<gridsize, blocksize>>>(d_iatt30_ppartkey, d_iatt34_ptype_offset, d_iatt34_ptype_char, d_iatt35_psize, d_jht16);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_part1313 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_part13! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_part13")
        }
    }

    std::clock_t start_krnl_partsupp1514 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_partsupp15<<<gridsize, blocksize>>>(d_iatt39_pspartke, d_iatt40_pssuppke, d_iatt42_pssupply, d_jht16, d_jht17, d_aht18, d_agg1);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_partsupp1514 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_partsupp15! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_partsupp15")
        }
    }

    std::clock_t start_krnl_aggregation1815 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation18<<<gridsize, blocksize>>>(d_aht18, d_agg1, d_jht21, d_jht21_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation1815 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation18! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation18")
        }
    }

    std::clock_t start_scanMultiHT16 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht21, 1600000, d_offs21);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT16 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_aggregation18_ins17 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation18_ins<<<gridsize, blocksize>>>(d_aht18, d_agg1, d_jht21, d_jht21_payload, d_offs21);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation18_ins17 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation18_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation18_ins")
        }
    }

    std::clock_t start_krnl_part21918 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_part219<<<gridsize, blocksize>>>(d_iatt44_ppartkey, d_iatt46_pmfgr_offset, d_iatt46_pmfgr_char, d_iatt48_ptype_offset, d_iatt48_ptype_char, d_iatt49_psize, d_jht21, d_jht21_payload, d_jht23, d_jht23_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_part21918 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_part219! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_part219")
        }
    }

    std::clock_t start_scanMultiHT19 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht23, 1600000, d_offs23);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT19 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_part219_ins20 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_part219_ins<<<gridsize, blocksize>>>(d_iatt44_ppartkey, d_iatt46_pmfgr_offset, d_iatt46_pmfgr_char, d_iatt48_ptype_offset, d_iatt48_ptype_char, d_iatt49_psize, d_jht21, d_jht21_payload, d_jht23, d_jht23_payload, d_offs23);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_part219_ins20 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_part219_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_part219_ins")
        }
    }

    std::clock_t start_krnl_partsupp22221 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_partsupp222<<<gridsize, blocksize>>>(d_iatt53_pspartke, d_iatt54_pssuppke, d_iatt56_pssupply, d_jht23, d_jht23_payload, d_jht25, d_nout_result, d_oatt14_sacctbal, d_oatt10_sname_offset, d_iatt10_sname_char, d_oatt6_nname_offset, d_iatt6_nname_char, d_oatt44_ppartkey, d_oatt46_pmfgr_offset, d_iatt46_pmfgr_char, d_oatt11_saddress_offset, d_iatt11_saddress_char, d_oatt13_sphone_offset, d_iatt13_sphone_char, d_oatt15_scomment_offset, d_iatt15_scomment_char);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_partsupp22221 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_partsupp222! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_partsupp222")
        }
    }

    std::clock_t stop_totalKernelTime4 = std::clock();
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt14_sacctbal.data(), d_oatt14_sacctbal, 800000 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt10_sname_offset.data(), d_oatt10_sname_offset, 800000 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt6_nname_offset.data(), d_oatt6_nname_offset, 800000 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt44_ppartkey.data(), d_oatt44_ppartkey, 800000 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt46_pmfgr_offset.data(), d_oatt46_pmfgr_offset, 800000 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt11_saddress_offset.data(), d_oatt11_saddress_offset, 800000 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt13_sphone_offset.data(), d_oatt13_sphone_offset, 800000 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt15_scomment_offset.data(), d_oatt15_scomment_offset, 800000 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    cudaFree( d_iatt2_rregionk);
    cudaFree( d_iatt3_rname_offset);
    cudaFree( d_iatt3_rname_char);
    cudaFree( d_jht4);
    cudaFree( d_iatt5_nnationk);
    cudaFree( d_iatt6_nname_offset);
    cudaFree( d_iatt6_nname_char);
    cudaFree( d_iatt7_nregionk);
    cudaFree( d_jht6);
    cudaFree( d_jht6_payload);
    cudaFree( d_offs6);
    cudaFree( d_iatt9_ssuppkey);
    cudaFree( d_iatt10_sname_offset);
    cudaFree( d_iatt10_sname_char);
    cudaFree( d_iatt11_saddress_offset);
    cudaFree( d_iatt11_saddress_char);
    cudaFree( d_iatt12_snationk);
    cudaFree( d_iatt13_sphone_offset);
    cudaFree( d_iatt13_sphone_char);
    cudaFree( d_iatt14_sacctbal);
    cudaFree( d_iatt15_scomment_offset);
    cudaFree( d_iatt15_scomment_char);
    cudaFree( d_jht25);
    cudaFree( d_jht10);
    cudaFree( d_jht12);
    cudaFree( d_jht17);
    cudaFree( d_iatt30_ppartkey);
    cudaFree( d_iatt34_ptype_offset);
    cudaFree( d_iatt34_ptype_char);
    cudaFree( d_iatt35_psize);
    cudaFree( d_jht16);
    cudaFree( d_iatt39_pspartke);
    cudaFree( d_iatt40_pssuppke);
    cudaFree( d_iatt42_pssupply);
    cudaFree( d_aht18);
    cudaFree( d_agg1);
    cudaFree( d_jht21);
    cudaFree( d_jht21_payload);
    cudaFree( d_offs21);
    cudaFree( d_iatt46_pmfgr_offset);
    cudaFree( d_iatt46_pmfgr_char);
    cudaFree( d_jht23);
    cudaFree( d_jht23_payload);
    cudaFree( d_offs23);
    cudaFree( d_nout_result);
    cudaFree( d_oatt14_sacctbal);
    cudaFree( d_oatt10_sname_offset);
    cudaFree( d_oatt6_nname_offset);
    cudaFree( d_oatt44_ppartkey);
    cudaFree( d_oatt46_pmfgr_offset);
    cudaFree( d_oatt11_saddress_offset);
    cudaFree( d_oatt13_sphone_offset);
    cudaFree( d_oatt15_scomment_offset);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda free! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda free")
        }
    }

    std::clock_t start_finish22 = std::clock();
    printf("\nResult: %i tuples\n", nout_result);
    if((nout_result > 800000)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("s_acctbal: ");
        printf("%15.2f", oatt14_sacctbal[pv]);
        printf("  ");
        printf("s_name: ");
        stringPrint ( iatt10_sname_char, oatt10_sname_offset[pv]);
        printf("  ");
        printf("n_name: ");
        stringPrint ( iatt6_nname_char, oatt6_nname_offset[pv]);
        printf("  ");
        printf("p_partkey: ");
        printf("%8i", oatt44_ppartkey[pv]);
        printf("  ");
        printf("p_mfgr: ");
        stringPrint ( iatt46_pmfgr_char, oatt46_pmfgr_offset[pv]);
        printf("  ");
        printf("s_address: ");
        stringPrint ( iatt11_saddress_char, oatt11_saddress_offset[pv]);
        printf("  ");
        printf("s_phone: ");
        stringPrint ( iatt13_sphone_char, oatt13_sphone_offset[pv]);
        printf("  ");
        printf("s_comment: ");
        stringPrint ( iatt15_scomment_char, oatt15_scomment_offset[pv]);
        printf("  ");
        printf("\n");
    }
    if((nout_result > 10)) {
        printf("[...]\n");
    }
    printf("\n");
    std::clock_t stop_finish22 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_region1", (stop_krnl_region15 - start_krnl_region15) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation3", (stop_krnl_nation36 - start_krnl_nation36) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT7 - start_scanMultiHT7) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation3_ins", (stop_krnl_nation3_ins8 - start_krnl_nation3_ins8) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_supplier5", (stop_krnl_supplier59 - start_krnl_supplier59) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_region27", (stop_krnl_region2710 - start_krnl_region2710) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation29", (stop_krnl_nation2911 - start_krnl_nation2911) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_supplier211", (stop_krnl_supplier21112 - start_krnl_supplier21112) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_part13", (stop_krnl_part1313 - start_krnl_part1313) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_partsupp15", (stop_krnl_partsupp1514 - start_krnl_partsupp1514) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation18", (stop_krnl_aggregation1815 - start_krnl_aggregation1815) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT16 - start_scanMultiHT16) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation18_ins", (stop_krnl_aggregation18_ins17 - start_krnl_aggregation18_ins17) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_part219", (stop_krnl_part21918 - start_krnl_part21918) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT19 - start_scanMultiHT19) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_part219_ins", (stop_krnl_part219_ins20 - start_krnl_part219_ins20) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_partsupp222", (stop_krnl_partsupp22221 - start_krnl_partsupp22221) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish22 - start_finish22) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime4 - start_totalKernelTime4) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
