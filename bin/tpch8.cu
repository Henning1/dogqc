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
struct jpayl18 {
    int att4_nnationk;
    str_t att5_nname;
};
struct jpayl5 {
    int att8_rregionk;
};
struct jpayl15 {
    int att11_nnationk;
};
struct jpayl9 {
    int att15_ppartkey;
};
struct jpayl12 {
    int att24_lorderke;
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
};
struct jpayl14 {
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
    int att41_ocustkey;
    unsigned att44_oorderda;
};
struct jpayl17 {
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
    unsigned att44_oorderda;
};
struct apayl22 {
    unsigned att64_oyear;
};

__global__ void krnl_nation1(
    int* iatt4_nnationk, size_t* iatt5_nname_offset, char* iatt5_nname_char, multi_ht* jht18, jpayl18* jht18_payload) {
    int att4_nnationk;
    str_t att5_nname;

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
            att4_nnationk = iatt4_nnationk[tid_nation1];
            att5_nname = stringScan ( iatt5_nname_offset, iatt5_nname_char, tid_nation1);
        }
        // -------- hash join build (opId: 18) --------
        if(active) {
            uint64_t hash18 = 0;
            if(active) {
                hash18 = 0;
                if(active) {
                    hash18 = hash ( (hash18 + ((uint64_t)att4_nnationk)));
                }
            }
            hashCountMulti ( jht18, 50, hash18);
        }
        loopVar += step;
    }

}

__global__ void krnl_nation1_ins(
    int* iatt4_nnationk, size_t* iatt5_nname_offset, char* iatt5_nname_char, multi_ht* jht18, jpayl18* jht18_payload, int* offs18) {
    int att4_nnationk;
    str_t att5_nname;

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
            att4_nnationk = iatt4_nnationk[tid_nation1];
            att5_nname = stringScan ( iatt5_nname_offset, iatt5_nname_char, tid_nation1);
        }
        // -------- hash join build (opId: 18) --------
        if(active) {
            uint64_t hash18 = 0;
            if(active) {
                hash18 = 0;
                if(active) {
                    hash18 = hash ( (hash18 + ((uint64_t)att4_nnationk)));
                }
            }
            jpayl18 payl;
            payl.att4_nnationk = att4_nnationk;
            payl.att5_nname = att5_nname;
            hashInsertMulti ( jht18, jht18_payload, offs18, 50, hash18, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_region2(
    int* iatt8_rregionk, size_t* iatt9_rname_offset, char* iatt9_rname_char, unique_ht<jpayl5>* jht5) {
    int att8_rregionk;
    str_t att9_rname;
    str_t c1 = stringConstant ( "AMERICA", 7);

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
            att8_rregionk = iatt8_rregionk[tid_region1];
            att9_rname = stringScan ( iatt9_rname_offset, iatt9_rname_char, tid_region1);
        }
        // -------- selection (opId: 3) --------
        if(active) {
            active = stringEquals ( att9_rname, c1);
        }
        // -------- hash join build (opId: 5) --------
        if(active) {
            jpayl5 payl5;
            payl5.att8_rregionk = att8_rregionk;
            uint64_t hash5;
            hash5 = 0;
            if(active) {
                hash5 = hash ( (hash5 + ((uint64_t)att8_rregionk)));
            }
            hashBuildUnique ( jht5, 10, hash5, &(payl5));
        }
        loopVar += step;
    }

}

__global__ void krnl_nation24(
    int* iatt11_nnationk, int* iatt13_nregionk, unique_ht<jpayl5>* jht5, unique_ht<jpayl15>* jht15) {
    int att11_nnationk;
    int att13_nregionk;
    int att8_rregionk;

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
            att11_nnationk = iatt11_nnationk[tid_nation2];
            att13_nregionk = iatt13_nregionk[tid_nation2];
        }
        // -------- hash join probe (opId: 5) --------
        uint64_t hash5 = 0;
        if(active) {
            hash5 = 0;
            if(active) {
                hash5 = hash ( (hash5 + ((uint64_t)att13_nregionk)));
            }
        }
        jpayl5* probepayl5;
        int numLookups5 = 0;
        if(active) {
            active = hashProbeUnique ( jht5, 10, hash5, numLookups5, &(probepayl5));
        }
        int bucketFound5 = 0;
        int probeActive5 = active;
        while((probeActive5 && !(bucketFound5))) {
            jpayl5 jprobepayl5 = *(probepayl5);
            att8_rregionk = jprobepayl5.att8_rregionk;
            bucketFound5 = 1;
            bucketFound5 &= ((att8_rregionk == att13_nregionk));
            if(!(bucketFound5)) {
                probeActive5 = hashProbeUnique ( jht5, 10, hash5, numLookups5, &(probepayl5));
            }
        }
        active = bucketFound5;
        // -------- hash join build (opId: 15) --------
        if(active) {
            jpayl15 payl15;
            payl15.att11_nnationk = att11_nnationk;
            uint64_t hash15;
            hash15 = 0;
            if(active) {
                hash15 = hash ( (hash15 + ((uint64_t)att11_nnationk)));
            }
            hashBuildUnique ( jht15, 50, hash15, &(payl15));
        }
        loopVar += step;
    }

}

__global__ void krnl_part6(
    int* iatt15_ppartkey, size_t* iatt19_ptype_offset, char* iatt19_ptype_char, unique_ht<jpayl9>* jht9) {
    int att15_ppartkey;
    str_t att19_ptype;
    str_t c2 = stringConstant ( "ECONOMY ANODIZED STEEL", 22);

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
            att15_ppartkey = iatt15_ppartkey[tid_part1];
            att19_ptype = stringScan ( iatt19_ptype_offset, iatt19_ptype_char, tid_part1);
        }
        // -------- selection (opId: 7) --------
        if(active) {
            active = stringEquals ( att19_ptype, c2);
        }
        // -------- hash join build (opId: 9) --------
        if(active) {
            jpayl9 payl9;
            payl9.att15_ppartkey = att15_ppartkey;
            uint64_t hash9;
            hash9 = 0;
            if(active) {
                hash9 = hash ( (hash9 + ((uint64_t)att15_ppartkey)));
            }
            hashBuildUnique ( jht9, 400000, hash9, &(payl9));
        }
        loopVar += step;
    }

}

__global__ void krnl_lineitem8(
    int* iatt24_lorderke, int* iatt25_lpartkey, int* iatt26_lsuppkey, float* iatt29_lextende, float* iatt30_ldiscoun, unique_ht<jpayl9>* jht9, multi_ht* jht12, jpayl12* jht12_payload) {
    int att24_lorderke;
    int att25_lpartkey;
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
    int att15_ppartkey;

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
            att24_lorderke = iatt24_lorderke[tid_lineitem1];
            att25_lpartkey = iatt25_lpartkey[tid_lineitem1];
            att26_lsuppkey = iatt26_lsuppkey[tid_lineitem1];
            att29_lextende = iatt29_lextende[tid_lineitem1];
            att30_ldiscoun = iatt30_ldiscoun[tid_lineitem1];
        }
        // -------- hash join probe (opId: 9) --------
        uint64_t hash9 = 0;
        if(active) {
            hash9 = 0;
            if(active) {
                hash9 = hash ( (hash9 + ((uint64_t)att25_lpartkey)));
            }
        }
        jpayl9* probepayl9;
        int numLookups9 = 0;
        if(active) {
            active = hashProbeUnique ( jht9, 400000, hash9, numLookups9, &(probepayl9));
        }
        int bucketFound9 = 0;
        int probeActive9 = active;
        while((probeActive9 && !(bucketFound9))) {
            jpayl9 jprobepayl9 = *(probepayl9);
            att15_ppartkey = jprobepayl9.att15_ppartkey;
            bucketFound9 = 1;
            bucketFound9 &= ((att15_ppartkey == att25_lpartkey));
            if(!(bucketFound9)) {
                probeActive9 = hashProbeUnique ( jht9, 400000, hash9, numLookups9, &(probepayl9));
            }
        }
        active = bucketFound9;
        // -------- hash join build (opId: 12) --------
        if(active) {
            uint64_t hash12 = 0;
            if(active) {
                hash12 = 0;
                if(active) {
                    hash12 = hash ( (hash12 + ((uint64_t)att24_lorderke)));
                }
            }
            hashCountMulti ( jht12, 120024, hash12);
        }
        loopVar += step;
    }

}

__global__ void krnl_lineitem8_ins(
    int* iatt24_lorderke, int* iatt25_lpartkey, int* iatt26_lsuppkey, float* iatt29_lextende, float* iatt30_ldiscoun, unique_ht<jpayl9>* jht9, multi_ht* jht12, jpayl12* jht12_payload, int* offs12) {
    int att24_lorderke;
    int att25_lpartkey;
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
    int att15_ppartkey;

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
            att24_lorderke = iatt24_lorderke[tid_lineitem1];
            att25_lpartkey = iatt25_lpartkey[tid_lineitem1];
            att26_lsuppkey = iatt26_lsuppkey[tid_lineitem1];
            att29_lextende = iatt29_lextende[tid_lineitem1];
            att30_ldiscoun = iatt30_ldiscoun[tid_lineitem1];
        }
        // -------- hash join probe (opId: 9) --------
        uint64_t hash9 = 0;
        if(active) {
            hash9 = 0;
            if(active) {
                hash9 = hash ( (hash9 + ((uint64_t)att25_lpartkey)));
            }
        }
        jpayl9* probepayl9;
        int numLookups9 = 0;
        if(active) {
            active = hashProbeUnique ( jht9, 400000, hash9, numLookups9, &(probepayl9));
        }
        int bucketFound9 = 0;
        int probeActive9 = active;
        while((probeActive9 && !(bucketFound9))) {
            jpayl9 jprobepayl9 = *(probepayl9);
            att15_ppartkey = jprobepayl9.att15_ppartkey;
            bucketFound9 = 1;
            bucketFound9 &= ((att15_ppartkey == att25_lpartkey));
            if(!(bucketFound9)) {
                probeActive9 = hashProbeUnique ( jht9, 400000, hash9, numLookups9, &(probepayl9));
            }
        }
        active = bucketFound9;
        // -------- hash join build (opId: 12) --------
        if(active) {
            uint64_t hash12 = 0;
            if(active) {
                hash12 = 0;
                if(active) {
                    hash12 = hash ( (hash12 + ((uint64_t)att24_lorderke)));
                }
            }
            jpayl12 payl;
            payl.att24_lorderke = att24_lorderke;
            payl.att26_lsuppkey = att26_lsuppkey;
            payl.att29_lextende = att29_lextende;
            payl.att30_ldiscoun = att30_ldiscoun;
            hashInsertMulti ( jht12, jht12_payload, offs12, 120024, hash12, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_orders10(
    int* iatt40_oorderke, int* iatt41_ocustkey, unsigned* iatt44_oorderda, multi_ht* jht12, jpayl12* jht12_payload, multi_ht* jht14, jpayl14* jht14_payload) {
    int att40_oorderke;
    int att41_ocustkey;
    unsigned att44_oorderda;
    unsigned warplane = (threadIdx.x % 32);
    int att24_lorderke;
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;

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
            att40_oorderke = iatt40_oorderke[tid_orders1];
            att41_ocustkey = iatt41_ocustkey[tid_orders1];
            att44_oorderda = iatt44_oorderda[tid_orders1];
        }
        // -------- selection (opId: 11) --------
        if(active) {
            active = ((att44_oorderda >= 19950101) && (att44_oorderda <= 19961231));
        }
        // -------- hash join probe (opId: 12) --------
        // -------- multiprobe multi broadcast (opId: 12) --------
        int matchEnd12 = 0;
        int matchEndBuf12 = 0;
        int matchOffset12 = 0;
        int matchOffsetBuf12 = 0;
        int probeActive12 = active;
        int att40_oorderke_bcbuf12;
        int att41_ocustkey_bcbuf12;
        unsigned att44_oorderda_bcbuf12;
        uint64_t hash12 = 0;
        if(probeActive12) {
            hash12 = 0;
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att40_oorderke)));
            }
            probeActive12 = hashProbeMulti ( jht12, 120024, hash12, matchOffsetBuf12, matchEndBuf12);
        }
        unsigned activeProbes12 = __ballot_sync(ALL_LANES,probeActive12);
        int num12 = 0;
        num12 = (matchEndBuf12 - matchOffsetBuf12);
        unsigned wideProbes12 = __ballot_sync(ALL_LANES,(num12 >= 32));
        att40_oorderke_bcbuf12 = att40_oorderke;
        att41_ocustkey_bcbuf12 = att41_ocustkey;
        att44_oorderda_bcbuf12 = att44_oorderda;
        while((activeProbes12 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes12)) {
                if((wideProbes12 > 0)) {
                    tupleLane = (__ffs(wideProbes12) - 1);
                    wideProbes12 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes12) - 1);
                }
                num = __shfl_sync(ALL_LANES,num12,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset12 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes12 -= (1 << tupleLane);
            }
            matchOffset12 += __shfl_sync(ALL_LANES,matchOffsetBuf12,broadcastLane);
            matchEnd12 = __shfl_sync(ALL_LANES,matchEndBuf12,broadcastLane);
            att40_oorderke = __shfl_sync(ALL_LANES,att40_oorderke_bcbuf12,broadcastLane);
            att41_ocustkey = __shfl_sync(ALL_LANES,att41_ocustkey_bcbuf12,broadcastLane);
            att44_oorderda = __shfl_sync(ALL_LANES,att44_oorderda_bcbuf12,broadcastLane);
            probeActive12 = (matchOffset12 < matchEnd12);
            while(__any_sync(ALL_LANES,probeActive12)) {
                active = probeActive12;
                active = 0;
                jpayl12 payl;
                if(probeActive12) {
                    payl = jht12_payload[matchOffset12];
                    att24_lorderke = payl.att24_lorderke;
                    att26_lsuppkey = payl.att26_lsuppkey;
                    att29_lextende = payl.att29_lextende;
                    att30_ldiscoun = payl.att30_ldiscoun;
                    active = 1;
                    active &= ((att24_lorderke == att40_oorderke));
                    matchOffset12 += 32;
                    probeActive12 &= ((matchOffset12 < matchEnd12));
                }
                // -------- hash join build (opId: 14) --------
                if(active) {
                    uint64_t hash14 = 0;
                    if(active) {
                        hash14 = 0;
                        if(active) {
                            hash14 = hash ( (hash14 + ((uint64_t)att41_ocustkey)));
                        }
                    }
                    hashCountMulti ( jht14, 300000, hash14);
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_orders10_ins(
    int* iatt40_oorderke, int* iatt41_ocustkey, unsigned* iatt44_oorderda, multi_ht* jht12, jpayl12* jht12_payload, multi_ht* jht14, jpayl14* jht14_payload, int* offs14) {
    int att40_oorderke;
    int att41_ocustkey;
    unsigned att44_oorderda;
    unsigned warplane = (threadIdx.x % 32);
    int att24_lorderke;
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;

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
            att40_oorderke = iatt40_oorderke[tid_orders1];
            att41_ocustkey = iatt41_ocustkey[tid_orders1];
            att44_oorderda = iatt44_oorderda[tid_orders1];
        }
        // -------- selection (opId: 11) --------
        if(active) {
            active = ((att44_oorderda >= 19950101) && (att44_oorderda <= 19961231));
        }
        // -------- hash join probe (opId: 12) --------
        // -------- multiprobe multi broadcast (opId: 12) --------
        int matchEnd12 = 0;
        int matchEndBuf12 = 0;
        int matchOffset12 = 0;
        int matchOffsetBuf12 = 0;
        int probeActive12 = active;
        int att40_oorderke_bcbuf12;
        int att41_ocustkey_bcbuf12;
        unsigned att44_oorderda_bcbuf12;
        uint64_t hash12 = 0;
        if(probeActive12) {
            hash12 = 0;
            if(active) {
                hash12 = hash ( (hash12 + ((uint64_t)att40_oorderke)));
            }
            probeActive12 = hashProbeMulti ( jht12, 120024, hash12, matchOffsetBuf12, matchEndBuf12);
        }
        unsigned activeProbes12 = __ballot_sync(ALL_LANES,probeActive12);
        int num12 = 0;
        num12 = (matchEndBuf12 - matchOffsetBuf12);
        unsigned wideProbes12 = __ballot_sync(ALL_LANES,(num12 >= 32));
        att40_oorderke_bcbuf12 = att40_oorderke;
        att41_ocustkey_bcbuf12 = att41_ocustkey;
        att44_oorderda_bcbuf12 = att44_oorderda;
        while((activeProbes12 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes12)) {
                if((wideProbes12 > 0)) {
                    tupleLane = (__ffs(wideProbes12) - 1);
                    wideProbes12 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes12) - 1);
                }
                num = __shfl_sync(ALL_LANES,num12,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset12 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes12 -= (1 << tupleLane);
            }
            matchOffset12 += __shfl_sync(ALL_LANES,matchOffsetBuf12,broadcastLane);
            matchEnd12 = __shfl_sync(ALL_LANES,matchEndBuf12,broadcastLane);
            att40_oorderke = __shfl_sync(ALL_LANES,att40_oorderke_bcbuf12,broadcastLane);
            att41_ocustkey = __shfl_sync(ALL_LANES,att41_ocustkey_bcbuf12,broadcastLane);
            att44_oorderda = __shfl_sync(ALL_LANES,att44_oorderda_bcbuf12,broadcastLane);
            probeActive12 = (matchOffset12 < matchEnd12);
            while(__any_sync(ALL_LANES,probeActive12)) {
                active = probeActive12;
                active = 0;
                jpayl12 payl;
                if(probeActive12) {
                    payl = jht12_payload[matchOffset12];
                    att24_lorderke = payl.att24_lorderke;
                    att26_lsuppkey = payl.att26_lsuppkey;
                    att29_lextende = payl.att29_lextende;
                    att30_ldiscoun = payl.att30_ldiscoun;
                    active = 1;
                    active &= ((att24_lorderke == att40_oorderke));
                    matchOffset12 += 32;
                    probeActive12 &= ((matchOffset12 < matchEnd12));
                }
                // -------- hash join build (opId: 14) --------
                if(active) {
                    uint64_t hash14 = 0;
                    if(active) {
                        hash14 = 0;
                        if(active) {
                            hash14 = hash ( (hash14 + ((uint64_t)att41_ocustkey)));
                        }
                    }
                    jpayl14 payl;
                    payl.att26_lsuppkey = att26_lsuppkey;
                    payl.att29_lextende = att29_lextende;
                    payl.att30_ldiscoun = att30_ldiscoun;
                    payl.att41_ocustkey = att41_ocustkey;
                    payl.att44_oorderda = att44_oorderda;
                    hashInsertMulti ( jht14, jht14_payload, offs14, 300000, hash14, &(payl));
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_customer13(
    int* iatt49_ccustkey, int* iatt52_cnationk, multi_ht* jht14, jpayl14* jht14_payload, unique_ht<jpayl15>* jht15, multi_ht* jht17, jpayl17* jht17_payload) {
    int att49_ccustkey;
    int att52_cnationk;
    unsigned warplane = (threadIdx.x % 32);
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
    int att41_ocustkey;
    unsigned att44_oorderda;
    int att11_nnationk;

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
            att49_ccustkey = iatt49_ccustkey[tid_customer1];
            att52_cnationk = iatt52_cnationk[tid_customer1];
        }
        // -------- hash join probe (opId: 14) --------
        // -------- multiprobe multi broadcast (opId: 14) --------
        int matchEnd14 = 0;
        int matchEndBuf14 = 0;
        int matchOffset14 = 0;
        int matchOffsetBuf14 = 0;
        int probeActive14 = active;
        int att49_ccustkey_bcbuf14;
        int att52_cnationk_bcbuf14;
        uint64_t hash14 = 0;
        if(probeActive14) {
            hash14 = 0;
            if(active) {
                hash14 = hash ( (hash14 + ((uint64_t)att49_ccustkey)));
            }
            probeActive14 = hashProbeMulti ( jht14, 300000, hash14, matchOffsetBuf14, matchEndBuf14);
        }
        unsigned activeProbes14 = __ballot_sync(ALL_LANES,probeActive14);
        int num14 = 0;
        num14 = (matchEndBuf14 - matchOffsetBuf14);
        unsigned wideProbes14 = __ballot_sync(ALL_LANES,(num14 >= 32));
        att49_ccustkey_bcbuf14 = att49_ccustkey;
        att52_cnationk_bcbuf14 = att52_cnationk;
        while((activeProbes14 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes14)) {
                if((wideProbes14 > 0)) {
                    tupleLane = (__ffs(wideProbes14) - 1);
                    wideProbes14 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes14) - 1);
                }
                num = __shfl_sync(ALL_LANES,num14,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset14 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes14 -= (1 << tupleLane);
            }
            matchOffset14 += __shfl_sync(ALL_LANES,matchOffsetBuf14,broadcastLane);
            matchEnd14 = __shfl_sync(ALL_LANES,matchEndBuf14,broadcastLane);
            att49_ccustkey = __shfl_sync(ALL_LANES,att49_ccustkey_bcbuf14,broadcastLane);
            att52_cnationk = __shfl_sync(ALL_LANES,att52_cnationk_bcbuf14,broadcastLane);
            probeActive14 = (matchOffset14 < matchEnd14);
            while(__any_sync(ALL_LANES,probeActive14)) {
                active = probeActive14;
                active = 0;
                jpayl14 payl;
                if(probeActive14) {
                    payl = jht14_payload[matchOffset14];
                    att26_lsuppkey = payl.att26_lsuppkey;
                    att29_lextende = payl.att29_lextende;
                    att30_ldiscoun = payl.att30_ldiscoun;
                    att41_ocustkey = payl.att41_ocustkey;
                    att44_oorderda = payl.att44_oorderda;
                    active = 1;
                    active &= ((att41_ocustkey == att49_ccustkey));
                    matchOffset14 += 32;
                    probeActive14 &= ((matchOffset14 < matchEnd14));
                }
                // -------- hash join probe (opId: 15) --------
                uint64_t hash15 = 0;
                if(active) {
                    hash15 = 0;
                    if(active) {
                        hash15 = hash ( (hash15 + ((uint64_t)att52_cnationk)));
                    }
                }
                jpayl15* probepayl15;
                int numLookups15 = 0;
                if(active) {
                    active = hashProbeUnique ( jht15, 50, hash15, numLookups15, &(probepayl15));
                }
                int bucketFound15 = 0;
                int probeActive15 = active;
                while((probeActive15 && !(bucketFound15))) {
                    jpayl15 jprobepayl15 = *(probepayl15);
                    att11_nnationk = jprobepayl15.att11_nnationk;
                    bucketFound15 = 1;
                    bucketFound15 &= ((att11_nnationk == att52_cnationk));
                    if(!(bucketFound15)) {
                        probeActive15 = hashProbeUnique ( jht15, 50, hash15, numLookups15, &(probepayl15));
                    }
                }
                active = bucketFound15;
                // -------- hash join build (opId: 17) --------
                if(active) {
                    uint64_t hash17 = 0;
                    if(active) {
                        hash17 = 0;
                        if(active) {
                            hash17 = hash ( (hash17 + ((uint64_t)att26_lsuppkey)));
                        }
                    }
                    hashCountMulti ( jht17, 75000, hash17);
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_customer13_ins(
    int* iatt49_ccustkey, int* iatt52_cnationk, multi_ht* jht14, jpayl14* jht14_payload, unique_ht<jpayl15>* jht15, multi_ht* jht17, jpayl17* jht17_payload, int* offs17) {
    int att49_ccustkey;
    int att52_cnationk;
    unsigned warplane = (threadIdx.x % 32);
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
    int att41_ocustkey;
    unsigned att44_oorderda;
    int att11_nnationk;

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
            att49_ccustkey = iatt49_ccustkey[tid_customer1];
            att52_cnationk = iatt52_cnationk[tid_customer1];
        }
        // -------- hash join probe (opId: 14) --------
        // -------- multiprobe multi broadcast (opId: 14) --------
        int matchEnd14 = 0;
        int matchEndBuf14 = 0;
        int matchOffset14 = 0;
        int matchOffsetBuf14 = 0;
        int probeActive14 = active;
        int att49_ccustkey_bcbuf14;
        int att52_cnationk_bcbuf14;
        uint64_t hash14 = 0;
        if(probeActive14) {
            hash14 = 0;
            if(active) {
                hash14 = hash ( (hash14 + ((uint64_t)att49_ccustkey)));
            }
            probeActive14 = hashProbeMulti ( jht14, 300000, hash14, matchOffsetBuf14, matchEndBuf14);
        }
        unsigned activeProbes14 = __ballot_sync(ALL_LANES,probeActive14);
        int num14 = 0;
        num14 = (matchEndBuf14 - matchOffsetBuf14);
        unsigned wideProbes14 = __ballot_sync(ALL_LANES,(num14 >= 32));
        att49_ccustkey_bcbuf14 = att49_ccustkey;
        att52_cnationk_bcbuf14 = att52_cnationk;
        while((activeProbes14 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes14)) {
                if((wideProbes14 > 0)) {
                    tupleLane = (__ffs(wideProbes14) - 1);
                    wideProbes14 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes14) - 1);
                }
                num = __shfl_sync(ALL_LANES,num14,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset14 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes14 -= (1 << tupleLane);
            }
            matchOffset14 += __shfl_sync(ALL_LANES,matchOffsetBuf14,broadcastLane);
            matchEnd14 = __shfl_sync(ALL_LANES,matchEndBuf14,broadcastLane);
            att49_ccustkey = __shfl_sync(ALL_LANES,att49_ccustkey_bcbuf14,broadcastLane);
            att52_cnationk = __shfl_sync(ALL_LANES,att52_cnationk_bcbuf14,broadcastLane);
            probeActive14 = (matchOffset14 < matchEnd14);
            while(__any_sync(ALL_LANES,probeActive14)) {
                active = probeActive14;
                active = 0;
                jpayl14 payl;
                if(probeActive14) {
                    payl = jht14_payload[matchOffset14];
                    att26_lsuppkey = payl.att26_lsuppkey;
                    att29_lextende = payl.att29_lextende;
                    att30_ldiscoun = payl.att30_ldiscoun;
                    att41_ocustkey = payl.att41_ocustkey;
                    att44_oorderda = payl.att44_oorderda;
                    active = 1;
                    active &= ((att41_ocustkey == att49_ccustkey));
                    matchOffset14 += 32;
                    probeActive14 &= ((matchOffset14 < matchEnd14));
                }
                // -------- hash join probe (opId: 15) --------
                uint64_t hash15 = 0;
                if(active) {
                    hash15 = 0;
                    if(active) {
                        hash15 = hash ( (hash15 + ((uint64_t)att52_cnationk)));
                    }
                }
                jpayl15* probepayl15;
                int numLookups15 = 0;
                if(active) {
                    active = hashProbeUnique ( jht15, 50, hash15, numLookups15, &(probepayl15));
                }
                int bucketFound15 = 0;
                int probeActive15 = active;
                while((probeActive15 && !(bucketFound15))) {
                    jpayl15 jprobepayl15 = *(probepayl15);
                    att11_nnationk = jprobepayl15.att11_nnationk;
                    bucketFound15 = 1;
                    bucketFound15 &= ((att11_nnationk == att52_cnationk));
                    if(!(bucketFound15)) {
                        probeActive15 = hashProbeUnique ( jht15, 50, hash15, numLookups15, &(probepayl15));
                    }
                }
                active = bucketFound15;
                // -------- hash join build (opId: 17) --------
                if(active) {
                    uint64_t hash17 = 0;
                    if(active) {
                        hash17 = 0;
                        if(active) {
                            hash17 = hash ( (hash17 + ((uint64_t)att26_lsuppkey)));
                        }
                    }
                    jpayl17 payl;
                    payl.att26_lsuppkey = att26_lsuppkey;
                    payl.att29_lextende = att29_lextende;
                    payl.att30_ldiscoun = att30_ldiscoun;
                    payl.att44_oorderda = att44_oorderda;
                    hashInsertMulti ( jht17, jht17_payload, offs17, 75000, hash17, &(payl));
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_supplier16(
    int* iatt57_ssuppkey, int* iatt60_snationk, multi_ht* jht17, jpayl17* jht17_payload, multi_ht* jht18, jpayl18* jht18_payload, agg_ht<apayl22>* aht22, float* agg1, float* agg2, int* agg3) {
    int att57_ssuppkey;
    int att60_snationk;
    unsigned warplane = (threadIdx.x % 32);
    int att26_lsuppkey;
    float att29_lextende;
    float att30_ldiscoun;
    unsigned att44_oorderda;
    int att4_nnationk;
    str_t att5_nname;
    unsigned att64_oyear;
    float att65_volume;
    float att66_casevolu;
    str_t c3 = stringConstant ( "BRAZIL", 6);

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
            att57_ssuppkey = iatt57_ssuppkey[tid_supplier1];
            att60_snationk = iatt60_snationk[tid_supplier1];
        }
        // -------- hash join probe (opId: 17) --------
        // -------- multiprobe multi broadcast (opId: 17) --------
        int matchEnd17 = 0;
        int matchEndBuf17 = 0;
        int matchOffset17 = 0;
        int matchOffsetBuf17 = 0;
        int probeActive17 = active;
        int att57_ssuppkey_bcbuf17;
        int att60_snationk_bcbuf17;
        uint64_t hash17 = 0;
        if(probeActive17) {
            hash17 = 0;
            if(active) {
                hash17 = hash ( (hash17 + ((uint64_t)att57_ssuppkey)));
            }
            probeActive17 = hashProbeMulti ( jht17, 75000, hash17, matchOffsetBuf17, matchEndBuf17);
        }
        unsigned activeProbes17 = __ballot_sync(ALL_LANES,probeActive17);
        int num17 = 0;
        num17 = (matchEndBuf17 - matchOffsetBuf17);
        unsigned wideProbes17 = __ballot_sync(ALL_LANES,(num17 >= 32));
        att57_ssuppkey_bcbuf17 = att57_ssuppkey;
        att60_snationk_bcbuf17 = att60_snationk;
        while((activeProbes17 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes17)) {
                if((wideProbes17 > 0)) {
                    tupleLane = (__ffs(wideProbes17) - 1);
                    wideProbes17 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes17) - 1);
                }
                num = __shfl_sync(ALL_LANES,num17,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset17 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes17 -= (1 << tupleLane);
            }
            matchOffset17 += __shfl_sync(ALL_LANES,matchOffsetBuf17,broadcastLane);
            matchEnd17 = __shfl_sync(ALL_LANES,matchEndBuf17,broadcastLane);
            att57_ssuppkey = __shfl_sync(ALL_LANES,att57_ssuppkey_bcbuf17,broadcastLane);
            att60_snationk = __shfl_sync(ALL_LANES,att60_snationk_bcbuf17,broadcastLane);
            probeActive17 = (matchOffset17 < matchEnd17);
            while(__any_sync(ALL_LANES,probeActive17)) {
                active = probeActive17;
                active = 0;
                jpayl17 payl;
                if(probeActive17) {
                    payl = jht17_payload[matchOffset17];
                    att26_lsuppkey = payl.att26_lsuppkey;
                    att29_lextende = payl.att29_lextende;
                    att30_ldiscoun = payl.att30_ldiscoun;
                    att44_oorderda = payl.att44_oorderda;
                    active = 1;
                    active &= ((att26_lsuppkey == att57_ssuppkey));
                    matchOffset17 += 32;
                    probeActive17 &= ((matchOffset17 < matchEnd17));
                }
                // -------- hash join probe (opId: 18) --------
                // -------- multiprobe multi broadcast (opId: 18) --------
                int matchEnd18 = 0;
                int matchEndBuf18 = 0;
                int matchOffset18 = 0;
                int matchOffsetBuf18 = 0;
                int probeActive18 = active;
                float att29_lextende_bcbuf18;
                float att30_ldiscoun_bcbuf18;
                unsigned att44_oorderda_bcbuf18;
                int att60_snationk_bcbuf18;
                uint64_t hash18 = 0;
                if(probeActive18) {
                    hash18 = 0;
                    if(active) {
                        hash18 = hash ( (hash18 + ((uint64_t)att60_snationk)));
                    }
                    probeActive18 = hashProbeMulti ( jht18, 50, hash18, matchOffsetBuf18, matchEndBuf18);
                }
                unsigned activeProbes18 = __ballot_sync(ALL_LANES,probeActive18);
                int num18 = 0;
                num18 = (matchEndBuf18 - matchOffsetBuf18);
                unsigned wideProbes18 = __ballot_sync(ALL_LANES,(num18 >= 32));
                att29_lextende_bcbuf18 = att29_lextende;
                att30_ldiscoun_bcbuf18 = att30_ldiscoun;
                att44_oorderda_bcbuf18 = att44_oorderda;
                att60_snationk_bcbuf18 = att60_snationk;
                while((activeProbes18 > 0)) {
                    unsigned tupleLane;
                    unsigned broadcastLane;
                    int numFilled = 0;
                    int num = 0;
                    while(((numFilled < 32) && activeProbes18)) {
                        if((wideProbes18 > 0)) {
                            tupleLane = (__ffs(wideProbes18) - 1);
                            wideProbes18 -= (1 << tupleLane);
                        }
                        else {
                            tupleLane = (__ffs(activeProbes18) - 1);
                        }
                        num = __shfl_sync(ALL_LANES,num18,tupleLane);
                        if((numFilled && ((numFilled + num) > 32))) {
                            break;
                        }
                        if((warplane >= numFilled)) {
                            broadcastLane = tupleLane;
                            matchOffset18 = (warplane - numFilled);
                        }
                        numFilled += num;
                        activeProbes18 -= (1 << tupleLane);
                    }
                    matchOffset18 += __shfl_sync(ALL_LANES,matchOffsetBuf18,broadcastLane);
                    matchEnd18 = __shfl_sync(ALL_LANES,matchEndBuf18,broadcastLane);
                    att29_lextende = __shfl_sync(ALL_LANES,att29_lextende_bcbuf18,broadcastLane);
                    att30_ldiscoun = __shfl_sync(ALL_LANES,att30_ldiscoun_bcbuf18,broadcastLane);
                    att44_oorderda = __shfl_sync(ALL_LANES,att44_oorderda_bcbuf18,broadcastLane);
                    att60_snationk = __shfl_sync(ALL_LANES,att60_snationk_bcbuf18,broadcastLane);
                    probeActive18 = (matchOffset18 < matchEnd18);
                    while(__any_sync(ALL_LANES,probeActive18)) {
                        active = probeActive18;
                        active = 0;
                        jpayl18 payl;
                        if(probeActive18) {
                            payl = jht18_payload[matchOffset18];
                            att4_nnationk = payl.att4_nnationk;
                            att5_nname = payl.att5_nname;
                            active = 1;
                            active &= ((att4_nnationk == att60_snationk));
                            matchOffset18 += 32;
                            probeActive18 &= ((matchOffset18 < matchEnd18));
                        }
                        // -------- map (opId: 19) --------
                        if(active) {
                            att64_oyear = (att44_oorderda / 10000);
                        }
                        // -------- map (opId: 20) --------
                        if(active) {
                            att65_volume = (att29_lextende * ((float)1.0f - att30_ldiscoun));
                        }
                        // -------- map (opId: 21) --------
                        if(active) {
                            float casevar1138;
                            if(stringEquals ( att5_nname, c3)) {
                                casevar1138 = att65_volume;
                            }
                            else {
                                casevar1138 = (float)0;
                            }
                            att66_casevolu = casevar1138;
                        }
                        // -------- aggregation (opId: 22) --------
                        int bucket = 0;
                        if(active) {
                            uint64_t hash22 = 0;
                            hash22 = 0;
                            if(active) {
                                hash22 = hash ( (hash22 + ((uint64_t)att64_oyear)));
                            }
                            apayl22 payl;
                            payl.att64_oyear = att64_oyear;
                            int bucketFound = 0;
                            int numLookups = 0;
                            while(!(bucketFound)) {
                                bucket = hashAggregateGetBucket ( aht22, 75000, hash22, numLookups, &(payl));
                                apayl22 probepayl = aht22[bucket].payload;
                                bucketFound = 1;
                                bucketFound &= ((payl.att64_oyear == probepayl.att64_oyear));
                            }
                        }
                        if(active) {
                            atomicAdd(&(agg1[bucket]), ((float)att66_casevolu));
                            atomicAdd(&(agg2[bucket]), ((float)att65_volume));
                            atomicAdd(&(agg3[bucket]), ((int)1));
                        }
                    }
                }
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation22(
    agg_ht<apayl22>* aht22, float* agg1, float* agg2, int* agg3, int* nout_result, unsigned* oatt64_oyear, float* oatt67_mktshare, int* oatt3_salesnum) {
    unsigned att64_oyear;
    float att1_sumvolum;
    float att2_sumvolum;
    int att3_salesnum;
    float att67_mktshare;
    unsigned warplane = (threadIdx.x % 32);
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

    int tid_aggregation22 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation22 = loopVar;
        active = (loopVar < 75000);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 22) --------
        if(active) {
            active &= ((aht22[tid_aggregation22].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl22 payl = aht22[tid_aggregation22].payload;
            att64_oyear = payl.att64_oyear;
        }
        if(active) {
            att1_sumvolum = agg1[tid_aggregation22];
            att2_sumvolum = agg2[tid_aggregation22];
            att3_salesnum = agg3[tid_aggregation22];
        }
        // -------- map (opId: 23) --------
        if(active) {
            att67_mktshare = (att1_sumvolum / att2_sumvolum);
        }
        // -------- projection (no code) (opId: 24) --------
        // -------- materialize (opId: 25) --------
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
            oatt64_oyear[wp] = att64_oyear;
            oatt67_mktshare[wp] = att67_mktshare;
            oatt3_salesnum[wp] = att3_salesnum;
        }
        loopVar += step;
    }

}

int main() {
    int* iatt4_nnationk;
    iatt4_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    size_t* iatt5_nname_offset;
    iatt5_nname_offset = ( size_t*) map_memory_file ( "mmdb/nation_n_name_offset" );
    char* iatt5_nname_char;
    iatt5_nname_char = ( char*) map_memory_file ( "mmdb/nation_n_name_char" );
    int* iatt8_rregionk;
    iatt8_rregionk = ( int*) map_memory_file ( "mmdb/region_r_regionkey" );
    size_t* iatt9_rname_offset;
    iatt9_rname_offset = ( size_t*) map_memory_file ( "mmdb/region_r_name_offset" );
    char* iatt9_rname_char;
    iatt9_rname_char = ( char*) map_memory_file ( "mmdb/region_r_name_char" );
    int* iatt11_nnationk;
    iatt11_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    int* iatt13_nregionk;
    iatt13_nregionk = ( int*) map_memory_file ( "mmdb/nation_n_regionkey" );
    int* iatt15_ppartkey;
    iatt15_ppartkey = ( int*) map_memory_file ( "mmdb/part_p_partkey" );
    size_t* iatt19_ptype_offset;
    iatt19_ptype_offset = ( size_t*) map_memory_file ( "mmdb/part_p_type_offset" );
    char* iatt19_ptype_char;
    iatt19_ptype_char = ( char*) map_memory_file ( "mmdb/part_p_type_char" );
    int* iatt24_lorderke;
    iatt24_lorderke = ( int*) map_memory_file ( "mmdb/lineitem_l_orderkey" );
    int* iatt25_lpartkey;
    iatt25_lpartkey = ( int*) map_memory_file ( "mmdb/lineitem_l_partkey" );
    int* iatt26_lsuppkey;
    iatt26_lsuppkey = ( int*) map_memory_file ( "mmdb/lineitem_l_suppkey" );
    float* iatt29_lextende;
    iatt29_lextende = ( float*) map_memory_file ( "mmdb/lineitem_l_extendedprice" );
    float* iatt30_ldiscoun;
    iatt30_ldiscoun = ( float*) map_memory_file ( "mmdb/lineitem_l_discount" );
    int* iatt40_oorderke;
    iatt40_oorderke = ( int*) map_memory_file ( "mmdb/orders_o_orderkey" );
    int* iatt41_ocustkey;
    iatt41_ocustkey = ( int*) map_memory_file ( "mmdb/orders_o_custkey" );
    unsigned* iatt44_oorderda;
    iatt44_oorderda = ( unsigned*) map_memory_file ( "mmdb/orders_o_orderdate" );
    int* iatt49_ccustkey;
    iatt49_ccustkey = ( int*) map_memory_file ( "mmdb/customer_c_custkey" );
    int* iatt52_cnationk;
    iatt52_cnationk = ( int*) map_memory_file ( "mmdb/customer_c_nationkey" );
    int* iatt57_ssuppkey;
    iatt57_ssuppkey = ( int*) map_memory_file ( "mmdb/supplier_s_suppkey" );
    int* iatt60_snationk;
    iatt60_snationk = ( int*) map_memory_file ( "mmdb/supplier_s_nationkey" );

    int nout_result;
    std::vector < unsigned > oatt64_oyear(37500);
    std::vector < float > oatt67_mktshare(37500);
    std::vector < int > oatt3_salesnum(37500);

    // wake up gpu
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in wake up gpu! " << cudaGetErrorString( err ) << std::endl;
            ERROR("wake up gpu")
        }
    }

    int* d_iatt4_nnationk;
    cudaMalloc((void**) &d_iatt4_nnationk, 25* sizeof(int) );
    size_t* d_iatt5_nname_offset;
    cudaMalloc((void**) &d_iatt5_nname_offset, (25 + 1)* sizeof(size_t) );
    char* d_iatt5_nname_char;
    cudaMalloc((void**) &d_iatt5_nname_char, 186* sizeof(char) );
    int* d_iatt8_rregionk;
    cudaMalloc((void**) &d_iatt8_rregionk, 5* sizeof(int) );
    size_t* d_iatt9_rname_offset;
    cudaMalloc((void**) &d_iatt9_rname_offset, (5 + 1)* sizeof(size_t) );
    char* d_iatt9_rname_char;
    cudaMalloc((void**) &d_iatt9_rname_char, 43* sizeof(char) );
    int* d_iatt11_nnationk;
    d_iatt11_nnationk = d_iatt4_nnationk;
    int* d_iatt13_nregionk;
    cudaMalloc((void**) &d_iatt13_nregionk, 25* sizeof(int) );
    int* d_iatt15_ppartkey;
    cudaMalloc((void**) &d_iatt15_ppartkey, 200000* sizeof(int) );
    size_t* d_iatt19_ptype_offset;
    cudaMalloc((void**) &d_iatt19_ptype_offset, (200000 + 1)* sizeof(size_t) );
    char* d_iatt19_ptype_char;
    cudaMalloc((void**) &d_iatt19_ptype_char, 4119955* sizeof(char) );
    int* d_iatt24_lorderke;
    cudaMalloc((void**) &d_iatt24_lorderke, 6001215* sizeof(int) );
    int* d_iatt25_lpartkey;
    cudaMalloc((void**) &d_iatt25_lpartkey, 6001215* sizeof(int) );
    int* d_iatt26_lsuppkey;
    cudaMalloc((void**) &d_iatt26_lsuppkey, 6001215* sizeof(int) );
    float* d_iatt29_lextende;
    cudaMalloc((void**) &d_iatt29_lextende, 6001215* sizeof(float) );
    float* d_iatt30_ldiscoun;
    cudaMalloc((void**) &d_iatt30_ldiscoun, 6001215* sizeof(float) );
    int* d_iatt40_oorderke;
    cudaMalloc((void**) &d_iatt40_oorderke, 1500000* sizeof(int) );
    int* d_iatt41_ocustkey;
    cudaMalloc((void**) &d_iatt41_ocustkey, 1500000* sizeof(int) );
    unsigned* d_iatt44_oorderda;
    cudaMalloc((void**) &d_iatt44_oorderda, 1500000* sizeof(unsigned) );
    int* d_iatt49_ccustkey;
    cudaMalloc((void**) &d_iatt49_ccustkey, 150000* sizeof(int) );
    int* d_iatt52_cnationk;
    cudaMalloc((void**) &d_iatt52_cnationk, 150000* sizeof(int) );
    int* d_iatt57_ssuppkey;
    cudaMalloc((void**) &d_iatt57_ssuppkey, 10000* sizeof(int) );
    int* d_iatt60_snationk;
    cudaMalloc((void**) &d_iatt60_snationk, 10000* sizeof(int) );
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    unsigned* d_oatt64_oyear;
    cudaMalloc((void**) &d_oatt64_oyear, 37500* sizeof(unsigned) );
    float* d_oatt67_mktshare;
    cudaMalloc((void**) &d_oatt67_mktshare, 37500* sizeof(float) );
    int* d_oatt3_salesnum;
    cudaMalloc((void**) &d_oatt3_salesnum, 37500* sizeof(int) );
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

    multi_ht* d_jht18;
    cudaMalloc((void**) &d_jht18, 50* sizeof(multi_ht) );
    jpayl18* d_jht18_payload;
    cudaMalloc((void**) &d_jht18_payload, 50* sizeof(jpayl18) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht18, 50);
    }
    int* d_offs18;
    cudaMalloc((void**) &d_offs18, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs18, 0, 1);
    }
    unique_ht<jpayl5>* d_jht5;
    cudaMalloc((void**) &d_jht5, 10* sizeof(unique_ht<jpayl5>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht5, 10);
    }
    unique_ht<jpayl15>* d_jht15;
    cudaMalloc((void**) &d_jht15, 50* sizeof(unique_ht<jpayl15>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht15, 50);
    }
    unique_ht<jpayl9>* d_jht9;
    cudaMalloc((void**) &d_jht9, 400000* sizeof(unique_ht<jpayl9>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht9, 400000);
    }
    multi_ht* d_jht12;
    cudaMalloc((void**) &d_jht12, 120024* sizeof(multi_ht) );
    jpayl12* d_jht12_payload;
    cudaMalloc((void**) &d_jht12_payload, 120024* sizeof(jpayl12) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht12, 120024);
    }
    int* d_offs12;
    cudaMalloc((void**) &d_offs12, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs12, 0, 1);
    }
    multi_ht* d_jht14;
    cudaMalloc((void**) &d_jht14, 300000* sizeof(multi_ht) );
    jpayl14* d_jht14_payload;
    cudaMalloc((void**) &d_jht14_payload, 300000* sizeof(jpayl14) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht14, 300000);
    }
    int* d_offs14;
    cudaMalloc((void**) &d_offs14, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs14, 0, 1);
    }
    multi_ht* d_jht17;
    cudaMalloc((void**) &d_jht17, 75000* sizeof(multi_ht) );
    jpayl17* d_jht17_payload;
    cudaMalloc((void**) &d_jht17_payload, 75000* sizeof(jpayl17) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht17, 75000);
    }
    int* d_offs17;
    cudaMalloc((void**) &d_offs17, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs17, 0, 1);
    }
    agg_ht<apayl22>* d_aht22;
    cudaMalloc((void**) &d_aht22, 75000* sizeof(agg_ht<apayl22>) );
    {
        int gridsize=920;
        int blocksize=128;
        initAggHT<<<gridsize, blocksize>>>(d_aht22, 75000);
    }
    float* d_agg1;
    cudaMalloc((void**) &d_agg1, 75000* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg1, 0.0f, 75000);
    }
    float* d_agg2;
    cudaMalloc((void**) &d_agg2, 75000* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg2, 0.0f, 75000);
    }
    int* d_agg3;
    cudaMalloc((void**) &d_agg3, 75000* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg3, 0, 75000);
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

    cudaMemcpy( d_iatt4_nnationk, iatt4_nnationk, 25 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt5_nname_offset, iatt5_nname_offset, (25 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt5_nname_char, iatt5_nname_char, 186 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt8_rregionk, iatt8_rregionk, 5 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt9_rname_offset, iatt9_rname_offset, (5 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt9_rname_char, iatt9_rname_char, 43 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt13_nregionk, iatt13_nregionk, 25 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt15_ppartkey, iatt15_ppartkey, 200000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt19_ptype_offset, iatt19_ptype_offset, (200000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt19_ptype_char, iatt19_ptype_char, 4119955 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt24_lorderke, iatt24_lorderke, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt25_lpartkey, iatt25_lpartkey, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt26_lsuppkey, iatt26_lsuppkey, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt29_lextende, iatt29_lextende, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt30_ldiscoun, iatt30_ldiscoun, 6001215 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt40_oorderke, iatt40_oorderke, 1500000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt41_ocustkey, iatt41_ocustkey, 1500000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt44_oorderda, iatt44_oorderda, 1500000 * sizeof(unsigned), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt49_ccustkey, iatt49_ccustkey, 150000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt52_cnationk, iatt52_cnationk, 150000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt57_ssuppkey, iatt57_ssuppkey, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt60_snationk, iatt60_snationk, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime56 = std::clock();
    std::clock_t start_krnl_nation157 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation1<<<gridsize, blocksize>>>(d_iatt4_nnationk, d_iatt5_nname_offset, d_iatt5_nname_char, d_jht18, d_jht18_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation157 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation1! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation1")
        }
    }

    std::clock_t start_scanMultiHT58 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht18, 50, d_offs18);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT58 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_nation1_ins59 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation1_ins<<<gridsize, blocksize>>>(d_iatt4_nnationk, d_iatt5_nname_offset, d_iatt5_nname_char, d_jht18, d_jht18_payload, d_offs18);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation1_ins59 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation1_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation1_ins")
        }
    }

    std::clock_t start_krnl_region260 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_region2<<<gridsize, blocksize>>>(d_iatt8_rregionk, d_iatt9_rname_offset, d_iatt9_rname_char, d_jht5);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_region260 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_region2! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_region2")
        }
    }

    std::clock_t start_krnl_nation2461 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation24<<<gridsize, blocksize>>>(d_iatt11_nnationk, d_iatt13_nregionk, d_jht5, d_jht15);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation2461 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation24! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation24")
        }
    }

    std::clock_t start_krnl_part662 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_part6<<<gridsize, blocksize>>>(d_iatt15_ppartkey, d_iatt19_ptype_offset, d_iatt19_ptype_char, d_jht9);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_part662 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_part6! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_part6")
        }
    }

    std::clock_t start_krnl_lineitem863 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_lineitem8<<<gridsize, blocksize>>>(d_iatt24_lorderke, d_iatt25_lpartkey, d_iatt26_lsuppkey, d_iatt29_lextende, d_iatt30_ldiscoun, d_jht9, d_jht12, d_jht12_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem863 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem8! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem8")
        }
    }

    std::clock_t start_scanMultiHT64 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht12, 120024, d_offs12);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT64 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_lineitem8_ins65 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_lineitem8_ins<<<gridsize, blocksize>>>(d_iatt24_lorderke, d_iatt25_lpartkey, d_iatt26_lsuppkey, d_iatt29_lextende, d_iatt30_ldiscoun, d_jht9, d_jht12, d_jht12_payload, d_offs12);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem8_ins65 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem8_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem8_ins")
        }
    }

    std::clock_t start_krnl_orders1066 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_orders10<<<gridsize, blocksize>>>(d_iatt40_oorderke, d_iatt41_ocustkey, d_iatt44_oorderda, d_jht12, d_jht12_payload, d_jht14, d_jht14_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_orders1066 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_orders10! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_orders10")
        }
    }

    std::clock_t start_scanMultiHT67 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht14, 300000, d_offs14);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT67 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_orders10_ins68 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_orders10_ins<<<gridsize, blocksize>>>(d_iatt40_oorderke, d_iatt41_ocustkey, d_iatt44_oorderda, d_jht12, d_jht12_payload, d_jht14, d_jht14_payload, d_offs14);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_orders10_ins68 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_orders10_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_orders10_ins")
        }
    }

    std::clock_t start_krnl_customer1369 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_customer13<<<gridsize, blocksize>>>(d_iatt49_ccustkey, d_iatt52_cnationk, d_jht14, d_jht14_payload, d_jht15, d_jht17, d_jht17_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_customer1369 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_customer13! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_customer13")
        }
    }

    std::clock_t start_scanMultiHT70 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht17, 75000, d_offs17);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT70 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_customer13_ins71 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_customer13_ins<<<gridsize, blocksize>>>(d_iatt49_ccustkey, d_iatt52_cnationk, d_jht14, d_jht14_payload, d_jht15, d_jht17, d_jht17_payload, d_offs17);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_customer13_ins71 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_customer13_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_customer13_ins")
        }
    }

    std::clock_t start_krnl_supplier1672 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_supplier16<<<gridsize, blocksize>>>(d_iatt57_ssuppkey, d_iatt60_snationk, d_jht17, d_jht17_payload, d_jht18, d_jht18_payload, d_aht22, d_agg1, d_agg2, d_agg3);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_supplier1672 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_supplier16! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_supplier16")
        }
    }

    std::clock_t start_krnl_aggregation2273 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation22<<<gridsize, blocksize>>>(d_aht22, d_agg1, d_agg2, d_agg3, d_nout_result, d_oatt64_oyear, d_oatt67_mktshare, d_oatt3_salesnum);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation2273 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation22! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation22")
        }
    }

    std::clock_t stop_totalKernelTime56 = std::clock();
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt64_oyear.data(), d_oatt64_oyear, 37500 * sizeof(unsigned), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt67_mktshare.data(), d_oatt67_mktshare, 37500 * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt3_salesnum.data(), d_oatt3_salesnum, 37500 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    cudaFree( d_iatt4_nnationk);
    cudaFree( d_iatt5_nname_offset);
    cudaFree( d_iatt5_nname_char);
    cudaFree( d_jht18);
    cudaFree( d_jht18_payload);
    cudaFree( d_offs18);
    cudaFree( d_iatt8_rregionk);
    cudaFree( d_iatt9_rname_offset);
    cudaFree( d_iatt9_rname_char);
    cudaFree( d_jht5);
    cudaFree( d_iatt13_nregionk);
    cudaFree( d_jht15);
    cudaFree( d_iatt15_ppartkey);
    cudaFree( d_iatt19_ptype_offset);
    cudaFree( d_iatt19_ptype_char);
    cudaFree( d_jht9);
    cudaFree( d_iatt24_lorderke);
    cudaFree( d_iatt25_lpartkey);
    cudaFree( d_iatt26_lsuppkey);
    cudaFree( d_iatt29_lextende);
    cudaFree( d_iatt30_ldiscoun);
    cudaFree( d_jht12);
    cudaFree( d_jht12_payload);
    cudaFree( d_offs12);
    cudaFree( d_iatt40_oorderke);
    cudaFree( d_iatt41_ocustkey);
    cudaFree( d_iatt44_oorderda);
    cudaFree( d_jht14);
    cudaFree( d_jht14_payload);
    cudaFree( d_offs14);
    cudaFree( d_iatt49_ccustkey);
    cudaFree( d_iatt52_cnationk);
    cudaFree( d_jht17);
    cudaFree( d_jht17_payload);
    cudaFree( d_offs17);
    cudaFree( d_iatt57_ssuppkey);
    cudaFree( d_iatt60_snationk);
    cudaFree( d_aht22);
    cudaFree( d_agg1);
    cudaFree( d_agg2);
    cudaFree( d_agg3);
    cudaFree( d_nout_result);
    cudaFree( d_oatt64_oyear);
    cudaFree( d_oatt67_mktshare);
    cudaFree( d_oatt3_salesnum);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda free! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda free")
        }
    }

    std::clock_t start_finish74 = std::clock();
    printf("\nResult: %i tuples\n", nout_result);
    if((nout_result > 37500)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("o_year: ");
        printf("%10i", oatt64_oyear[pv]);
        printf("  ");
        printf("mkt_share: ");
        printf("%15.2f", oatt67_mktshare[pv]);
        printf("  ");
        printf("salesnum: ");
        printf("%8i", oatt3_salesnum[pv]);
        printf("  ");
        printf("\n");
    }
    if((nout_result > 10)) {
        printf("[...]\n");
    }
    printf("\n");
    std::clock_t stop_finish74 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_nation1", (stop_krnl_nation157 - start_krnl_nation157) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT58 - start_scanMultiHT58) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation1_ins", (stop_krnl_nation1_ins59 - start_krnl_nation1_ins59) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_region2", (stop_krnl_region260 - start_krnl_region260) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation24", (stop_krnl_nation2461 - start_krnl_nation2461) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_part6", (stop_krnl_part662 - start_krnl_part662) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem8", (stop_krnl_lineitem863 - start_krnl_lineitem863) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT64 - start_scanMultiHT64) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem8_ins", (stop_krnl_lineitem8_ins65 - start_krnl_lineitem8_ins65) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_orders10", (stop_krnl_orders1066 - start_krnl_orders1066) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT67 - start_scanMultiHT67) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_orders10_ins", (stop_krnl_orders10_ins68 - start_krnl_orders10_ins68) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_customer13", (stop_krnl_customer1369 - start_krnl_customer1369) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT70 - start_scanMultiHT70) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_customer13_ins", (stop_krnl_customer13_ins71 - start_krnl_customer13_ins71) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_supplier16", (stop_krnl_supplier1672 - start_krnl_supplier1672) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation22", (stop_krnl_aggregation2273 - start_krnl_aggregation2273) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish74 - start_finish74) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime56 - start_totalKernelTime56) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
