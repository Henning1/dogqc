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
    int att2_ppartkey;
};
struct jpayl8 {
    int att11_pspartke;
    int att12_pssuppke;
    int att13_psavailq;
};
struct apayl7 {
    int att17_lpartkey;
    int att18_lsuppkey;
};
struct jpayl14 {
    int att12_pssuppke;
};
struct jpayl13 {
    int att32_nnationk;
};

__global__ void krnl_part1(
    int* iatt2_ppartkey, size_t* iatt3_pname_offset, char* iatt3_pname_char, agg_ht<jpayl4>* jht4) {
    int att2_ppartkey;
    str_t att3_pname;
    str_t c1 = stringConstant ( "forest%", 7);

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
            att3_pname = stringScan ( iatt3_pname_offset, iatt3_pname_char, tid_part1);
        }
        // -------- selection (opId: 2) --------
        if(active) {
            active = stringLikeCheck ( att3_pname, c1);
        }
        // -------- hash join build (opId: 4) --------
        if(active) {
            uint64_t hash4;
            hash4 = 0;
            if(active) {
                hash4 = hash ( (hash4 + ((uint64_t)att2_ppartkey)));
            }
            int bucket = 0;
            jpayl4 payl4;
            payl4.att2_ppartkey = att2_ppartkey;
            int bucketFound = 0;
            int numLookups = 0;
            while(!(bucketFound)) {
                bucket = hashAggregateGetBucket ( jht4, 40000, hash4, numLookups, &(payl4));
                jpayl4 probepayl = jht4[bucket].payload;
                bucketFound = 1;
                bucketFound &= ((payl4.att2_ppartkey == probepayl.att2_ppartkey));
            }
        }
        loopVar += step;
    }

}

__global__ void krnl_partsupp3(
    int* iatt11_pspartke, int* iatt12_pssuppke, int* iatt13_psavailq, agg_ht<jpayl4>* jht4, unique_ht<jpayl8>* jht8) {
    int att11_pspartke;
    int att12_pssuppke;
    int att13_psavailq;
    int att2_ppartkey;

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
            att11_pspartke = iatt11_pspartke[tid_partsupp1];
            att12_pssuppke = iatt12_pssuppke[tid_partsupp1];
            att13_psavailq = iatt13_psavailq[tid_partsupp1];
        }
        // -------- hash join probe (opId: 4) --------
        if(active) {
            uint64_t hash4 = 0;
            hash4 = 0;
            if(active) {
                hash4 = hash ( (hash4 + ((uint64_t)att11_pspartke)));
            }
            int numLookups4 = 0;
            int location4 = 0;
            int filterMatch4 = 0;
            int activeProbe4 = 1;
            while((!(filterMatch4) && activeProbe4)) {
                activeProbe4 = hashAggregateFindBucket ( jht4, 40000, hash4, numLookups4, location4);
                if(activeProbe4) {
                    jpayl4 probepayl = jht4[location4].payload;
                    att2_ppartkey = probepayl.att2_ppartkey;
                    filterMatch4 = 1;
                    filterMatch4 &= ((att2_ppartkey == att11_pspartke));
                }
            }
            active &= (filterMatch4);
        }
        // -------- hash join build (opId: 8) --------
        if(active) {
            jpayl8 payl8;
            payl8.att11_pspartke = att11_pspartke;
            payl8.att12_pssuppke = att12_pssuppke;
            payl8.att13_psavailq = att13_psavailq;
            uint64_t hash8;
            hash8 = 0;
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att11_pspartke)));
            }
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att12_pssuppke)));
            }
            hashBuildUnique ( jht8, 160000, hash8, &(payl8));
        }
        loopVar += step;
    }

}

__global__ void krnl_lineitem5(
    int* iatt17_lpartkey, int* iatt18_lsuppkey, int* iatt20_lquantit, unsigned* iatt26_lshipdat, agg_ht<apayl7>* aht7, float* agg1) {
    int att17_lpartkey;
    int att18_lsuppkey;
    int att20_lquantit;
    unsigned att26_lshipdat;

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
            att17_lpartkey = iatt17_lpartkey[tid_lineitem1];
            att18_lsuppkey = iatt18_lsuppkey[tid_lineitem1];
            att20_lquantit = iatt20_lquantit[tid_lineitem1];
            att26_lshipdat = iatt26_lshipdat[tid_lineitem1];
        }
        // -------- selection (opId: 6) --------
        if(active) {
            active = ((att26_lshipdat >= 19940101) && (att26_lshipdat < 19950101));
        }
        // -------- aggregation (opId: 7) --------
        int bucket = 0;
        if(active) {
            uint64_t hash7 = 0;
            hash7 = 0;
            if(active) {
                hash7 = hash ( (hash7 + ((uint64_t)att17_lpartkey)));
            }
            if(active) {
                hash7 = hash ( (hash7 + ((uint64_t)att18_lsuppkey)));
            }
            apayl7 payl;
            payl.att17_lpartkey = att17_lpartkey;
            payl.att18_lsuppkey = att18_lsuppkey;
            int bucketFound = 0;
            int numLookups = 0;
            while(!(bucketFound)) {
                bucket = hashAggregateGetBucket ( aht7, 2400486, hash7, numLookups, &(payl));
                apayl7 probepayl = aht7[bucket].payload;
                bucketFound = 1;
                bucketFound &= ((payl.att17_lpartkey == probepayl.att17_lpartkey));
                bucketFound &= ((payl.att18_lsuppkey == probepayl.att18_lsuppkey));
            }
        }
        if(active) {
            atomicAdd(&(agg1[bucket]), ((float)att20_lquantit));
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation7(
    agg_ht<apayl7>* aht7, float* agg1, unique_ht<jpayl8>* jht8, multi_ht* jht14, jpayl14* jht14_payload) {
    int att17_lpartkey;
    int att18_lsuppkey;
    float att1_sumqty;
    int att11_pspartke;
    int att12_pssuppke;
    int att13_psavailq;

    int tid_aggregation7 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation7 = loopVar;
        active = (loopVar < 2400486);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 7) --------
        if(active) {
            active &= ((aht7[tid_aggregation7].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl7 payl = aht7[tid_aggregation7].payload;
            att17_lpartkey = payl.att17_lpartkey;
            att18_lsuppkey = payl.att18_lsuppkey;
        }
        if(active) {
            att1_sumqty = agg1[tid_aggregation7];
        }
        // -------- hash join probe (opId: 8) --------
        uint64_t hash8 = 0;
        if(active) {
            hash8 = 0;
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att17_lpartkey)));
            }
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att18_lsuppkey)));
            }
        }
        jpayl8* probepayl8;
        int numLookups8 = 0;
        if(active) {
            active = hashProbeUnique ( jht8, 160000, hash8, numLookups8, &(probepayl8));
        }
        int bucketFound8 = 0;
        int probeActive8 = active;
        while((probeActive8 && !(bucketFound8))) {
            jpayl8 jprobepayl8 = *(probepayl8);
            att11_pspartke = jprobepayl8.att11_pspartke;
            att12_pssuppke = jprobepayl8.att12_pssuppke;
            att13_psavailq = jprobepayl8.att13_psavailq;
            bucketFound8 = 1;
            bucketFound8 &= ((att11_pspartke == att17_lpartkey));
            bucketFound8 &= ((att12_pssuppke == att18_lsuppkey));
            if(!(bucketFound8)) {
                probeActive8 = hashProbeUnique ( jht8, 160000, hash8, numLookups8, &(probepayl8));
            }
        }
        active = bucketFound8;
        // -------- selection (opId: 9) --------
        if(active) {
            active = (att13_psavailq > (0.5f * att1_sumqty));
        }
        // -------- hash join build (opId: 14) --------
        if(active) {
            uint64_t hash14 = 0;
            if(active) {
                hash14 = 0;
                if(active) {
                    hash14 = hash ( (hash14 + ((uint64_t)att12_pssuppke)));
                }
            }
            hashCountMulti ( jht14, 192038, hash14);
        }
        loopVar += step;
    }

}

__global__ void krnl_aggregation7_ins(
    agg_ht<apayl7>* aht7, float* agg1, unique_ht<jpayl8>* jht8, multi_ht* jht14, jpayl14* jht14_payload, int* offs14) {
    int att17_lpartkey;
    int att18_lsuppkey;
    float att1_sumqty;
    int att11_pspartke;
    int att12_pssuppke;
    int att13_psavailq;

    int tid_aggregation7 = 0;
    unsigned loopVar = ((blockIdx.x * blockDim.x) + threadIdx.x);
    unsigned step = (blockDim.x * gridDim.x);
    unsigned flushPipeline = 0;
    int active = 0;
    while(!(flushPipeline)) {
        tid_aggregation7 = loopVar;
        active = (loopVar < 2400486);
        // flush pipeline if no new elements
        flushPipeline = !(__ballot_sync(ALL_LANES,active));
        if(active) {
        }
        // -------- scan aggregation ht (opId: 7) --------
        if(active) {
            active &= ((aht7[tid_aggregation7].lock.lock == OnceLock::LOCK_DONE));
        }
        if(active) {
            apayl7 payl = aht7[tid_aggregation7].payload;
            att17_lpartkey = payl.att17_lpartkey;
            att18_lsuppkey = payl.att18_lsuppkey;
        }
        if(active) {
            att1_sumqty = agg1[tid_aggregation7];
        }
        // -------- hash join probe (opId: 8) --------
        uint64_t hash8 = 0;
        if(active) {
            hash8 = 0;
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att17_lpartkey)));
            }
            if(active) {
                hash8 = hash ( (hash8 + ((uint64_t)att18_lsuppkey)));
            }
        }
        jpayl8* probepayl8;
        int numLookups8 = 0;
        if(active) {
            active = hashProbeUnique ( jht8, 160000, hash8, numLookups8, &(probepayl8));
        }
        int bucketFound8 = 0;
        int probeActive8 = active;
        while((probeActive8 && !(bucketFound8))) {
            jpayl8 jprobepayl8 = *(probepayl8);
            att11_pspartke = jprobepayl8.att11_pspartke;
            att12_pssuppke = jprobepayl8.att12_pssuppke;
            att13_psavailq = jprobepayl8.att13_psavailq;
            bucketFound8 = 1;
            bucketFound8 &= ((att11_pspartke == att17_lpartkey));
            bucketFound8 &= ((att12_pssuppke == att18_lsuppkey));
            if(!(bucketFound8)) {
                probeActive8 = hashProbeUnique ( jht8, 160000, hash8, numLookups8, &(probepayl8));
            }
        }
        active = bucketFound8;
        // -------- selection (opId: 9) --------
        if(active) {
            active = (att13_psavailq > (0.5f * att1_sumqty));
        }
        // -------- hash join build (opId: 14) --------
        if(active) {
            uint64_t hash14 = 0;
            if(active) {
                hash14 = 0;
                if(active) {
                    hash14 = hash ( (hash14 + ((uint64_t)att12_pssuppke)));
                }
            }
            jpayl14 payl;
            payl.att12_pssuppke = att12_pssuppke;
            hashInsertMulti ( jht14, jht14_payload, offs14, 192038, hash14, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_nation10(
    int* iatt32_nnationk, size_t* iatt33_nname_offset, char* iatt33_nname_char, multi_ht* jht13, jpayl13* jht13_payload) {
    int att32_nnationk;
    str_t att33_nname;
    str_t c2 = stringConstant ( "CANADA", 6);

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
            att32_nnationk = iatt32_nnationk[tid_nation1];
            att33_nname = stringScan ( iatt33_nname_offset, iatt33_nname_char, tid_nation1);
        }
        // -------- selection (opId: 11) --------
        if(active) {
            active = stringEquals ( att33_nname, c2);
        }
        // -------- hash join build (opId: 13) --------
        if(active) {
            uint64_t hash13 = 0;
            if(active) {
                hash13 = 0;
                if(active) {
                    hash13 = hash ( (hash13 + ((uint64_t)att32_nnationk)));
                }
            }
            hashCountMulti ( jht13, 50, hash13);
        }
        loopVar += step;
    }

}

__global__ void krnl_nation10_ins(
    int* iatt32_nnationk, size_t* iatt33_nname_offset, char* iatt33_nname_char, multi_ht* jht13, jpayl13* jht13_payload, int* offs13) {
    int att32_nnationk;
    str_t att33_nname;
    str_t c2 = stringConstant ( "CANADA", 6);

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
            att32_nnationk = iatt32_nnationk[tid_nation1];
            att33_nname = stringScan ( iatt33_nname_offset, iatt33_nname_char, tid_nation1);
        }
        // -------- selection (opId: 11) --------
        if(active) {
            active = stringEquals ( att33_nname, c2);
        }
        // -------- hash join build (opId: 13) --------
        if(active) {
            uint64_t hash13 = 0;
            if(active) {
                hash13 = 0;
                if(active) {
                    hash13 = hash ( (hash13 + ((uint64_t)att32_nnationk)));
                }
            }
            jpayl13 payl;
            payl.att32_nnationk = att32_nnationk;
            hashInsertMulti ( jht13, jht13_payload, offs13, 50, hash13, &(payl));
        }
        loopVar += step;
    }

}

__global__ void krnl_supplier12(
    int* iatt36_ssuppkey, size_t* iatt37_sname_offset, char* iatt37_sname_char, size_t* iatt38_saddress_offset, char* iatt38_saddress_char, int* iatt39_snationk, multi_ht* jht13, jpayl13* jht13_payload, multi_ht* jht14, jpayl14* jht14_payload, int* nout_result, str_offs* oatt37_sname_offset, str_offs* oatt38_saddress_offset) {
    int att36_ssuppkey;
    str_t att37_sname;
    str_t att38_saddress;
    int att39_snationk;
    unsigned warplane = (threadIdx.x % 32);
    int att32_nnationk;
    int att12_pssuppke;
    unsigned prefixlanes = (0xffffffff >> (32 - warplane));

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
            att36_ssuppkey = iatt36_ssuppkey[tid_supplier1];
            att37_sname = stringScan ( iatt37_sname_offset, iatt37_sname_char, tid_supplier1);
            att38_saddress = stringScan ( iatt38_saddress_offset, iatt38_saddress_char, tid_supplier1);
            att39_snationk = iatt39_snationk[tid_supplier1];
        }
        // -------- hash join probe (opId: 13) --------
        // -------- multiprobe multi broadcast (opId: 13) --------
        int matchEnd13 = 0;
        int matchEndBuf13 = 0;
        int matchOffset13 = 0;
        int matchOffsetBuf13 = 0;
        int probeActive13 = active;
        int att36_ssuppkey_bcbuf13;
        str_t att37_sname_bcbuf13;
        str_t att38_saddress_bcbuf13;
        int att39_snationk_bcbuf13;
        uint64_t hash13 = 0;
        if(probeActive13) {
            hash13 = 0;
            if(active) {
                hash13 = hash ( (hash13 + ((uint64_t)att39_snationk)));
            }
            probeActive13 = hashProbeMulti ( jht13, 50, hash13, matchOffsetBuf13, matchEndBuf13);
        }
        unsigned activeProbes13 = __ballot_sync(ALL_LANES,probeActive13);
        int num13 = 0;
        num13 = (matchEndBuf13 - matchOffsetBuf13);
        unsigned wideProbes13 = __ballot_sync(ALL_LANES,(num13 >= 32));
        att36_ssuppkey_bcbuf13 = att36_ssuppkey;
        att37_sname_bcbuf13 = att37_sname;
        att38_saddress_bcbuf13 = att38_saddress;
        att39_snationk_bcbuf13 = att39_snationk;
        while((activeProbes13 > 0)) {
            unsigned tupleLane;
            unsigned broadcastLane;
            int numFilled = 0;
            int num = 0;
            while(((numFilled < 32) && activeProbes13)) {
                if((wideProbes13 > 0)) {
                    tupleLane = (__ffs(wideProbes13) - 1);
                    wideProbes13 -= (1 << tupleLane);
                }
                else {
                    tupleLane = (__ffs(activeProbes13) - 1);
                }
                num = __shfl_sync(ALL_LANES,num13,tupleLane);
                if((numFilled && ((numFilled + num) > 32))) {
                    break;
                }
                if((warplane >= numFilled)) {
                    broadcastLane = tupleLane;
                    matchOffset13 = (warplane - numFilled);
                }
                numFilled += num;
                activeProbes13 -= (1 << tupleLane);
            }
            matchOffset13 += __shfl_sync(ALL_LANES,matchOffsetBuf13,broadcastLane);
            matchEnd13 = __shfl_sync(ALL_LANES,matchEndBuf13,broadcastLane);
            att36_ssuppkey = __shfl_sync(ALL_LANES,att36_ssuppkey_bcbuf13,broadcastLane);
            att37_sname = __shfl_sync(ALL_LANES,att37_sname_bcbuf13,broadcastLane);
            att38_saddress = __shfl_sync(ALL_LANES,att38_saddress_bcbuf13,broadcastLane);
            att39_snationk = __shfl_sync(ALL_LANES,att39_snationk_bcbuf13,broadcastLane);
            probeActive13 = (matchOffset13 < matchEnd13);
            while(__any_sync(ALL_LANES,probeActive13)) {
                active = probeActive13;
                active = 0;
                jpayl13 payl;
                if(probeActive13) {
                    payl = jht13_payload[matchOffset13];
                    att32_nnationk = payl.att32_nnationk;
                    active = 1;
                    active &= ((att32_nnationk == att39_snationk));
                    matchOffset13 += 32;
                    probeActive13 &= ((matchOffset13 < matchEnd13));
                }
                // -------- hash join probe (opId: 14) --------
                int matchEnd14 = 0;
                int matchOffset14 = 0;
                int matchStep14 = 1;
                int filterMatch14 = 0;
                int probeActive14 = active;
                uint64_t hash14 = 0;
                if(probeActive14) {
                    hash14 = 0;
                    if(active) {
                        hash14 = hash ( (hash14 + ((uint64_t)att36_ssuppkey)));
                    }
                    probeActive14 = hashProbeMulti ( jht14, 192038, hash14, matchOffset14, matchEnd14);
                }
                while(probeActive14) {
                    jpayl14 payl;
                    payl = jht14_payload[matchOffset14];
                    att12_pssuppke = payl.att12_pssuppke;
                    filterMatch14 = 1;
                    filterMatch14 &= ((att12_pssuppke == att36_ssuppkey));
                    matchOffset14 += matchStep14;
                    probeActive14 &= (!(filterMatch14));
                    probeActive14 &= ((matchOffset14 < matchEnd14));
                }
                active &= (filterMatch14);
                // -------- projection (no code) (opId: 15) --------
                // -------- materialize (opId: 16) --------
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
                    oatt37_sname_offset[wp] = toStringOffset ( iatt37_sname_char, att37_sname);
                    oatt38_saddress_offset[wp] = toStringOffset ( iatt38_saddress_char, att38_saddress);
                }
            }
        }
        loopVar += step;
    }

}

int main() {
    int* iatt2_ppartkey;
    iatt2_ppartkey = ( int*) map_memory_file ( "mmdb/part_p_partkey" );
    size_t* iatt3_pname_offset;
    iatt3_pname_offset = ( size_t*) map_memory_file ( "mmdb/part_p_name_offset" );
    char* iatt3_pname_char;
    iatt3_pname_char = ( char*) map_memory_file ( "mmdb/part_p_name_char" );
    int* iatt11_pspartke;
    iatt11_pspartke = ( int*) map_memory_file ( "mmdb/partsupp_ps_partkey" );
    int* iatt12_pssuppke;
    iatt12_pssuppke = ( int*) map_memory_file ( "mmdb/partsupp_ps_suppkey" );
    int* iatt13_psavailq;
    iatt13_psavailq = ( int*) map_memory_file ( "mmdb/partsupp_ps_availqty" );
    int* iatt17_lpartkey;
    iatt17_lpartkey = ( int*) map_memory_file ( "mmdb/lineitem_l_partkey" );
    int* iatt18_lsuppkey;
    iatt18_lsuppkey = ( int*) map_memory_file ( "mmdb/lineitem_l_suppkey" );
    int* iatt20_lquantit;
    iatt20_lquantit = ( int*) map_memory_file ( "mmdb/lineitem_l_quantity" );
    unsigned* iatt26_lshipdat;
    iatt26_lshipdat = ( unsigned*) map_memory_file ( "mmdb/lineitem_l_shipdate" );
    int* iatt32_nnationk;
    iatt32_nnationk = ( int*) map_memory_file ( "mmdb/nation_n_nationkey" );
    size_t* iatt33_nname_offset;
    iatt33_nname_offset = ( size_t*) map_memory_file ( "mmdb/nation_n_name_offset" );
    char* iatt33_nname_char;
    iatt33_nname_char = ( char*) map_memory_file ( "mmdb/nation_n_name_char" );
    int* iatt36_ssuppkey;
    iatt36_ssuppkey = ( int*) map_memory_file ( "mmdb/supplier_s_suppkey" );
    size_t* iatt37_sname_offset;
    iatt37_sname_offset = ( size_t*) map_memory_file ( "mmdb/supplier_s_name_offset" );
    char* iatt37_sname_char;
    iatt37_sname_char = ( char*) map_memory_file ( "mmdb/supplier_s_name_char" );
    size_t* iatt38_saddress_offset;
    iatt38_saddress_offset = ( size_t*) map_memory_file ( "mmdb/supplier_s_address_offset" );
    char* iatt38_saddress_char;
    iatt38_saddress_char = ( char*) map_memory_file ( "mmdb/supplier_s_address_char" );
    int* iatt39_snationk;
    iatt39_snationk = ( int*) map_memory_file ( "mmdb/supplier_s_nationkey" );

    int nout_result;
    std::vector < str_offs > oatt37_sname_offset(3840);
    std::vector < str_offs > oatt38_saddress_offset(3840);

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
    size_t* d_iatt3_pname_offset;
    cudaMalloc((void**) &d_iatt3_pname_offset, (200000 + 1)* sizeof(size_t) );
    char* d_iatt3_pname_char;
    cudaMalloc((void**) &d_iatt3_pname_char, 6550230* sizeof(char) );
    int* d_iatt11_pspartke;
    cudaMalloc((void**) &d_iatt11_pspartke, 800000* sizeof(int) );
    int* d_iatt12_pssuppke;
    cudaMalloc((void**) &d_iatt12_pssuppke, 800000* sizeof(int) );
    int* d_iatt13_psavailq;
    cudaMalloc((void**) &d_iatt13_psavailq, 800000* sizeof(int) );
    int* d_iatt17_lpartkey;
    cudaMalloc((void**) &d_iatt17_lpartkey, 6001215* sizeof(int) );
    int* d_iatt18_lsuppkey;
    cudaMalloc((void**) &d_iatt18_lsuppkey, 6001215* sizeof(int) );
    int* d_iatt20_lquantit;
    cudaMalloc((void**) &d_iatt20_lquantit, 6001215* sizeof(int) );
    unsigned* d_iatt26_lshipdat;
    cudaMalloc((void**) &d_iatt26_lshipdat, 6001215* sizeof(unsigned) );
    int* d_iatt32_nnationk;
    cudaMalloc((void**) &d_iatt32_nnationk, 25* sizeof(int) );
    size_t* d_iatt33_nname_offset;
    cudaMalloc((void**) &d_iatt33_nname_offset, (25 + 1)* sizeof(size_t) );
    char* d_iatt33_nname_char;
    cudaMalloc((void**) &d_iatt33_nname_char, 186* sizeof(char) );
    int* d_iatt36_ssuppkey;
    cudaMalloc((void**) &d_iatt36_ssuppkey, 10000* sizeof(int) );
    size_t* d_iatt37_sname_offset;
    cudaMalloc((void**) &d_iatt37_sname_offset, (10000 + 1)* sizeof(size_t) );
    char* d_iatt37_sname_char;
    cudaMalloc((void**) &d_iatt37_sname_char, 180009* sizeof(char) );
    size_t* d_iatt38_saddress_offset;
    cudaMalloc((void**) &d_iatt38_saddress_offset, (10000 + 1)* sizeof(size_t) );
    char* d_iatt38_saddress_char;
    cudaMalloc((void**) &d_iatt38_saddress_char, 249461* sizeof(char) );
    int* d_iatt39_snationk;
    cudaMalloc((void**) &d_iatt39_snationk, 10000* sizeof(int) );
    int* d_nout_result;
    cudaMalloc((void**) &d_nout_result, 1* sizeof(int) );
    str_offs* d_oatt37_sname_offset;
    cudaMalloc((void**) &d_oatt37_sname_offset, 3840* sizeof(str_offs) );
    str_offs* d_oatt38_saddress_offset;
    cudaMalloc((void**) &d_oatt38_saddress_offset, 3840* sizeof(str_offs) );
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

    agg_ht<jpayl4>* d_jht4;
    cudaMalloc((void**) &d_jht4, 40000* sizeof(agg_ht<jpayl4>) );
    {
        int gridsize=920;
        int blocksize=128;
        initAggHT<<<gridsize, blocksize>>>(d_jht4, 40000);
    }
    unique_ht<jpayl8>* d_jht8;
    cudaMalloc((void**) &d_jht8, 160000* sizeof(unique_ht<jpayl8>) );
    {
        int gridsize=920;
        int blocksize=128;
        initUniqueHT<<<gridsize, blocksize>>>(d_jht8, 160000);
    }
    agg_ht<apayl7>* d_aht7;
    cudaMalloc((void**) &d_aht7, 2400486* sizeof(agg_ht<apayl7>) );
    {
        int gridsize=920;
        int blocksize=128;
        initAggHT<<<gridsize, blocksize>>>(d_aht7, 2400486);
    }
    float* d_agg1;
    cudaMalloc((void**) &d_agg1, 2400486* sizeof(float) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_agg1, 0.0f, 2400486);
    }
    multi_ht* d_jht14;
    cudaMalloc((void**) &d_jht14, 192038* sizeof(multi_ht) );
    jpayl14* d_jht14_payload;
    cudaMalloc((void**) &d_jht14_payload, 192038* sizeof(jpayl14) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht14, 192038);
    }
    int* d_offs14;
    cudaMalloc((void**) &d_offs14, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs14, 0, 1);
    }
    multi_ht* d_jht13;
    cudaMalloc((void**) &d_jht13, 50* sizeof(multi_ht) );
    jpayl13* d_jht13_payload;
    cudaMalloc((void**) &d_jht13_payload, 50* sizeof(jpayl13) );
    {
        int gridsize=920;
        int blocksize=128;
        initMultiHT<<<gridsize, blocksize>>>(d_jht13, 50);
    }
    int* d_offs13;
    cudaMalloc((void**) &d_offs13, 1* sizeof(int) );
    {
        int gridsize=920;
        int blocksize=128;
        initArray<<<gridsize, blocksize>>>(d_offs13, 0, 1);
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
    cudaMemcpy( d_iatt3_pname_offset, iatt3_pname_offset, (200000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt3_pname_char, iatt3_pname_char, 6550230 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt11_pspartke, iatt11_pspartke, 800000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt12_pssuppke, iatt12_pssuppke, 800000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt13_psavailq, iatt13_psavailq, 800000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt17_lpartkey, iatt17_lpartkey, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt18_lsuppkey, iatt18_lsuppkey, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt20_lquantit, iatt20_lquantit, 6001215 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt26_lshipdat, iatt26_lshipdat, 6001215 * sizeof(unsigned), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt32_nnationk, iatt32_nnationk, 25 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt33_nname_offset, iatt33_nname_offset, (25 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt33_nname_char, iatt33_nname_char, 186 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt36_ssuppkey, iatt36_ssuppkey, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt37_sname_offset, iatt37_sname_offset, (10000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt37_sname_char, iatt37_sname_char, 180009 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt38_saddress_offset, iatt38_saddress_offset, (10000 + 1) * sizeof(size_t), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt38_saddress_char, iatt38_saddress_char, 249461 * sizeof(char), cudaMemcpyHostToDevice);
    cudaMemcpy( d_iatt39_snationk, iatt39_snationk, 10000 * sizeof(int), cudaMemcpyHostToDevice);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy in! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy in")
        }
    }

    std::clock_t start_totalKernelTime166 = std::clock();
    std::clock_t start_krnl_part1167 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_part1<<<gridsize, blocksize>>>(d_iatt2_ppartkey, d_iatt3_pname_offset, d_iatt3_pname_char, d_jht4);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_part1167 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_part1! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_part1")
        }
    }

    std::clock_t start_krnl_partsupp3168 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_partsupp3<<<gridsize, blocksize>>>(d_iatt11_pspartke, d_iatt12_pssuppke, d_iatt13_psavailq, d_jht4, d_jht8);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_partsupp3168 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_partsupp3! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_partsupp3")
        }
    }

    std::clock_t start_krnl_lineitem5169 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_lineitem5<<<gridsize, blocksize>>>(d_iatt17_lpartkey, d_iatt18_lsuppkey, d_iatt20_lquantit, d_iatt26_lshipdat, d_aht7, d_agg1);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_lineitem5169 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_lineitem5! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_lineitem5")
        }
    }

    std::clock_t start_krnl_aggregation7170 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation7<<<gridsize, blocksize>>>(d_aht7, d_agg1, d_jht8, d_jht14, d_jht14_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation7170 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation7! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation7")
        }
    }

    std::clock_t start_scanMultiHT171 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht14, 192038, d_offs14);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT171 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_aggregation7_ins172 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_aggregation7_ins<<<gridsize, blocksize>>>(d_aht7, d_agg1, d_jht8, d_jht14, d_jht14_payload, d_offs14);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_aggregation7_ins172 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_aggregation7_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_aggregation7_ins")
        }
    }

    std::clock_t start_krnl_nation10173 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation10<<<gridsize, blocksize>>>(d_iatt32_nnationk, d_iatt33_nname_offset, d_iatt33_nname_char, d_jht13, d_jht13_payload);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation10173 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation10! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation10")
        }
    }

    std::clock_t start_scanMultiHT174 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        scanMultiHT<<<gridsize, blocksize>>>(d_jht13, 50, d_offs13);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_scanMultiHT174 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in scanMultiHT! " << cudaGetErrorString( err ) << std::endl;
            ERROR("scanMultiHT")
        }
    }

    std::clock_t start_krnl_nation10_ins175 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_nation10_ins<<<gridsize, blocksize>>>(d_iatt32_nnationk, d_iatt33_nname_offset, d_iatt33_nname_char, d_jht13, d_jht13_payload, d_offs13);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_nation10_ins175 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_nation10_ins! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_nation10_ins")
        }
    }

    std::clock_t start_krnl_supplier12176 = std::clock();
    {
        int gridsize=920;
        int blocksize=128;
        krnl_supplier12<<<gridsize, blocksize>>>(d_iatt36_ssuppkey, d_iatt37_sname_offset, d_iatt37_sname_char, d_iatt38_saddress_offset, d_iatt38_saddress_char, d_iatt39_snationk, d_jht13, d_jht13_payload, d_jht14, d_jht14_payload, d_nout_result, d_oatt37_sname_offset, d_oatt38_saddress_offset);
    }
    cudaDeviceSynchronize();
    std::clock_t stop_krnl_supplier12176 = std::clock();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in krnl_supplier12! " << cudaGetErrorString( err ) << std::endl;
            ERROR("krnl_supplier12")
        }
    }

    std::clock_t stop_totalKernelTime166 = std::clock();
    cudaMemcpy( &nout_result, d_nout_result, 1 * sizeof(int), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt37_sname_offset.data(), d_oatt37_sname_offset, 3840 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaMemcpy( oatt38_saddress_offset.data(), d_oatt38_saddress_offset, 3840 * sizeof(str_offs), cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda memcpy out! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda memcpy out")
        }
    }

    cudaFree( d_iatt2_ppartkey);
    cudaFree( d_iatt3_pname_offset);
    cudaFree( d_iatt3_pname_char);
    cudaFree( d_jht4);
    cudaFree( d_iatt11_pspartke);
    cudaFree( d_iatt12_pssuppke);
    cudaFree( d_iatt13_psavailq);
    cudaFree( d_jht8);
    cudaFree( d_iatt17_lpartkey);
    cudaFree( d_iatt18_lsuppkey);
    cudaFree( d_iatt20_lquantit);
    cudaFree( d_iatt26_lshipdat);
    cudaFree( d_aht7);
    cudaFree( d_agg1);
    cudaFree( d_jht14);
    cudaFree( d_jht14_payload);
    cudaFree( d_offs14);
    cudaFree( d_iatt32_nnationk);
    cudaFree( d_iatt33_nname_offset);
    cudaFree( d_iatt33_nname_char);
    cudaFree( d_jht13);
    cudaFree( d_jht13_payload);
    cudaFree( d_offs13);
    cudaFree( d_iatt36_ssuppkey);
    cudaFree( d_iatt37_sname_offset);
    cudaFree( d_iatt37_sname_char);
    cudaFree( d_iatt38_saddress_offset);
    cudaFree( d_iatt38_saddress_char);
    cudaFree( d_iatt39_snationk);
    cudaFree( d_nout_result);
    cudaFree( d_oatt37_sname_offset);
    cudaFree( d_oatt38_saddress_offset);
    cudaDeviceSynchronize();
    {
        cudaError err = cudaGetLastError();
        if(err != cudaSuccess) {
            std::cerr << "Cuda Error in cuda free! " << cudaGetErrorString( err ) << std::endl;
            ERROR("cuda free")
        }
    }

    std::clock_t start_finish177 = std::clock();
    printf("\nResult: %i tuples\n", nout_result);
    if((nout_result > 3840)) {
        ERROR("Index out of range. Output size larger than allocated with expected result number.")
    }
    for ( int pv = 0; ((pv < 10) && (pv < nout_result)); pv += 1) {
        printf("s_name: ");
        stringPrint ( iatt37_sname_char, oatt37_sname_offset[pv]);
        printf("  ");
        printf("s_address: ");
        stringPrint ( iatt38_saddress_char, oatt38_saddress_offset[pv]);
        printf("  ");
        printf("\n");
    }
    if((nout_result > 10)) {
        printf("[...]\n");
    }
    printf("\n");
    std::clock_t stop_finish177 = std::clock();

    printf("<timing>\n");
    printf ( "%32s: %6.1f ms\n", "krnl_part1", (stop_krnl_part1167 - start_krnl_part1167) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_partsupp3", (stop_krnl_partsupp3168 - start_krnl_partsupp3168) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_lineitem5", (stop_krnl_lineitem5169 - start_krnl_lineitem5169) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation7", (stop_krnl_aggregation7170 - start_krnl_aggregation7170) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT171 - start_scanMultiHT171) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_aggregation7_ins", (stop_krnl_aggregation7_ins172 - start_krnl_aggregation7_ins172) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation10", (stop_krnl_nation10173 - start_krnl_nation10173) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "scanMultiHT", (stop_scanMultiHT174 - start_scanMultiHT174) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_nation10_ins", (stop_krnl_nation10_ins175 - start_krnl_nation10_ins175) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "krnl_supplier12", (stop_krnl_supplier12176 - start_krnl_supplier12176) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "finish", (stop_finish177 - start_finish177) / (double) (CLOCKS_PER_SEC / 1000) );
    printf ( "%32s: %6.1f ms\n", "totalKernelTime", (stop_totalKernelTime166 - start_totalKernelTime166) / (double) (CLOCKS_PER_SEC / 1000) );
    printf("</timing>\n");
}
