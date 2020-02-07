#include "util.cuh"


#define HASH_EMPTY 0xffffffffffffffff
#define HASH_MAX   0x7fffffffffffffff


__device__ __forceinline__ uint64_t hash(uint64_t key)
{
    key += ~(key << 32);
    key ^= (key >> 22);
    key += ~(key << 13);
    key ^= (key >> 8);
    key += (key << 3);
    key ^= (key >> 15);
    key += ~(key << 27);
    key ^= (key >> 31);
    return key & (HASH_MAX);
}


__device__ uint64_t stringHash ( str_t s ) {
    uint64_t h = 1;
    unsigned char rem = 0;
    for ( char* c = s.start; c < s.end; ++c ) {
	char exChr = *(c);
        h *= exChr; 
	rem++;
	// apply hash function every 8 byte
	if ( rem%8 == 0 ) {
	    h = hash ( h );
	    rem = 0;
	}
    }
    return h;
}


__device__ uint64_t stringHashPushDown ( bool active, str_t s ) {
    uint64_t hashResult = 1;
    unsigned warplane = threadIdx.x % 32;
    unsigned hashTodoMask = __ballot_sync ( ALL_LANES, active );
    while ( hashTodoMask > 0 ) {
        unsigned strLane = ( __ffs ( hashTodoMask ) - 1 );
        hashTodoMask -= ( 1 << strLane );
        str_t hs = __shfl_sync ( ALL_LANES, s, strLane);
    	char* c = hs.start + warplane;
    	uint32_t hsub = 1;
        while ( c < hs.end ) {
	    hsub *= (*c);
	    c+=32;
        }
	hsub = hash ( hsub );
	uint64_t hash;
        hash  = (uint64_t) __ballot_sync ( ALL_LANES, hsub & 1u );
	hash += (uint64_t) __ballot_sync ( ALL_LANES, hsub & 2u ) << 32;
	if ( warplane == strLane ) {
	    hashResult = hash;
        }
    }
    return hashResult;
}


template <typename T>
struct unique_ht {
    uint64_t hash;
    T payload;
};


// intialize an array as used e.g. for join hash tables
template <typename T>
__global__ void initUniqueHT ( unique_ht<T>* ht, int32_t num ) {
    for (int i = blockIdx.x * blockDim.x + threadIdx.x; i < num; i += blockDim.x * gridDim.x) {
	ht[i].hash = HASH_EMPTY;
    }
}


template <typename T>
__device__ void hashBuildUnique ( unique_ht<T>* hash_table, int ht_size, uint64_t hash, T* payload ) {
    int org_location = hash % ht_size;
    uint32_t location = org_location;
    unique_ht<T>* elem;

    while ( true )  {
        elem = &(hash_table[location]);
        unsigned long long probe_hash = atomicCAS( (unsigned long long*) &(elem->hash), HASH_EMPTY, (unsigned long long)hash );
        if(probe_hash == HASH_EMPTY) {
            elem->payload = *payload;
	    return;
        }
        location = (location + 1) % ht_size;
        if(location == org_location) {
            printf ( "build on full hash table, location: %i\n", location );
            return;
        }
    }
}


template <typename T>
__device__ bool hashProbeUnique ( unique_ht<T>* hash_table, int ht_size, uint64_t hash, int& numLookups, T** payload ) {
    int location;
    while ( numLookups < ht_size )  {
        location = ( hash + numLookups ) % ht_size;
        unique_ht<T>& elem = hash_table[location];
        numLookups++;
        if ( elem.hash == hash ) {
            *payload = &elem.payload;
            return true;
        } else if ( elem.hash == HASH_EMPTY ) {
            return false;
        }
    }
    printf ( "probing full hash table - num lookups: %i\n", numLookups );
    return false;
}


// simplified version of multi ht without locking but with intermediate prefix sum
struct multi_ht {
    uint32_t offset;
    uint32_t count;
};


// intialize an array as used e.g. for join hash tables
__global__ void initMultiHT ( multi_ht* ht, int32_t num ) {
    for (int i = blockIdx.x * blockDim.x + threadIdx.x; i < num; i += blockDim.x * gridDim.x) {
	ht[i].offset = 0xffffffff;
	ht[i].count = 0;
    }
}


// compact scan implementation to allocate entry buckets
__global__ void scanMultiHT ( multi_ht* ht, uint32_t num, int* range_offset ) {
    unsigned int mask = 0xffffffff;
    int warpSize = 32;
    int lane_id = threadIdx.x % warpSize;
    int lim = ( ( num + warpSize - 1 ) / warpSize ) * warpSize;

    for (int i = blockIdx.x * blockDim.x + threadIdx.x; i < lim; i += blockDim.x * gridDim.x) {
        uint32_t val = 0;
        if ( i < num ) 
            val = ht[i].count;
        uint32_t scan = val;
        #pragma unroll
        for (int s = 1; s <= warpSize; s *= 2) {
            int n = __shfl_up_sync ( mask, scan, s, warpSize);
            if (lane_id >= s) scan += n;
        }
        uint32_t glob;
        if (threadIdx.x % warpSize == warpSize - 1) {
            glob = atomicAdd ( range_offset, scan );
        }
        glob = __shfl_sync ( mask, glob, warpSize -1, warpSize );
        if ( i < num ) {
            ht[i].offset = scan - val + glob;
            ht[i].count = 0;
	}
    }
}


// join hash insert: count the number of matching elements
__device__ void hashCountMulti ( multi_ht* ht, int32_t ht_size, uint64_t hash ) {
    int org_location = hash % ht_size;
    uint32_t location = org_location;
    multi_ht* entry = &ht[location];
    atomicAdd ( &(entry->count), 1);
}


// join hash insert: insert elements
template <typename T>
__device__ void hashInsertMulti ( multi_ht* ht, T* payload, int* range_offset, int32_t ht_size, uint64_t hash, T* payl ) {
    uint32_t location = hash % ht_size;
    multi_ht& entry = ht [ location ];
    uint32_t tupleOffset = atomicAdd ( &(entry.count), 1);
    payload [ tupleOffset  + entry.offset ] = *payl;
    return;
}


// join hash probe
__device__ bool hashProbeMulti ( multi_ht* ht, uint32_t ht_size, uint64_t hash, int& offset, int& end ) {
    uint32_t location = hash % ht_size;
    multi_ht& entry = ht [ location ];
    if ( entry.count != 0 ) {
        offset = entry.offset;
        end = offset + entry.count;
        return true;
    } else {
        return false;
    }
}


/*
 A lock that ensures that a section is only executed once.
 E.g. assigning the key to a ht entry
 */
struct OnceLock {

    static const unsigned LOCK_FRESH   = 0;
    static const unsigned LOCK_WORKING = 1;
    static const unsigned LOCK_DONE    = 2;

    volatile unsigned lock;

    __device__ void init() {
        lock = LOCK_FRESH;
    } 

    __device__ bool enter() {
        unsigned lockState = atomicCAS ( (unsigned*) &lock, LOCK_FRESH, LOCK_WORKING );
        return lockState == LOCK_FRESH;
    }

    __device__ void done() {
        __threadfence();
	lock = LOCK_DONE;
        __threadfence();
    }

    __device__ void wait() {
        while ( lock != LOCK_DONE );
    }
};
template <typename T>
struct agg_ht {
    OnceLock lock;
    uint64_t hash;
    T payload;
};


template <typename T>
__global__ void initAggHT ( agg_ht<T>* ht, int32_t num ) {
    for (int i = blockIdx.x * blockDim.x + threadIdx.x; i < num; i += blockDim.x * gridDim.x) {
	ht[i].lock.init();
	ht[i].hash = HASH_EMPTY;
    }
}


// returns candidate bucket
template <typename T>
__device__ int hashAggregateGetBucket ( agg_ht<T>* ht, int32_t ht_size, uint64_t grouphash, int& numLookups, T* payl ) {
    int location=-1;
    bool done=false;
    while ( !done ) {
        location = ( grouphash + numLookups ) % ht_size;
        agg_ht<T>& entry = ht [ location ];
        numLookups++;
        if ( entry.lock.enter() ) {
            entry.payload = *payl;
            entry.hash = grouphash;
            entry.lock.done();
        }
        entry.lock.wait();
        done = (entry.hash == grouphash);
        if ( numLookups == ht_size ) {
            printf ( "hash table full\n" );
            break;
	}
    }
    return location;
}


// return value indicates if more candidate buckets exist
// location used as return value for payload location
template <typename T>
__device__ bool hashAggregateFindBucket ( agg_ht<T>* ht, int32_t ht_size, uint64_t grouphash, int& numLookups, int& location ) {
    location=-1;
    bool done=false;
    while ( !done ) {
        location = ( grouphash + numLookups++ ) % ht_size;
        if ( ht [ location ].hash == HASH_EMPTY ) {
            return false;
        }
        done = ( ht [ location ].hash == grouphash);
        
    }
    return true;
}
