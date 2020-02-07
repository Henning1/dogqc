#pragma once

#define ALL_LANES 0xffffffff

// intialize an array as used e.g. for join hash tables
template<typename T>
__global__ void initArray ( T* array, T value, int num ) {
    for (int i = blockIdx.x * blockDim.x + threadIdx.x; i < num; i += blockDim.x * gridDim.x) {
        array[i] = value;
    }
}

__device__ static float atomicMax(float* address, float val)
{
    int* address_as_i = (int*) address;
    int old = *address_as_i, assumed;
    do {
        assumed = old;
        old = ::atomicCAS(address_as_i, assumed,
            __float_as_int(::fmaxf(val, __int_as_float(assumed))));
    } while (assumed != old);
    return __int_as_float(old);
}

__device__ static float atomicMin(float* address, float val)
{
    int* address_as_i = (int*) address;
    int old = *address_as_i, assumed;
    do {
        assumed = old;
        old = ::atomicCAS(address_as_i, assumed,
            __float_as_int(::fminf(val, __int_as_float(assumed))));
    } while (assumed != old);
    return __int_as_float(old);
}

__device__ static double atomicMax(double* address, double val)
{
    unsigned long long int* address_as_i = (unsigned long long int*) address;
    unsigned long long int old = *address_as_i, assumed;
    do {
        assumed = old;
        old = ::atomicCAS(address_as_i, assumed,
            __double_as_longlong(::fmax(val, __longlong_as_double(assumed))));
    } while (assumed != old);
    return __longlong_as_double(old);
}

__device__ static double atomicMin(double* address, double val)
{
    unsigned long long int* address_as_i = (unsigned long long int*) address;
    unsigned long long int old = *address_as_i, assumed;
    do {
        assumed = old;
        old = ::atomicCAS(address_as_i, assumed,
            __double_as_longlong(::fmin(val, __longlong_as_double(assumed))));
    } while (assumed != old);
    return __longlong_as_double(old);
}

__device__ str_t stringScan ( size_t* offs, char* chrs, int ix ) {
    str_t s;
    s.start = chrs + offs[ix];
    s.end = chrs + offs[ix+1];
    return s;
}

__device__ str_offs toStringOffset ( char* chrs, str_t s ) {
    str_offs o;
    o.start = s.start - chrs;
    o.end = s.end - chrs;
    return o;
}

__device__ str_t stringConstant ( const char* chars, int len ) {
    str_t s;
    s.start = const_cast<char*>(chars);
    s.end = const_cast<char*>(chars) + len;
    return s;

}

__device__ str_t __shfl_sync ( unsigned lanemask, str_t v, int sourceLane ) {
    str_t res;
    res.start = (char*) __shfl_sync ( lanemask, (uint64_t) v.start, sourceLane );
    res.end   = (char*) __shfl_sync ( lanemask, (uint64_t) v.end,   sourceLane );
    return res;
}

__device__ bool stringEquals ( str_t a, str_t b ) {
    int lena = a.end - a.start;
    int lenb = b.end - b.start;
    if ( lena != lenb )
        return false;
    char* c = a.start;
    char* d = b.start;
    for ( ; c < a.end; c++, d++ ) {
        if ( *c != *d ) 
            return false;
    }
    return true;
}

__device__ bool stringEqualsPushdown ( bool active, str_t a, str_t b ) {
    unsigned warplane = threadIdx.x % 32;
    bool equal = active;
    if ( equal ) { 
        equal = ( (a.end - a.start) == (b.end - b.start) );
    }
    unsigned cmpTodoMask = __ballot_sync ( ALL_LANES, equal );
    while ( cmpTodoMask > 0 ) {
        unsigned strLane = ( __ffs ( cmpTodoMask ) - 1 );
        cmpTodoMask -= ( 1 << strLane );
        str_t strA = __shfl_sync ( ALL_LANES, a, strLane);
        str_t strB = __shfl_sync ( ALL_LANES, b, strLane);
        bool currEqual = true;
        char* chrA = strA.start + warplane;
        char* chrB = strB.start + warplane;
        while ( chrA < strA.end && currEqual ) {
            currEqual &= __all_sync ( ALL_LANES, (*chrA) == (*chrB) );
            chrA += 32;
            chrB += 32;
        }
        if ( warplane == strLane ) {
            equal = currEqual;
        }
    }
    return equal || (!active);
}


__device__ str_t stringSubstring ( str_t str, int from, int fr ) {
    str_t res;
    // todo: throw error if for is negative
    res.start = str.start + from - 1;
    res.end = res.start + fr;
    if ( res.start < str.start )
        res.start = str.start;
    if ( res.end > str.end )
        res.end = str.end;
    return res;
}

__inline__ __device__ bool cmpLike ( char c, char l ) {
    return ( c == l ) || ( l == '_' );
}

__device__ bool stringLikeCheck ( str_t string, str_t like ) {
    char *sPos, *lPos, *sTrace, *lTrace;
    char *lInStart = like.start;
    char *lInEnd   = like.end;
    char *sInStart = string.start;
    char *sInEnd   = string.end;

    // prefix 
    if ( *like.start != '%' ) { 
        sPos = string.start;
        lPos = like.start;
        for ( ; lPos < like.end && sPos < string.end && (*lPos) != '%'; ++lPos, ++sPos ) {
            if ( !cmpLike ( *sPos, *lPos ) )
                return false;
        }
        lInStart = lPos; 
        sInStart = sPos; 
    }
    
    // suffix 
    if ( *(like.end-1) != '%' ) {
        sPos = string.end-1;
        lPos = like.end-1;
        for ( ; lPos >= like.start && sPos >= string.start && (*lPos) != '%'; --lPos, --sPos ) {
            if ( !cmpLike ( *sPos, *lPos ) )
                return false;
        }
        lInEnd = lPos;
        sInEnd = sPos+1; // first suffix char 
    }

    // infixes 
    if ( lInStart < lInEnd ) {
        lPos = lInStart+1; // skip '%'
        sPos = sInStart;
        while ( sPos < sInEnd && lPos < lInEnd ) { // loop 's' string
            lTrace = lPos;
            sTrace = sPos;
            while ( cmpLike ( *sTrace, *lTrace ) && sTrace < sInEnd ) { // loop infix matches
                ++lTrace;
                if ( *lTrace == '%' ) {
                    lPos = ++lTrace;
                    sPos = sTrace;
                    break;
                }
                ++sTrace; 
            }
            ++sPos;
        }
    }
    return lPos >= lInEnd;
}

