
#define ERROR(msg) \
    fprintf(stderr, "ERROR: %s\n", msg); \
    fprintf(stderr, "Line %i of function %s in file %s\n", __LINE__, __func__, __FILE__); \
    exit(EXIT_FAILURE);


struct str_t {
    char* start;
    char* end;
};

struct str_offs {
    size_t start;
    size_t end;
};

void stringPrint ( char* chrs, str_offs s, FILE* fout=stdout, int nchars=10 ) {
    int n = s.end - s.start;
    int i = 0;
    int p = 0; 
 
    for (p=0; p<nchars-n; p++)
        fputc (' ', fout );
        
    for (; p<nchars; p++,i++)
        fputc ( chrs[s.start+i], fout );
}

