//=================== file = genzipf.c ======================================
//=  Program to generate data for join experimentation                      =
//=-------------------------------------------------------------------------=
//=  Build: gcc zipfgen.c -lm -std=c99 -o zipfgen                           =
//=  Execute: ./zipfgen --mode 1 --nbuild 10000000 --nprobe 10000000 \      =
//=                     --zalpha 0.75 --zn 10000000 --zsplit 1              =
//=-------------------------------------------------------------------------=
//=  Execute: genzipf                                                       =
//===========================================================================
//=  adaptation: Henning Funke Oct '18                                      =
//=          TU Dortmund University                                         =
//=-------------------------------------------------------------------------=
//=  based on:                                                              =
//=  Author: Kenneth J. Christensen                                         =
//=          University of South Florida                                    =
//=          WWW: http://www.csee.usf.edu/~christen                         =
//=          Email: christen@csee.usf.edu                                   =
//=  History: KJC (11/16/03) - Genesis (from genexp.c)                      =
//===========================================================================


//----- Include files -------------------------------------------------------
#include <assert.h>             // Needed for assert() macro
#include <stdio.h>              // Needed for printf()
#include <stdlib.h>             // Needed for exit() and ato*()
#include <math.h>               // Needed for pow()
#include <time.h>
#include <getopt.h>
#include <stdint.h>
#include <float.h>

//----- Constants -----------------------------------------------------------
#define  FALSE          0       // Boolean false
#define  TRUE           1       // Boolean true

typedef struct {
    double alpha;
    int n;
    double c;
    double *sum_probs;
    int *labels;
} zipf_dist;




//----- Function prototypes -------------------------------------------------
int          zipf(double alpha, int n);  // Returns a Zipf random variable
double       rand_val();
zipf_dist*   generate_zipf_dist ( double alpha, int n );
int          sample_zipf_dist ( zipf_dist* d );
void         free_zipf_dist ( zipf_dist* d);
void         random_permutation ( int* data, int n );

//===== Main program ========================================================
int
main (int argc, char **argv)
{
  // io vars
  FILE   *fp_r;                   // File pointer to output file
  FILE   *fp_s;                   // File pointer to output file
  
  // parameters
  double alpha;                 // Alpha parameter
  double n;                     // N parameter
  int    num_build;             // Number of values
  int    num_probe;             // Number of values
  int    num_split;             // Number of values
  int    mode;

  // tmp vars
  int    zipf_rv;               // Zipf random variable
  int    i;                     // Loop counter
    
  srand ( time ( NULL ) );

  // get command line options
  int c;
  while (1) {
      static struct option long_options[] = {
          /* These options set a flag. */
          {"mode",    required_argument, 0, 'm'},
          {"nbuild",  required_argument, 0, 'b'},
          {"nprobe",  required_argument, 0, 'p'},
          {"zalpha",  required_argument, 0, 'a'},
          {"zn",      required_argument, 0, 'n'},
          {"zsplit",  required_argument, 0, 's'},
          {"seed",    required_argument, 0, 'r'},
          {0, 0, 0, 0}
      };

      /* getopt_long stores the option index here. */
      int option_index = 0;
      c = getopt_long (argc, argv, "m:b:p:a:n:s:r:",
                       long_options, &option_index);

      /* Detect the end of the options. */
      if (c == -1)
        break;

      switch (c) {

        //printf("%s\n", c);
 
        case 0:
          /* If this option set a flag, do nothing else now. */
          if (long_options[option_index].flag != 0)
            break;

        case 'm':
          mode = atoi(optarg);
          break;

        case 'b':
          num_build = atoi(optarg);
          break;

        case 'p':
          num_probe = atoi(optarg);
          break;

        case 'a':
          alpha = atof(optarg);
          break;

        case 'n':
          n = atof(optarg);
          break;
        
        case 's':
          num_split = atoi(optarg);
          break;
        
        case 'r':
          srand ( atoi(optarg) );
          break;

        case '?':
          printf("mode   ( short m )   -   1: zipf distributed build. 2: primary key build. 3: build with 32 instances of each key 4: build with 8 instances of each key.\n");
          printf("nbuild ( short b )   -   number of build elements\n");
          printf("nprobe ( short p )   -   number of probe elements\n");
          printf("zalpha ( short a )   -   alpha for zipf distributions\n");
          printf("zn     ( short n )   -   N for zipf distributions\n");
          printf("zsplit ( short s )   -   number of zipf distributions\n");
          printf("seed   ( short r )   -   random seed (time based if none given)\n");
          return 0;

        default:
          abort ();
      }
  }

  // create/open the files
  fp_r = fopen("r_build.tbl", "w");
  if (fp_r == NULL)
  {
    printf("ERROR in creating output file (%s) \n", "r_build.tbl");
    exit(1);
  }
  fp_s = fopen("s_probe.tbl", "w");
  if (fp_s == NULL)
  {
    printf("ERROR in creating output file (%s) \n", "s_probe.tbl");
    exit(1);
  }

  int* build = malloc ( sizeof ( int ) *  num_build );
  int* probe = malloc ( sizeof ( int ) *  num_probe );

  //1: zipf distributed build. 2: primary key build. 3: build with 32 instances of each key 4: build with 8 instances of each key
  switch ( mode ) {
  case 1: ;
      // generate num_split distributions
      zipf_dist** ds = malloc ( sizeof ( zipf_dist* ) *  num_split );
      for ( int d=0; d<num_split; d++) {
        ds[d] = generate_zipf_dist ( alpha, n );
      }
    
      // Generate and output zipf random variables
      for (i=0; i<num_build; i++)
      {
        zipf_rv = sample_zipf_dist ( ds [ i% num_split ] );
        fprintf(fp_r, "%d|%i|\n", zipf_rv, i);
      }
      
      // Generate and output dense values
      for (i=0; i<num_probe; i++) {
        fprintf(fp_s, "%i|%i|\n", i, i);
      }
      
      // free distributions
      for ( int d=0; d<num_split; d++) {
        free_zipf_dist ( ds[d] );
      }
      free ( ds );
  break;
  case 2:
      // Generate build data
      for (i=0; i<num_build; i++)
      {
          build[i] = i;
      }
      random_permutation ( build, num_build );
      
      // Output build data
      for (i=0; i<num_build; i++)
      {
        fprintf(fp_r, "%i|%i|\n", build[i], i);
      }
      
      // Output probe data
      for (i=0; i<num_probe; i++) {
        fprintf(fp_s, "%i|%i|\n", i, i);
      }
    
  break;
  case 3: {
      // Generate build data
      i = 0;
      int p = 0;
      while ( i < num_build ) {
          int j=0;
          while ( j < 32 & i < num_build ) {
              build[i] = p;
              j++;
              i++;
          }
          p++;
      }
      random_permutation ( build, num_build );
      
      // Output build data
      for (i=0; i<num_build; i++)
      {
        fprintf(fp_r, "%i|%i|\n", build[i], i);
      }
      
      // Output probe data
      //for (i=0; i<num_probe; i++) {
      for (i=0; i<p; i++) {
        //fprintf(fp_s, "%i|%i|\n", rand() % num_probe, i);
        fprintf(fp_s, "%i|%i|\n", i, i);
      }
  }
  break;
  case 4: {
      // Generate build data
      i = 0;
      int p = 0;
      while ( i < num_build ) {
          int j=0;
          while ( j < 8 & i < num_build ) {
              build[i] = p;
              j++;
              i++;
          }
          p++;
      }
      random_permutation ( build, num_build );
      
      // Output build data
      for (i=0; i<num_build; i++)
      {
        fprintf(fp_r, "%i|%i|\n", build[i], i);
      }
      
      // Output probe data
      //for (i=0; i<num_probe; i++) {
      for (i=0; i<p; i++) {
        //fprintf(fp_s, "%i|%i|\n", rand() % num_probe, i);
        fprintf(fp_s, "%i|%i|\n", i, i);
      }
  }
  break;
  }  

  free ( build );
  free ( probe );
  
  fclose(fp_r);
  fclose(fp_s);
}



void random_permutation ( int* data, int n ) {
  for (int i = 1; i < n; i++) {
  	int j, t;
  	j = rand() % (n-i) + i;
  	t = data[j]; data[j] = data[i]; data[i] = t; // Swap i and j
  }
}


zipf_dist* generate_zipf_dist( double alpha, int n ) {

  zipf_dist* d = malloc ( sizeof(zipf_dist) );
  d->sum_probs = malloc ( sizeof(double) * n );
  d->labels = malloc ( sizeof(int) * n );
  d->alpha = alpha;
  d->n = n;

  for (int i=1; i<=n; i++)
    d->c = d->c + (1.0 / pow((double) i, alpha));
  d->c = 1.0 / d->c;

  // Compute normalization constant and probabilities
  d->sum_probs[0] = 0;
  d->labels[0] = 0;
  for (int i=1; i<=d->n; i++) {
    d->sum_probs[i] = d->sum_probs[i-1] + d->c / pow((double) i, d->alpha);
    d->labels[i]=i;
  }

  //random permutation of labels
  random_permutation ( d->labels, n );

  return d;
}

int sample_zipf_dist ( zipf_dist* d ) {
  // Pull a uniform random number (0 < z < 1)
  double z;
  int low, high, mid, zipf_value;
  do
  {
    z = rand_val();
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = d->n, mid;
  do {
    mid = floor((low+high)/2);
    if (d->sum_probs[mid] >= z && d->sum_probs[mid-1] < z) {
      zipf_value = d->labels[mid];
      break;
    } else if (d->sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  if(!((zipf_value >=1) && (zipf_value <= d->n))) {
    printf("error zipf_value: %i, n: %i\n", zipf_value, d->n );
  }
  assert((zipf_value >=1) && (zipf_value <= d->n));

  return(zipf_value);

}

void free_zipf_dist ( zipf_dist* d) {
  free ( d->sum_probs );
  free ( d->labels );
  free ( d );

}



//===========================================================================
//=  Function to generate Zipf (power law) distributed random variables     =
//=    - Input: alpha and N                                                 =
//=    - Output: Returns with Zipf distributed random variable              =
//===========================================================================
int zipf(double alpha, int n)
{
  static int first = TRUE;      // Static first time flag
  static double c = 0;          // Normalization constant
  static double *sum_probs;     // Pre-calculated sum of probabilities
  static int *labels;
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int    i;                     // Loop counter
  int low, high, mid;           // Binary-search bounds

  // Compute normalization constant on first call only
  if (first == TRUE)
  {
    for (i=1; i<=n; i++)
      c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;

    sum_probs = malloc((n+1)*sizeof(*sum_probs));
    labels = malloc((n+1)*sizeof(*labels));
    sum_probs[0] = 0;
    labels[0] = 0;
    for (i=1; i<=n; i++) {
      sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
      labels[i]=i;
    }

    //random permutation of labels
    int *perm = labels;
    for (int i = 1; i < n; i++) {
    	int j, t;
    	j = rand() % (n-i) + i;
    	t = perm[j]; perm[j] = perm[i]; perm[i] = t; // Swap i and j
    }

    first = FALSE;
  }

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = rand_val(0);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n, mid;
  do {
    mid = floor((low+high)/2);
    if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
      zipf_value = labels[mid];
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  if(!((zipf_value >=1) && (zipf_value <= n))) {
    printf("error zipf_value: %i, n: %i\n", zipf_value, n );
  }
  assert((zipf_value >=1) && (zipf_value <= n));

  return(zipf_value);
}

double rand_val()
{
    double div = RAND_MAX;
    return (rand() / div);
}
