#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <omp.h>

#define xstr(s) str(s)
#define str(s) #s
#define REP 5

void usage(char* x) {
	printf("Usage: -n <n> -r <seed> [-a]\n\twhere the buffer size is given by 2^(n), the random seed initialization by seed and the -a flag used for running multiple cycles and print the average\n");
	exit(__LINE__);
}

int main(int argc, char* argv[]) {

#ifdef dtype

	char ch;
	int i, r, avg_flag=0;
	while((ch = getopt(argc, argv, "n:r:ah")) != EOF) {
#define CHECKRTYPE(exitval,opt) {						\
		if (exitval == gread) prexit("Unexpected option -%c!\n", opt);	\
		else gread = !exitval;						\
	}
		switch (ch) {
			//BC approx  c param is the costanst used in Bader stopping cretierion 
			case 'n' : 	sscanf(optarg, "%d", &i); break;
			case 'r' :	sscanf(optarg, "%u", &r); break;
			case 'a' :  avg_flag = 1; break;
			case 'e' :  exit(1);
			case 'h':
			case '?':
			default:
				usage(argv[0]);
				exit(EXIT_FAILURE);
		}
#undef CHECKRTYPE
	}

//	int i = atoi(argv[1]);
//	int r = atoi(argv[2]);
	long long int N = 1 << i;
	srand(r);

	printf("Threads: %d\n", omp_get_max_threads());
	printf("REP: %d\n", REP);
    printf("N: %d\n", N);
    printf("r: %d\n", r);

	dtype *a, *b, *c;
	a = (dtype*) malloc(sizeof(dtype)*N);
	b = (dtype*) malloc(sizeof(dtype)*N);
	c = (dtype*) malloc(sizeof(dtype)*N);

	#pragma omp parallel for
	for (int i=0; i<N; i++) {
		a[i] = rand();
		b[i] = rand();
		c[i] = 0;
	}

	double avg = 0.0;
	double elapsedTime;
        struct timeval t1, t2;

	for (int j=0; j<REP; j++) {	
		
		gettimeofday(&t1, NULL);
		#pragma omp parallel for
		for (int i=0; i<N; i++)
			c[i] = a[i] + b[i];
		gettimeofday(&t2, NULL);

		// compute and print the elapsed time in millisec
		elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;      // sec to ms
	    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0;   // us to ms
    	
		int nBytes = N*sizeof(dtype);
		int nOps = N;
	
	
		printf("dtype: %s, buffSize: %d B, time: %lf ms, arithmetic: %lf FLOP/s\n", xstr( dtype ), nBytes, elapsedTime, (double)nOps/(elapsedTime*1000));

		if (avg_flag) 
			avg += elapsedTime;
		else
			break;
	
	}

	if (avg_flag) avg /= REP;
	if (avg_flag) printf("Average time over %d repetitions: %lf\n", REP, avg);

	free(a);
	free(b);
	free(c);
	return(0);
#else
	printf("ERROR: you must compile the program by difining the datatype with -Ddtype=<type>\n");
	return(__LINE__);
#endif
}