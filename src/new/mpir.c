/*******************************
 * MPIR
 ******************************/

#ifndef VOLATILE
#if defined(__STDC__) || defined(__cplusplus)
#define VOLATILE volatile
#else
#define VOLATILE
#endif
#endif

VOLATILE int MPIR_debug_gate = 0;

/*******************************
 * End MPIR
 ******************************/
