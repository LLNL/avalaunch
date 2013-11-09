/* Copyright (c) 2001-2013, The Ohio State University. All rights
 * reserved.
 *
 * This file is part of the MVAPICH2 software package developed by the
 * team members of The Ohio State University's Network-Based Computing
 * Laboratory (NBCL), headed by Professor Dhabaleswar K. (DK) Panda.
 *
 * For detailed copyright and licensing information, please refer to the
 * copyright file COPYRIGHT in the top level MVAPICH2 directory.
 *
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "debug_utils.h"

// Prefix to distinguish output from different processes
#define OUTPUT_PREFIX_LENGTH 256
char output_prefix[OUTPUT_PREFIX_LENGTH] = "";

void set_output_prefix( char* prefix ) {
    strncpy( output_prefix, prefix, OUTPUT_PREFIX_LENGTH );
    output_prefix[OUTPUT_PREFIX_LENGTH-1]= '\0';
}

const char *get_output_prefix() {
    return output_prefix;
}



// Verbosity level for fork/kill/waitpid operations in mpirun_rsh and mpispawn
int DEBUG_Fork_verbose = 0;

// Verbosity level for Fault Tolerance operations
int DEBUG_FT_verbose = 0;

// Verbosity level for Checkpoint/Restart operations
int DEBUG_CR_verbose = 0;

// Verbosity level for Migration operations
int DEBUG_MIG_verbose = 0;

// Verbosity level for UD flow control
int DEBUG_UD_verbose = 0;

// Verbosity level for UD ZCOPY Rndv
int DEBUG_ZCY_verbose = 0;

// Verbosity level for On-Demand Connection Management
int DEBUG_CM_verbose = 0;

// Verbosity level for XRC.
int DEBUG_XRC_verbose = 0;

// Verbosity level for UD stats
int DEBUG_UDSTAT_verbose = 0;

// Verbosity level for memory stats
int DEBUG_MEM_verbose = 0;

// Verbosity level for GPU CUDA
int DEBUG_CUDA_verbose = 0;

// Verbosity level for IB MULTICAST
int DEBUG_MCST_verbose = 0;

// Verbosity level for SHMEM Collectives
int DEBUG_SHM_verbose;

static inline int env2int (char *name)
{
    char* env_str = getenv( name );
    if ( env_str == NULL ) {
        return 0;
    } else {
        return atoi( env_str );
    }
}


// Initialize the verbosity level of the above variables
int initialize_debug_variables() {
    DEBUG_Fork_verbose = env2int( "MV2_DEBUG_FORK_VERBOSE" );
    DEBUG_FT_verbose = env2int( "MV2_DEBUG_FT_VERBOSE" );
    DEBUG_CR_verbose = env2int( "MV2_DEBUG_CR_VERBOSE" );
    DEBUG_MIG_verbose = env2int( "MV2_DEBUG_MIG_VERBOSE" );
    DEBUG_UD_verbose = env2int( "MV2_DEBUG_UD_VERBOSE" );
    DEBUG_ZCY_verbose = env2int( "MV2_DEBUG_ZCOPY_VERBOSE" );
    DEBUG_CM_verbose = env2int( "MV2_DEBUG_CM_VERBOSE" );
    DEBUG_XRC_verbose = env2int( "MV2_DEBUG_XRC_VERBOSE" );
    DEBUG_UDSTAT_verbose = env2int( "MV2_DEBUG_UDSTAT_VERBOSE" );
    DEBUG_MEM_verbose = env2int( "MV2_DEBUG_MEM_USAGE_VERBOSE" );
    DEBUG_CUDA_verbose = env2int( "MV2_DEBUG_CUDA_VERBOSE" );
    DEBUG_MCST_verbose = env2int( "MV2_DEBUG_MCST_VERBOSE" );
    DEBUG_SHM_verbose = env2int( "MV2_DEBUG_SHM_VERBOSE" );
    return 0;
}

void mv2_print_mem_usage()
{
    FILE *file = fopen ("/proc/self/status", "r");
    char vmpeak[100], vmhwm[100];

    if ( file != NULL ) {
        char line[100];
        while (fgets(line, 100, file) != NULL) {
            if (strstr(line, "VmPeak") != NULL) {
                strcpy(vmpeak, line);
                vmpeak[strcspn(vmpeak, "\n")] = '\0';
            }
            if (strstr(line, "VmHWM") != NULL) {
                strcpy(vmhwm, line);
                vmhwm[strcspn(vmhwm, "\n")] = '\0';
            }
        }
        PRINT_INFO(DEBUG_MEM_verbose, "%s %s\n", vmpeak, vmhwm);
        fclose(file);
    } else {
        PRINT_INFO(DEBUG_MEM_verbose, "Status file could not be opened \n");
    }
}
