#include <stdio.h>
#include <stdlib.h>

#include "pmi.h"

int main(int argc, char* argv[])
{
  int rc;

  int spawned;
  PMI_Init(&spawned);

  int rank, ranks, jobid;
  PMI_Get_rank(&rank);
  PMI_Get_size(&ranks);
  PMI_Get_appnum(&jobid);

  //printf("Rank %d, Size %d, Num %d\n", rank, ranks, jobid);
  //fflush(stdout);

  int kvs_len, key_len, val_len;
  PMI_KVS_Get_name_length_max(&kvs_len);
  PMI_KVS_Get_key_length_max(&key_len);
  PMI_KVS_Get_value_length_max(&val_len);

  //printf("Rank %d, kvslen %d, keylen %d, vallen %d\n", rank, kvs_len, key_len, val_len);
  //fflush(stdout);

  char* kvs = malloc(kvs_len);
  char* key = malloc(key_len);
  char* val = malloc(val_len);

  PMI_KVS_Get_my_name(kvs, kvs_len);
  //printf("Rank %d, Name %s\n", rank, kvs);
  //fflush(stdout);

  /* create key and value */
  snprintf(key, key_len, "%d", rank);
  snprintf(val, val_len, "value=%dof%d", rank, ranks);

  rc = PMI_KVS_Put(kvs, key, val);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Put rc=%d\n", rank, rc);
    fflush(stdout);
  }

  rc = PMI_KVS_Commit(kvs);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Commit rc=%d\n", rank, rc);
    fflush(stdout);
  }

  rc = PMI_Barrier();
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Barrier rc=%d\n", rank, rc);
    fflush(stdout);
  }

  /* get keys */
  char* leftkey  = malloc(key_len);
  char* rightkey = malloc(key_len);
  char* leftval  = malloc(val_len);
  char* rightval = malloc(val_len);

  snprintf(leftkey,  key_len, "%d", (rank + ranks - 1) % ranks);
  snprintf(rightkey, key_len, "%d", (rank + ranks + 1) % ranks);

  rc = PMI_KVS_Get(kvs, leftkey, leftval, val_len);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Get rc=%d\n", rank, rc);
    fflush(stdout);
  }

  rc = PMI_KVS_Get(kvs, rightkey, rightval, val_len);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Get rc=%d\n", rank, rc);
    fflush(stdout);
  }

//  printf("Rank %d Left: %s=%s Right: %s=%s\n", rank, leftkey, leftval, rightkey, rightval);
//  fflush(stdout);

  /* create key and value */
  snprintf(key, key_len, "key2-%d", rank);
  snprintf(val, val_len, "%d", rank);

  rc = PMI_KVS_Put(kvs, key, val);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Put rc=%d\n", rank, rc);
    fflush(stdout);
  }

  rc = PMI_KVS_Commit(kvs);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Commit rc=%d\n", rank, rc);
    fflush(stdout);
  }

  rc = PMI_Barrier();
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Barrier rc=%d\n", rank, rc);
    fflush(stdout);
  }

  snprintf(leftkey,  key_len, "key2-%d", (rank + ranks - 1) % ranks);
  snprintf(rightkey, key_len, "key2-%d", (rank + ranks + 1) % ranks);

  rc = PMI_KVS_Get(kvs, leftkey, leftval, val_len);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Get rc=%d\n", rank, rc);
    fflush(stdout);
  }

  rc = PMI_KVS_Get(kvs, rightkey, rightval, val_len);
  if (rc != PMI_SUCCESS) {
    printf("Rank %d PMI_KVS_Get rc=%d\n", rank, rc);
    fflush(stdout);
  }

//  printf("Rank %d Left: %s=%s Right: %s=%s\n", rank, leftkey, leftval, rightkey, rightval);
//  fflush(stdout);

  free(leftkey);
  free(rightkey);
  free(leftval);
  free(rightval);

  free(kvs);
  free(key);
  free(val);

  //printf("Rank %d, Calling finalize\n", rank);
  //fflush(stdout);

  PMI_Finalize();

  return 0;
}
