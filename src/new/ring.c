#include "ring.h"
#include "spawn_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

extern VOLATILE int MPIR_debug_gate;

/*******************************
 * End MPIR
 ******************************/

/* simple library that exchanges variable length data
 * among procs to create a ring */
int ring_create(
  const char* addr,
  uint64_t* rank,
  uint64_t* ranks,
  char** left,
  char** right)
{
  char* value;

  /* if being debugged, wait for debugger to attach */
  if ((value = getenv("MV2_MPIR")) != NULL) {
    while (MPIR_debug_gate == 0);
  }

  /* read server addr */
  char* server_name = NULL;
  if ((value = getenv("MV2_PMI_ADDR")) != NULL) {
    server_name = SPAWN_STRDUP(value);
  }
  if (server_name == NULL) {
    return RING_FAILURE;
  }

  /* create an endpoint */
  spawn_net_type type = spawn_net_infer_type(server_name);
  spawn_net_endpoint* ep = spawn_net_open(type);

  /* connect to server */
  spawn_net_channel* ch = spawn_net_connect(server_name);
  if (ch == SPAWN_NET_CHANNEL_NULL) {
    spawn_net_close(&ep);
    return RING_FAILURE;
  }

  /* create a strmap with our address */
  strmap* map = strmap_new();
  strmap_set(map, "ADDR", addr);

  /* send strmap to server */
  spawn_net_write_strmap(ch, map);

  /* read strmap from server */
  spawn_net_read_strmap(ch, map);

  /* extract values from map */
  const char* rank_str  = strmap_get(map, "RANK");
  const char* ranks_str = strmap_get(map, "RANKS");
  const char* left_str  = strmap_get(map, "LEFT");
  const char* right_str = strmap_get(map, "RIGHT");

  /* set output params */
  *rank  = atoi(rank_str);
  *ranks = atoi(ranks_str);
  *left  = SPAWN_STRDUP(left_str);
  *right = SPAWN_STRDUP(right_str);

  /* delete the strmap */
  strmap_delete(&map);

  /* disconnect channel */
  spawn_net_disconnect(&ch);

  /* close endpoint */
  spawn_net_close(&ep);

  /* free the server name */
  spawn_free(&server_name);

  return RING_SUCCESS;
}

/* simple library that exchanges variable length data
 * among procs to create a ring */
int ring_create2(
  const char* addr,
  uint64_t* rank,
  uint64_t* ranks,
  char** left,
  char** right)
{
  char* value;

  /* if being debugged, wait for debugger to attach */
  if ((value = getenv("MV2_MPIR")) != NULL) {
    while (MPIR_debug_gate == 0);
  }

  /*********************
   * Open connection
   ********************/
 
  /* read server addr */
  char* server_name = NULL;
  if ((value = getenv("MV2_PMI_ADDR")) != NULL) {
    server_name = SPAWN_STRDUP(value);
  }
  if (server_name == NULL) {
    return RING_FAILURE;
  }

  /* create an endpoint */
  spawn_net_type type = spawn_net_infer_type(server_name);
  spawn_net_endpoint* ep = spawn_net_open(type);
  if (ep == SPAWN_NET_ENDPOINT_NULL) {
    spawn_free(&server_name);
    return RING_FAILURE;
  }

  /* connect to server */
  spawn_net_channel* ch = spawn_net_connect(server_name);
  if (ch == SPAWN_NET_CHANNEL_NULL) {
    spawn_free(&server_name);
    spawn_net_close(&ep);
    return RING_FAILURE;
  }

  /*********************
   * Send PMI_INIT
   ********************/
 
  /* create PMI_INIT message */
  strmap* map = strmap_new();
  strmap_set(map, "MSG", "PMI_INIT");

  /* write init message to server */
  spawn_net_write_strmap(ch, map);

  /* read init reply from server */
  spawn_net_read_strmap(ch, map);

  /* get our rank and ranks from reply */
  const char* rank_str  = strmap_get(map, "RANK");
  const char* ranks_str = strmap_get(map, "RANKS");

  /* set output params */
  *rank  = atoi(rank_str);
  *ranks = atoi(ranks_str);

  /* delete the init message */
  strmap_delete(&map);

  /*********************
   * Send PMI_RING_IN
   ********************/
 
  /* create a message for ring exchange */
  map = strmap_new();
  strmap_set(map, "MSG",  "PMI_RING_IN");
  strmap_set(map, "LEFT",  addr);
  strmap_set(map, "RIGHT", addr);

  /* send strmap to server */
  spawn_net_write_strmap(ch, map);

  /* read strmap from server */
  spawn_net_read_strmap(ch, map);

  /* extract values from map */
  const char* left_str  = strmap_get(map, "LEFT");
  const char* right_str = strmap_get(map, "RIGHT");

  /* set output params */
  *left  = SPAWN_STRDUP(left_str);
  *right = SPAWN_STRDUP(right_str);

  /* delete the strmap */
  strmap_delete(&map);

  /*********************
   * Send PMI_FINALIZE
   ********************/
 
  /* create PMI Finalize message */
  map = strmap_new();
  strmap_set(map, "MSG", "PMI_FINALIZE");

  /* send strmap to server */
  spawn_net_write_strmap(ch, map);

  /* delete the strmap */
  strmap_delete(&map);

  /*********************
   * Shut down connection
   ********************/
 
  /* disconnect channel */
  spawn_net_disconnect(&ch);

  /* close endpoint */
  spawn_net_close(&ep);

  /* free the server name */
  spawn_free(&server_name);

  return RING_SUCCESS;
}
