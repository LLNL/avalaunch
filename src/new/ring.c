#include "ring.h"
#include "spawn_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
