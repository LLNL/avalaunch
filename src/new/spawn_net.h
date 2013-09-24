#ifndef SPAWN_NET_H
#define SPAWN_NET_H

#ifdef __cplusplus
extern "C" {
#endif

#define SPAWN_SUCCESS (0)
#define SPAWN_FAILURE (1)

typedef enum spawn_endpoint_type_enum {
  SPAWN_EP_TYPE_NULL = 0,
  SPAWN_EP_TYPE_TCP  = 1,
} spawn_endpoint_type;

typedef struct spawn_endpoint_struct {
  int type;
  const char* name;
  void* data;
} spawn_endpoint_t;

typedef struct spawn_channel_struct {
  int type;
  const char* str;
  void* data;
} spawn_channel_t;

/* open endpoint for listening */
int spawn_net_open(spawn_endpoint_type type, spawn_endpoint_t* ep);

/* close endpoint */
int spawn_net_close(spawn_endpoint_t* ep);

/* get endpoint name */
const char* spawn_net_name(const spawn_endpoint_t* ep);

/* connect to named endpoint */
int spawn_net_connect(const char* name, spawn_channel_t* channel);

/* accept connection on endpoint */
int spawn_net_accept(const spawn_endpoint_t* ep, spawn_channel_t* channel);

/* close connection */
int spawn_net_disconnect(spawn_channel_t* channel);

/* TODO: isend/irecv/waitall */
/* send */
/* recv */

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_NET_H */
