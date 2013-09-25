#ifndef SPAWN_NET_H
#define SPAWN_NET_H

#ifdef __cplusplus
extern "C" {
#endif

#define SPAWN_SUCCESS (0)
#define SPAWN_FAILURE (1)

typedef enum spawn_net_type_enum {
  SPAWN_NET_TYPE_NULL = 0, /* netowrk not defined */
  SPAWN_NET_TYPE_TCP  = 1, /* TCP sockets */
} spawn_net_type;

/* represents an endpoint which others may connect to */
typedef struct spawn_net_endpoint_struct {
  int type;         /* network type for endpoint */
  const char* name; /* address of endpoint */
  void* data;       /* network-specific data */
} spawn_net_endpoint;

/* represents an open, reliable channel between two endpoints */
typedef struct spawn_net_channel_struct {
  int type;         /* network type for channel */
  const char* name; /* printable name of channel */
  void* data;       /* network-specific data */
} spawn_net_channel;

/* open endpoint for listening */
int spawn_net_open(spawn_net_type type, spawn_net_endpoint* ep);

/* close listening endpoint */
int spawn_net_close(spawn_net_endpoint* ep);

/* get name of opened endpoint (pass to others for call to connect) */
const char* spawn_net_name(const spawn_net_endpoint* ep);

/* connect to named endpoint (name comes from spawn_net_name) */
int spawn_net_connect(const char* name, spawn_net_channel* ch);

/* accept connection on endpoint */
int spawn_net_accept(const spawn_net_endpoint* ep, spawn_net_channel* ch);

/* close connection */
int spawn_net_disconnect(spawn_net_channel* ch);

/* read size bytes from connection into buffer */
int spawn_net_read(const spawn_net_channel* ch, void* buf, size_t size);

/* write size bytes from buffer into connection */
int spawn_net_write(const spawn_net_channel* ch, const void* buf, size_t size);

/* TODO: isend/irecv/waitall */

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_NET_H */
