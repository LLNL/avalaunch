#ifndef SPAWN_NET_H
#define SPAWN_NET_H

#include <stdlib.h>
#include "list.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPAWN_SUCCESS (0)
#define SPAWN_FAILURE (1)

#define SPAWN_NET_ENDPOINT_NULL (NULL)
#define SPAWN_NET_CHANNEL_NULL (NULL)

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
  int type;                 /* network type for channel */
  const char* name;         /* printable name of channel */
  void* data;               /* network-specific data */
  struct list_head list;    /* circularly-linked list of channels */
} spawn_net_channel;

/* represents a channel group */
typedef struct spawn_net_channel_group_struct {
  int type;                     /* network type for channel group */
  int size;                     /* size of channel group */
  struct list_head chlist;      /* pointer to list of channels in group*/
} spawn_net_channel_group;

/* open endpoint for listening */
spawn_net_endpoint* spawn_net_open(spawn_net_type type);

/* close listening endpoint */
int spawn_net_close(spawn_net_endpoint** ep);

/* get name of opened endpoint (pass to others for call to connect) */
const char* spawn_net_name(const spawn_net_endpoint* ep);

/* connect to named endpoint (name comes from spawn_net_name) */
spawn_net_channel* spawn_net_connect(const char* name);

/* accept connection on endpoint */
spawn_net_channel* spawn_net_accept(const spawn_net_endpoint* ep);

/* close connection */
int spawn_net_disconnect(spawn_net_channel** ch);

/* read size bytes from connection into buffer */
int spawn_net_read(const spawn_net_channel* ch, void* buf, size_t size);

/* write size bytes from buffer into connection */
int spawn_net_write(const spawn_net_channel* ch, const void* buf, size_t size);

/* initialize a channel group */
int spawn_net_chgrp_init(spawn_net_channel_group* chgrp, int type);

/* get size of the channel group */
int spawn_net_chgrp_getsize(spawn_net_channel_group* chgrp);

/* add children channels to the channel group */
int spawn_net_chgrp_add(spawn_net_channel_group* chgrp, spawn_net_channel* ch);

/* multicast a message to all end-points connected via channel group  */
int spawn_net_mcast(const void* buf,
                    size_t size,
                    spawn_net_channel* parent_ch,
                    spawn_net_channel_group* chgrp,
                    int root);

/* TODO: isend/irecv/waitall */

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_NET_H */
