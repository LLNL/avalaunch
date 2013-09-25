#ifndef SPAWN_NET_TCP_H
#define SPAWN_NET_TCP_H

#include "spawn_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

int spawn_net_open_tcp(spawn_endpoint_t* ep);

int spawn_net_close_tcp(spawn_endpoint_t* ep);

int spawn_net_accept_tcp(const spawn_endpoint_t* ep, spawn_channel_t* ch);

int spawn_net_disconnect_tcp(spawn_channel_t* ch);

int spawn_net_read_tcp(const spawn_channel_t* ch, void* buf, size_t size);

int spawn_net_write_tcp(const spawn_channel_t* ch, const void* buf, size_t size);

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_NET_TCP_H */
