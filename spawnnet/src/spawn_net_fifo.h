#ifndef SPAWN_NET_FIFO_H
#define SPAWN_NET_FIFO_H

#include "spawn_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

spawn_net_endpoint* spawn_net_open_fifo();

int spawn_net_close_fifo(spawn_net_endpoint** pep);

spawn_net_channel* spawn_net_connect_fifo(const char* name);

spawn_net_channel* spawn_net_accept_fifo(const spawn_net_endpoint* ep);

int spawn_net_disconnect_fifo(spawn_net_channel** pch);

int spawn_net_read_fifo(const spawn_net_channel* ch, void* buf, size_t size);

int spawn_net_write_fifo(const spawn_net_channel* ch, const void* buf, size_t size);

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_NET_FIFO_H */
