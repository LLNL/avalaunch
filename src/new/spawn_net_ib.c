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
#include <ib_internal.h>

#include "spawn_internal.h"

extern int my_pg_rank;
extern int my_pg_size;

spawn_net_endpoint* spawn_net_open_ib()
{
    int nchild = 8;

    my_pg_rank = PG_RANK;

    mv2_hca_open();

    spawn_net_endpoint* ep = mv2_init_ud(nchild);
    if (SPAWN_NET_ENDPOINT_NULL == ep) {
        SPAWN_ERR("Error creating IB end point");
        return SPAWN_NET_ENDPOINT_NULL;
    }

    return ep;
}

int spawn_net_close_ib(spawn_net_endpoint** pep)
{
    /* TODO: close down UD endpoint */
}

spawn_net_channel* spawn_net_connect_ib(const char* name)
{
    comm_lock();
    spawn_net_channel* ch = mv2_ep_connect(name);
    if (ch == SPAWN_NET_CHANNEL_NULL) {
        SPAWN_ERR("Error creating IB communication channel");
    }
    comm_unlock();

    return ch;
}

spawn_net_channel* spawn_net_accept_ib(const spawn_net_endpoint* ep)
{
    comm_lock();
    spawn_net_channel* ch = mv2_ep_accept();
    if (ch == SPAWN_NET_CHANNEL_NULL) {
        SPAWN_ERR("Error accepting IB connection");
    }
    comm_unlock();

    return ch;
}

int spawn_net_disconnect_ib(spawn_net_channel** pch)
{
    comm_lock();
    comm_unlock();
    return SPAWN_SUCCESS;
}

int spawn_net_read_ib(const spawn_net_channel* ch, void* buf, size_t size)
{
    comm_lock();
    int ret = mv2_ud_recv(ch, buf, size);
    if (ret < 0) {
        SPAWN_ERR("Couldn't recv IB message");
    }
    comm_unlock();

    return ret;
}

int spawn_net_write_ib(const spawn_net_channel* ch, const void* buf, size_t size)
{
    comm_lock();
    int ret = mv2_ud_send(ch, buf, size);
    if (ret < 0) {
        SPAWN_ERR("Couldn't send IB message");
    }
    comm_unlock();

    return ret;
}
