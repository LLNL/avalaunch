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
    spawn_net_endpoint *ep = NULL;

    my_pg_rank = PG_RANK;

    mv2_hca_open();

    ep = mv2_init_ud(nchild);
    if (NULL == ep) {
        fprintf(stderr, "Error creating end point\n");
        return NULL;
    }

    return ep;
}

int spawn_net_close_ib(spawn_net_endpoint** pep)
{
    /* TODO: close down UD endpoint */
}

spawn_net_channel* spawn_net_connect_ib(const char* name)
{
    spawn_net_channel *ch = NULL;

    comm_lock();
    ch = mv2_ep_connect(name);
    if (NULL == ch) {
        fprintf(stderr, "Error creating communication channel\n");
        comm_unlock();
        return NULL;
    }
    comm_unlock();

    return ch;
}

spawn_net_channel* spawn_net_accept_ib(const spawn_net_endpoint* ep)
{
    spawn_net_channel *ch = NULL;

    comm_lock();
    ch = mv2_ep_accept();
    if (NULL == ch) {
        fprintf(stderr, "Error accepting connection\n");
        comm_unlock();
        return NULL;
    }
    comm_unlock();

    return ch;
}

int spawn_net_disconnect_ib(spawn_net_channel** pch)
{
    comm_lock();
    comm_unlock();
    return 0;
}

int spawn_net_read_ib(const spawn_net_channel* ch, void* buf, size_t size)
{
    int ret = 0;

    comm_lock();
    ret = mv2_ud_recv(ch, buf, size);
    if (ret < 0) {
        fprintf(stderr, "Couldn't send message\n");
        comm_unlock();
        return ret;
    }
    comm_unlock();

    return ret;
}

int spawn_net_write_ib(const spawn_net_channel* ch, const void* buf, size_t size)
{
    int ret = 0;

    comm_lock();
    ret = mv2_ud_send(ch, buf, size);
    if (ret < 0) {
        fprintf(stderr, "Couldn't send message\n");
        comm_unlock();
        return ret;
    }
    comm_unlock();

    return ret;
}
