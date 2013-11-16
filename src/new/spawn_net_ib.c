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

#include "spawn_internal.h"

#include "mv2_spawn_net_ud.h"

spawn_net_endpoint* spawn_net_open_ib()
{
    mv2_hca_open();
    spawn_net_endpoint* ep = mv2_init_ud();

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
    comm_unlock();

    return ch;
}

spawn_net_channel* spawn_net_accept_ib(const spawn_net_endpoint* ep)
{
    comm_lock();
    spawn_net_channel* ch = mv2_ep_accept();
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
    /* get pointer to vc from channel data field */
    MPIDI_VC_t* vc = (MPIDI_VC_t*) ch->data;
    if (vc == NULL) {
        return SPAWN_FAILURE;
    }

    comm_lock();
    int ret = mv2_ud_recv(vc, buf, size);
    comm_unlock();

    return ret;
}

int spawn_net_write_ib(const spawn_net_channel* ch, const void* buf, size_t size)
{
    /* get pointer to vc from channel data field */
    MPIDI_VC_t* vc = (MPIDI_VC_t*) ch->data;
    if (vc == NULL) {
        return SPAWN_FAILURE;
    }

    comm_lock();
    int ret = mv2_ud_send(vc, buf, size);
    comm_unlock();

    return ret;
}
