/*
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-667277.
 * All rights reserved.
 * This file is part of the SpawnNet library.
 * For details, see https://github.com/hpc/spawnnet
 * Please also read this file: LICENSE.TXT.
*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "spawn_internal.h"

spawn_net_endpoint* spawn_net_open(spawn_net_type type)
{
  /* open endpoint */
  if (type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_open_tcp();
  } else if (type == SPAWN_NET_TYPE_FIFO) {
    return spawn_net_open_fifo();
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_open_ib();
  }
#endif
  else {
    SPAWN_ERR("Unknown endpoint type %d", (int)type);
    return SPAWN_NET_ENDPOINT_NULL;
  }
}

int spawn_net_close(spawn_net_endpoint** pep)
{
  /* check that we got a valid pointer */
  if (pep == NULL) {
    SPAWN_ERR("Must pass pointer to endpoint");
    return SPAWN_FAILURE;
  }

  /* get pointer to endpoint */
  spawn_net_endpoint* ep = *pep;

  /* nothing to do with a NULL endpoint */
  if (ep == SPAWN_NET_ENDPOINT_NULL) {
    return SPAWN_SUCCESS;
  }

  /* otherwise, check the endpoint type */
  if (ep->type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_close_tcp(pep);
  } else if (ep->type == SPAWN_NET_TYPE_FIFO) {
    return spawn_net_close_fifo(pep);
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (ep->type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_close_ib(pep);
  }
#endif
  else {
    SPAWN_ERR("Unknown endpoint type %d", (int)ep->type);
    return SPAWN_FAILURE;
  }
}

const char* spawn_net_name(const spawn_net_endpoint* ep)
{
  /* return NULL for a NULL endpoint */
  if (ep == SPAWN_NET_ENDPOINT_NULL) {
    return NULL;
  }

  /* otherwise, return pointer to real name */
  const char* name = ep->name;
  return name;
}

spawn_net_type spawn_net_infer_type(const char* name)
{
  /* return a NULL channel on connect to NULL name */
  if (name == NULL) {
    return SPAWN_NET_TYPE_NULL;
  }

  /* otherwise, determine type and call real connect */
  if (strncmp(name, "TCP:", 4) == 0) {
    return SPAWN_NET_TYPE_TCP;
  } else if (strncmp(name, "FIFO:", 5) == 0) {
    return SPAWN_NET_TYPE_FIFO;
  } else if (strncmp(name, "IBUD:", 5) == 0) {
    return SPAWN_NET_TYPE_IBUD;
  } else {
    return SPAWN_NET_TYPE_NULL;
  }
}

spawn_net_channel* spawn_net_connect(const char* name)
{
  /* infer type by endpoint name */
  spawn_net_type type = spawn_net_infer_type(name);

  /* call appropriate connect routine */
  if (type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_connect_tcp(name);
  } else if (type == SPAWN_NET_TYPE_FIFO) {
    return spawn_net_connect_fifo(name);
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_connect_ib(name);
  }
#endif
  else {
    SPAWN_ERR("Unknown endpoint name format %s", name);
    return SPAWN_NET_CHANNEL_NULL;
  }
}

spawn_net_channel* spawn_net_accept(const spawn_net_endpoint* ep)
{
  /* return a NULL channel on accept of NULL endpoint */
  if (ep == SPAWN_NET_ENDPOINT_NULL) {
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* otherwise, call real accept routine for endpoint type */
  if (ep->type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_accept_tcp(ep);
  } else if (ep->type == SPAWN_NET_TYPE_FIFO) {
    return spawn_net_accept_fifo(ep);
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (ep->type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_accept_ib(ep);
  }
#endif
  else {
    SPAWN_ERR("Unknown endpoint type %d", ep->type);
    return SPAWN_NET_CHANNEL_NULL;
  }
}

int spawn_net_disconnect(spawn_net_channel** pch)
{
  /* check that we got a valid pointer */
  if (pch == NULL) {
    SPAWN_ERR("Must pass pointer to channel");
    return SPAWN_FAILURE;
  }

  /* get pointer to channel */
  spawn_net_channel* ch = *pch;

  /* nothing to do for a NULL channel */
  if (ch == SPAWN_NET_CHANNEL_NULL) {
    return SPAWN_SUCCESS;
  }

  /* otherwise, call close routine for channel type */
  if (ch->type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_disconnect_tcp(pch);
  } else if (ch->type == SPAWN_NET_TYPE_FIFO) {
    return spawn_net_disconnect_fifo(pch);
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (ch->type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_disconnect_ib(pch);
  }
#endif
  else {
    SPAWN_ERR("Unknown channel type %d", ch->type);
    return SPAWN_FAILURE;
  }
}

int spawn_net_read(const spawn_net_channel* ch, void* buf, size_t size)
{
  /* read is a NOP for a null channel */
  if (ch == SPAWN_NET_CHANNEL_NULL) {
    return SPAWN_SUCCESS;
  }

  /* otherwise, call read routine for channel type */
  if (ch->type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_read_tcp(ch, buf, size);
  } else if (ch->type == SPAWN_NET_TYPE_FIFO) {
    return spawn_net_read_fifo(ch, buf, size);
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (ch->type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_read_ib(ch, buf, size);
  }
#endif
  else {
    SPAWN_ERR("Unknown channel type %d", ch->type);
    return SPAWN_FAILURE;
  }
}

int spawn_net_write(const spawn_net_channel* ch, const void* buf, size_t size)
{
  /* write is a NOP for a null channel */
  if (ch == SPAWN_NET_CHANNEL_NULL) {
    return SPAWN_SUCCESS;
  }

  /* otherwise, call write routine for channel type */
  if (ch->type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_write_tcp(ch, buf, size);
  } else if (ch->type == SPAWN_NET_TYPE_FIFO) {
    return spawn_net_write_fifo(ch, buf, size);
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (ch->type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_write_ib(ch, buf, size);
  }
#endif
  else {
    SPAWN_ERR("Unknown channel type %d", ch->type);
    return SPAWN_FAILURE;
  }
}

int spawn_net_wait(
  int neps,
  const spawn_net_endpoint** eps,
  int nchs,
  const spawn_net_channel** chs,
  int* index)
{
  /* check that we got a pointer to a return value */
  if (index == NULL) {
    return SPAWN_FAILURE;
  }

  /* nothing to wait on if num is 0 */
  if (neps == 0 && nchs == 0) {
    *index = -1;
    return SPAWN_SUCCESS;
  }

  /* check that we got a list of endpoints and/or channels */
  if (eps == NULL && chs == NULL) {
    return SPAWN_FAILURE;
  }

  /* TODO: support mixing of channels */

  /* check that all channels are of the same type */
  int type_set = 0;
  spawn_net_type type;
  int i;
  for (i = 0; i < neps; i++) {
    /* get pointer to endpoint */
    const spawn_net_endpoint* ep = eps[i];

    /* skip NULL endpoints */
    if (ep == SPAWN_NET_ENDPOINT_NULL) {
        continue;
    }

    /* set type to the type of the first valid endpoint */
    if (! type_set) {
        type = ep->type;
        type_set = 1;
    }

    /* got a valid endpoint, check its type */
    if (ep->type != type) {
      SPAWN_ERR("Mixture of endpoint/channel types in array");
      return SPAWN_FAILURE;
    }
  }

  for (i = 0; i < nchs; i++) {
    /* get pointer to channel */
    const spawn_net_channel* ch = chs[i];

    /* skip NULL channels */
    if (ch == SPAWN_NET_CHANNEL_NULL) {
        continue;
    }

    /* set type to the type of the first valid channel */
    if (! type_set) {
        type = ch->type;
        type_set = 1;
    }

    /* got a valid channel, check its type */
    if (ch->type != type) {
      SPAWN_ERR("Mixture of endpoint/channel types in array");
      return SPAWN_FAILURE;
    }
  }

  /* otherwise, call write routine for channel type */
  if (type == SPAWN_NET_TYPE_TCP) {
    return spawn_net_wait_tcp(neps, eps, nchs, chs, index);
  }
#if 0
  else if (type == SPAWN_NET_TYPE_FIFO) {
    SPAWN_ERR("spawn_net_wait unsupported for FIFO channels");
  }
#endif
#ifdef HAVE_SPAWN_NET_IBUD
  else if (type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_wait_ib(neps, eps, nchs, chs, index);
  }
#endif
  else {
    SPAWN_ERR("Unknown channel type %d", type);
    return SPAWN_FAILURE;
  }
}
