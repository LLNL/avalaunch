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

int spawn_net_waitany(int num, const spawn_net_channel** chs, int* index)
{
  /* write is a NOP for a null channel */
  if (num == 0) {
    *index = -1;
    return SPAWN_SUCCESS;
  }

  /* check that we got a list of channels */
  if (chs == NULL) {
    return SPAWN_FAILURE;
  }

  /* TODO: need to check that all channels are of the same type */
  return spawn_net_waitany_ib(num, chs, index);

#if 0
  /* otherwise, call write routine for channel type */
  if (ch->type == SPAWN_NET_TYPE_TCP) {
    SPAWN_ERR("spawn_net_waitany unsupported for TCP channels");
  } else if (ch->type == SPAWN_NET_TYPE_FIFO) {
    SPAWN_ERR("spawn_net_waitany unsupported for FIFO channels");
  }
#ifdef HAVE_SPAWN_NET_IBUD
  else if (ch->type == SPAWN_NET_TYPE_IBUD) {
    return spawn_net_waitany_ib(num, chs, index);
  }
#endif
  else {
    SPAWN_ERR("Unknown channel type %d", ch->type);
    return SPAWN_FAILURE;
  }
#endif
}
