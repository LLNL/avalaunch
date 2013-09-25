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

int spawn_net_open(spawn_endpoint_type type, spawn_endpoint_t* ep)
{
  /* check that we got a valid pointer */
  if (ep == NULL) {
    SPAWN_ERR("Must pass address to endpoint struct");
    return SPAWN_FAILURE;
  }

  /* initialize endpoint */
  ep->type = SPAWN_EP_TYPE_NULL;
  ep->name = NULL;
  ep->data = NULL;

  /* open endpoint */
  if (type == SPAWN_EP_TYPE_TCP) {
    return spawn_net_open_tcp(ep);
  } else {
    SPAWN_ERR("Unknown endpoint type %d", (int)type);
    return SPAWN_FAILURE;
  }
}

int spawn_net_close(spawn_endpoint_t* ep)
{
  /* check that we got a valid pointer */
  if (ep == NULL) {
    SPAWN_ERR("Must pass address to endpoint struct");
    return SPAWN_FAILURE;
  }

  if (ep->type == SPAWN_EP_TYPE_NULL) {
    /* nothing to do with a NULL endpoint */
    return SPAWN_SUCCESS;
  } else if (ep->type == SPAWN_EP_TYPE_TCP) {
    return spawn_net_close_tcp(ep);
  } else {
    SPAWN_ERR("Unknown endpoint type %d", (int)ep->type);
    return SPAWN_FAILURE;
  }
}

const char* spawn_net_name(const spawn_endpoint_t* ep)
{
  /* check that we got a valid pointer */
  if (ep == NULL) {
    SPAWN_ERR("Must pass address to endpoint struct");
    spawn_exit(1);
  }

  const char* name = ep->name;
  return name;
}

int spawn_net_connect(const char* name, spawn_channel_t* ch)
{
  /* check that we got a valid pointer */
  if (name == NULL) {
    SPAWN_ERR("Must pass pointer to endpoint name");
    return SPAWN_FAILURE;
  }
  if (ch == NULL) {
    SPAWN_ERR("Must pass address to channel struct");
    return SPAWN_FAILURE;
  }

  if (strncmp(name, "TCP:", 4) == 0) {
    return spawn_net_connect_tcp(name, ch);
  } else {
    SPAWN_ERR("Unknown endpoint name format %s", name);
    return SPAWN_FAILURE;
  }

  return SPAWN_FAILURE;
}

int spawn_net_accept(const spawn_endpoint_t* ep, spawn_channel_t* ch)
{
  /* check that we got a valid pointer */
  if (ep == NULL) {
    SPAWN_ERR("Must pass address to endpoint struct");
    return SPAWN_FAILURE;
  }
  if (ch == NULL) {
    SPAWN_ERR("Must pass address to channel struct");
    return SPAWN_FAILURE;
  }

  /* initialize channel */
  ch->type = SPAWN_EP_TYPE_NULL;
  ch->name = NULL;
  ch->data = NULL;

  if (ep->type == SPAWN_EP_TYPE_NULL) {
    /* return a NULL channel for a NULL endpoint */
    return SPAWN_SUCCESS;
  } else if (ep->type == SPAWN_EP_TYPE_TCP) {
    return spawn_net_accept_tcp(ep, ch);
  } else {
    SPAWN_ERR("Unknown endpoint type %d", ep->type);
    return SPAWN_FAILURE;
  }

  return SPAWN_FAILURE;
}

int spawn_net_disconnect(spawn_channel_t* ch)
{
  /* check that we got a valid pointer */
  if (ch == NULL) {
    SPAWN_ERR("Must pass address to channel struct");
    return SPAWN_FAILURE;
  }

  if (ch->type == SPAWN_EP_TYPE_NULL) {
    /* return a NULL channel for a NULL endpoint */
    return SPAWN_SUCCESS;
  } else if (ch->type == SPAWN_EP_TYPE_TCP) {
    return spawn_net_disconnect_tcp(ch);
  } else {
    SPAWN_ERR("Unknown channel type %d", ch->type);
    return SPAWN_FAILURE;
  }

  return SPAWN_FAILURE;
}

int spawn_net_read(const spawn_channel_t* ch, void* buf, size_t size)
{
  /* check that we got a valid pointer */
  if (ch == NULL) {
    SPAWN_ERR("Must pass address to channel struct");
    return SPAWN_FAILURE;
  }

  if (ch->type == SPAWN_EP_TYPE_NULL) {
    /* return a NULL channel for a NULL endpoint */
    return SPAWN_SUCCESS;
  } else if (ch->type == SPAWN_EP_TYPE_TCP) {
    return spawn_net_read_tcp(ch, buf, size);
  } else {
    SPAWN_ERR("Unknown channel type %d", ch->type);
    return SPAWN_FAILURE;
  }

  return SPAWN_FAILURE;
}

int spawn_net_write(const spawn_channel_t* ch, const void* buf, size_t size)
{
  /* check that we got a valid pointer */
  if (ch == NULL) {
    SPAWN_ERR("Must pass address to channel struct");
    return SPAWN_FAILURE;
  }

  if (ch->type == SPAWN_EP_TYPE_NULL) {
    /* return a NULL channel for a NULL endpoint */
    return SPAWN_SUCCESS;
  } else if (ch->type == SPAWN_EP_TYPE_TCP) {
    return spawn_net_write_tcp(ch, buf, size);
  } else {
    SPAWN_ERR("Unknown channel type %d", ch->type);
    return SPAWN_FAILURE;
  }

  return SPAWN_FAILURE;
}
