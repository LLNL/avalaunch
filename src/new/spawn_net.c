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

static int spawn_net_tcp_backlog = 64;

static int spawn_net_open_tcp(spawn_endpoint_t* ep)
{
  /* create a TCP socket */
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    SPAWN_ERR("Failed to create TCP socket (socket() errno=%d %s)", errno, strerror(errno));
    return SPAWN_FAILURE;
  }

  /* prepare socket to be bound to ephemeral port - OS will assign us a free port */
  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  sin.sin_port = htons(0); /* bind ephemeral port - OS will assign us a free port */

  /* bind socket */
  if (bind(fd, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    SPAWN_ERR("Failed to bind socket (bind() errno=%d %s)", errno, strerror(errno));
    close(fd);
    return SPAWN_FAILURE;
  }

  /* listen for connections */
  if (listen(fd, spawn_net_tcp_backlog) < 0) {
    SPAWN_ERR("Failed to set socket to listen (listen() errno=%d %s)", errno, strerror(errno));
    close(fd);
    return SPAWN_FAILURE;
  }

  /* get our port number (set after listen) */
  socklen_t len = sizeof(sin);
  if (getsockname(fd, (struct sockaddr *) &sin, &len) < 0) {
    SPAWN_ERR("Failed to get socket name (getsockname() errno=%d %s)", errno, strerror(errno));
    close(fd);
    return SPAWN_FAILURE;
  }

  /* extract our ip and port */
  char hn[256];
  if (gethostname(hn, sizeof(hn)) < 0) {
    SPAWN_ERR("Failed to get hostname h_errno=%d", h_errno);
    close(fd);
    return SPAWN_FAILURE;
  }
  struct hostent* he = gethostbyname(hn);
  struct in_addr ip = * (struct in_addr *) *(he->h_addr_list);
  unsigned short port = (unsigned short) ntohs(sin.sin_port);

  /* create name and packed address strings */
  char* name = SPAWN_STRDUPF("TCP:%s:%u", inet_ntoa(ip), (unsigned int) port);

  /* store values in endpoint struct */
  ep->type = SPAWN_EP_TYPE_TCP;
  ep->name = name;
  ep->data = (void*)fd;

  return SPAWN_SUCCESS;
}

static int spawn_net_close_tcp(spawn_endpoint_t* ep)
{
  /* close the socket */
  int fd = (int) ep->data;
  if (fd > 0) {
    close(fd);
  }
  ep->data = (void*)-1;

  /* free the name string */
  spawn_free(&ep->name);

  ep->type = SPAWN_EP_TYPE_NULL;

  return SPAWN_SUCCESS;
}

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

  return ep->name;
}
