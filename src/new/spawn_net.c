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

/* allocates the name of a socket */
static char* spawn_net_get_local_sockname(int fd)
{
  /* get our hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) < 0) {
    SPAWN_ERR("Failed gethostname()");
    return NULL;
  }

  /* struct to record socket addr info */
  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));

  /* get local socket address */
  socklen_t len = sizeof(sin);
  if (getsockname(fd, (struct sockaddr *) &sin, &len) < 0) {
    SPAWN_ERR("Failed to get socket name (getsockname() errno=%d %s)", errno, strerror(errno));
    return NULL;
  }

  /* create name */
  int host_len = (int) strlen(hostname);
  struct in_addr ip = sin.sin_addr;
  unsigned short port = (unsigned short) ntohs(sin.sin_port);
  char* name = SPAWN_STRDUPF("TCP:%d:%s:%s:%u", host_len, hostname, inet_ntoa(ip), (unsigned int) port);
  return name;
}

static char* spawn_net_get_remote_sockname(int fd, const char* host)
{
  /* struct to record socket addr info */
  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));

  /* get peer's address info */
  socklen_t len = sizeof(sin);
  if (getpeername(fd, (struct sockaddr *) &sin, &len) < 0) {
    SPAWN_ERR("Failed to get socket name (getpeername() errno=%d %s)", errno, strerror(errno));
    return NULL;
  }

  /* create name */
  int host_len = (int) strlen(host);
  struct in_addr ip = sin.sin_addr;
  unsigned short port = (unsigned short) ntohs(sin.sin_port);
  char* name = SPAWN_STRDUPF("TCP:%d:%s:%s:%u", host_len, host, inet_ntoa(ip), (unsigned int) port);
  return name;
}

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

  /* get our hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) < 0) {
    SPAWN_ERR("Failed gethostname()");
    close(fd);
    return SPAWN_FAILURE;
  }

  /* get our ip address */
  struct hostent* he = gethostbyname(hostname);
  if (he == NULL) {
    SPAWN_ERR("Failed gethostbyname()");
    close(fd);
    return SPAWN_FAILURE;
  }
  struct in_addr ip = *(struct in_addr *) *(he->h_addr_list);

  /* get our port */
  memset(&sin, 0, sizeof(sin));
  socklen_t len = sizeof(sin);
  if (getsockname(fd, (struct sockaddr *) &sin, &len) < 0) {
    SPAWN_ERR("Failed to get socket name (getsockname() errno=%d %s)", errno, strerror(errno));
    close(fd);
    return SPAWN_FAILURE;
  }
  unsigned short port = (unsigned short) ntohs(sin.sin_port);

  /* create name and packed address strings */
  int host_len = (int) strlen(hostname);
  char* name = SPAWN_STRDUPF("TCP:%d:%s:%s:%u", host_len, hostname, inet_ntoa(ip), (unsigned int) port);

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

int spawn_net_connect_tcp(const char* name, spawn_channel_t* ch)
{
  /* verify that the address string starts with correct prefix */
  if (strncmp(name, "TCP:", 4) != 0) {
    SPAWN_ERR("Endpoint name is not TCP format %s", name);
    return SPAWN_FAILURE;
  }

  /* make a copy of name we can change */
  char* name_copy = strdup(name);
  if (name_copy == NULL) {
    SPAWN_ERR("Failed to copy name");
    return SPAWN_FAILURE;
  }

  /* advance past TCP: */
  char* ptr = name_copy;
  ptr += 4;

  /* pick out length of hostname */
  char* host_len_str = ptr;
  while (*ptr != ':') {
    ptr++;
  }
  *ptr = '\0';
  ptr++;

  /* set hostname and skip to ip address */
  char* host_str = ptr;
  int host_len = atoi(host_len_str);
  ptr += host_len;
  *ptr = '\0';
  ptr++;

  /* get ip string and advance to port */
  char* ip_str = ptr;
  while (*ptr != ':') {
    ptr++;
  }
  *ptr = '\0';
  ptr++;

  /* finally get port string */
  char* port_str = ptr;

  /* convert ip address string */
  struct in_addr ip;
  inet_aton(ip_str, &ip);

  /* convert port string */
  unsigned short port = (unsigned short) atoi(port_str);

  //printf("Host=%s, IP=%s, port=%s(%u)\n", host_str, ip_str, port_str, (unsigned int)port);

  /* set up address to connect to */
  struct sockaddr_in sockaddr;
  sockaddr.sin_family = AF_INET;
  sockaddr.sin_addr = ip;
  sockaddr.sin_port = htons(port);

  /* create a socket */
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    SPAWN_ERR("Failed to create socket for %s (socket() errno=%d %s)", name, errno, strerror(errno));
    free(name_copy);
    return SPAWN_FAILURE;
  }

  /* connect */
  int rc = connect(fd, (const struct sockaddr*) &sockaddr, sizeof(struct sockaddr_in));
  if (rc < 0) {
    SPAWN_ERR("Failed to connect to %s (connect() errno=%d %s)", name, errno, strerror(errno));
    close(fd);
    free(name_copy);
    return SPAWN_FAILURE;
  }

  /* create channel name */
  char* local_name = spawn_net_get_local_sockname(fd);
  char* remote_name = spawn_net_get_remote_sockname(fd, host_str);
  char* ch_name = SPAWN_STRDUPF("%s --> %s", local_name, remote_name);
  spawn_free(&remote_name);
  spawn_free(&local_name);

  ch->type = SPAWN_EP_TYPE_TCP;
  ch->name = ch_name;
  ch->data = (void*)fd;

  /* free our copy */
  free(name_copy);
  name_copy = NULL;

  return SPAWN_SUCCESS;
}

int spawn_net_accept_tcp(const spawn_endpoint_t* ep, spawn_channel_t* ch)
{
  int listenfd = (int)ep->data;

  /* accept an incoming connection request */
  struct sockaddr incoming_addr;
  socklen_t incoming_len = sizeof(incoming_addr);
  int fd = accept(listenfd, &incoming_addr, &incoming_len);
  if (fd < 0) {
    SPAWN_ERR("Failed to connect to %s (connect() errno=%d %s)", ep->name, errno, strerror(errno));
    return SPAWN_FAILURE;
  }

  /* get local socket name */
  char* local_name  = spawn_net_get_local_sockname(fd);
  char* remote_name = spawn_net_get_remote_sockname(fd, "remote");
  char* ch_name = SPAWN_STRDUPF("%s --> %s", local_name, remote_name);
  spawn_free(&remote_name);
  spawn_free(&local_name);

  /* set channel parameters */
  ch->type = SPAWN_EP_TYPE_TCP;
  ch->name = ch_name;
  ch->data = (void*)fd;

  return SPAWN_SUCCESS;
}

static int spawn_net_disconnect_tcp(spawn_channel_t* ch)
{
  /* close the socket */
  int fd = (int) ch->data;
  if (fd > 0) {
    close(fd);
  }
  ch->data = (void*)-1;

  /* free the name string */
  spawn_free(&ch->name);

  ch->type = SPAWN_EP_TYPE_NULL;

  return SPAWN_SUCCESS;
}

static int spawn_net_read_tcp(const spawn_channel_t* ch, void* buf, size_t size)
{
  /* close the socket */
  int fd = (int) ch->data;
  if (fd > 0) {
    size_t total = 0;
    char* ptr = (char*) buf;
    while (total < size) {
      size_t remaining = size - total;
      ssize_t count = read(fd, ptr, remaining);
      if (count > 0) {
        total += (size_t) count;
        ptr += count;
      } else if (count == 0) {
        SPAWN_ERR("Unexpected read of 0 bytes %s (read() errno=%d %s)", ch->name, errno, strerror(errno));
        return SPAWN_FAILURE;
      } else {
        SPAWN_ERR("Error reading socket %s (read() errno=%d %s)", ch->name, errno, strerror(errno));
        return SPAWN_FAILURE;
      }
    }
  }
  return SPAWN_SUCCESS;
}

static int spawn_net_write_tcp(const spawn_channel_t* ch, const void* buf, size_t size)
{
  /* close the socket */
  int fd = (int) ch->data;
  if (fd > 0) {
    size_t total = 0;
    char* ptr = (char*) buf;
    while (total < size) {
      size_t remaining = size - total;
      ssize_t count = write(fd, ptr, remaining);
      if (count > 0) {
        total += (size_t) count;
        ptr += count;
      } else if (count == 0) {
        SPAWN_ERR("Unexpected write of 0 bytes %s (write() errno=%d %s)", ch->name, errno, strerror(errno));
        return SPAWN_FAILURE;
      } else {
        SPAWN_ERR("Error writing socket %s (write() errno=%d %s)", ch->name, errno, strerror(errno));
        return SPAWN_FAILURE;
      }
    }
  }
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
