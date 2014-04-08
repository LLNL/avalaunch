#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "spawn_internal.h"

static int spawn_net_tcp_backlog = 64;

typedef struct spawn_epdata_t {
    int fd; /* file descriptor of listening socket */
} spawn_epdata;

typedef struct spawn_chdata_t {
    int fd; /* file descriptor of connected TCP socket */
} spawn_chdata;

static int reliable_read(const char* name, int fd, void* buf, size_t size)
{
  /* read from socket */
  size_t total = 0;
  char* ptr = (char*) buf;
  while (total < size) {
    /* compute number of bytes remaining and read */
    size_t remaining = size - total;
    ssize_t count = read(fd, ptr, remaining);
    if (count > 0) {
      /* we read some bytes, update our count and pointer position */
      total += (size_t) count;
      ptr += count;
    } else if (count == 0) {
      /* we can get this on EOF, e.g., remote socket closed normally,
       * however, since caller tried to read something, we return
       * an error */
      //SPAWN_ERR("Unexpected read of 0 bytes %s (read() errno=%d %s)", name, errno, strerror(errno));
      return SPAWN_FAILURE;
    } else {
      /* TODO: if EINTR, retry */

      SPAWN_ERR("Error reading socket %s (read() errno=%d %s)", name, errno, strerror(errno));
      return SPAWN_FAILURE;
    }
  }
  return SPAWN_SUCCESS;
}

static int reliable_write(const char* name, int fd, const void* buf, size_t size)
{
  /* write to socket */
  size_t total = 0;
  char* ptr = (char*) buf;
  while (total < size) {
    /* compute number of bytes remaining and write */
    size_t remaining = size - total;
    ssize_t count = write(fd, ptr, remaining);
    if (count > 0) {
      /* we wrote some bytes, update our count and pointer position */
      total += (size_t) count;
      ptr += count;
    } else if (count == 0) {
      SPAWN_ERR("Unexpected write of 0 bytes %s (write() errno=%d %s)", name, errno, strerror(errno));
      return SPAWN_FAILURE;
    } else {
      /* TODO: if EINTR, retry */

      SPAWN_ERR("Error writing socket %s (write() errno=%d %s)", name, errno, strerror(errno));
      return SPAWN_FAILURE;
    }
  }
  return SPAWN_SUCCESS;
}

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

/* modify socket so that data is sent as soon as it's written */
static int spawn_net_set_tcp_nodelay(int fd)
{
  /* read no delay setting from environment if set */
  int set_nodelay = 0;
  char *env;
  if ((env = getenv("SPAWN_TCP_NODELAY")) != NULL ) {
    set_nodelay = atoi(env);
  }

  /* disable TCP Nagle buffering if requested */
  if (set_nodelay) {
    int flag=1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag)) < 0 ) {
      SPAWN_ERR("Failed to set TCP_NODELAY option (setsockopt() errno=%d %s)", errno, strerror(errno));
      return SPAWN_FAILURE;
    }
  }

  return SPAWN_SUCCESS;
}

spawn_net_endpoint* spawn_net_open_tcp()
{
  /* create a TCP socket, we'll take new connections on this socket */
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    return SPAWN_NET_ENDPOINT_NULL;
  }

  /* set socket up for immediate send */
  if (spawn_net_set_tcp_nodelay(fd)) {
    close(fd);
    return SPAWN_NET_ENDPOINT_NULL;
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
    return SPAWN_NET_ENDPOINT_NULL;
  }

  /* listen for connections */
  if (listen(fd, spawn_net_tcp_backlog) < 0) {
    SPAWN_ERR("Failed to set socket to listen (listen() errno=%d %s)", errno, strerror(errno));
    close(fd);
    return SPAWN_NET_ENDPOINT_NULL;
  }

  /* get our hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) < 0) {
    SPAWN_ERR("Failed gethostname()");
    close(fd);
    return SPAWN_NET_ENDPOINT_NULL;
  }

  /* get our ip address */
  struct hostent* he = gethostbyname(hostname);
  if (he == NULL) {
    SPAWN_ERR("Failed gethostbyname()");
    close(fd);
    return SPAWN_NET_ENDPOINT_NULL;
  }
  struct in_addr ip = *(struct in_addr *) *(he->h_addr_list);

  /* get our port */
  memset(&sin, 0, sizeof(sin));
  socklen_t len = sizeof(sin);
  if (getsockname(fd, (struct sockaddr *) &sin, &len) < 0) {
    SPAWN_ERR("Failed to get socket name (getsockname() errno=%d %s)", errno, strerror(errno));
    close(fd);
    return SPAWN_NET_ENDPOINT_NULL;
  }
  unsigned short port = (unsigned short) ntohs(sin.sin_port);

  /* create name and packed address strings */
  int host_len = (int) strlen(hostname);
  char* name = SPAWN_STRDUPF("TCP:%d:%s:%s:%u", host_len, hostname, inet_ntoa(ip), (unsigned int) port);

  /* allocate TCP-specific endpoint data structure */
  spawn_epdata* epdata = (spawn_epdata*) SPAWN_MALLOC(sizeof(spawn_epdata));
  epdata->fd = fd;

  /* allocate and endpoint structure */
  spawn_net_endpoint* ep = (spawn_net_endpoint*) SPAWN_MALLOC(sizeof(spawn_net_endpoint));

  /* store values in endpoint struct */
  ep->type = SPAWN_NET_TYPE_TCP;
  ep->name = name;
  ep->data = (void*)epdata;

  return ep;
}

int spawn_net_close_tcp(spawn_net_endpoint** pep)
{
  /* check that we got a valid pointer */
  if (pep == NULL) {
    SPAWN_ERR("Endpoint is NULL");
    return SPAWN_FAILURE;
  }

  /* get pointer to endpoint */
  spawn_net_endpoint* ep = *pep;

  /* get pointer to TCP-specific endpoint data structure */
  spawn_epdata* epdata = (spawn_epdata*) ep->data;

  if (epdata != NULL) {
    /* close the socket */
    int fd = epdata->fd;
    if (fd > 0) {
      close(fd);
    }
  }

  /* free the TCP-specific data */
  spawn_free(&ep->data);

  /* free the name string */
  spawn_free(&ep->name);

  /* free the endpoint structure */
  spawn_free(&ep);

  /* set caller's pointer to NULL */
  *pep = SPAWN_NET_ENDPOINT_NULL;

  return SPAWN_SUCCESS;
}

spawn_net_channel* spawn_net_connect_tcp(const char* name)
{
  /* verify that the address string starts with correct prefix */
  if (strncmp(name, "TCP:", 4) != 0) {
    SPAWN_ERR("Endpoint name is not TCP format %s", name);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* make a copy of name that we can modify */
  char* name_copy = SPAWN_STRDUP(name);

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
    return SPAWN_NET_CHANNEL_NULL;
  }

  if (spawn_net_set_tcp_nodelay(fd)) {
    close(fd);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* connect */
  int rc = connect(fd, (const struct sockaddr*) &sockaddr, sizeof(struct sockaddr_in));
  if (rc < 0) {
    SPAWN_ERR("Failed to connect to %s (connect() errno=%d %s)", name, errno, strerror(errno));
    close(fd);
    free(name_copy);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* create channel name */
  char* local_name = spawn_net_get_local_sockname(fd);
  char* remote_name = spawn_net_get_remote_sockname(fd, host_str);
  char* ch_name = SPAWN_STRDUPF("%s --> %s", local_name, remote_name);
  spawn_free(&remote_name);
  spawn_free(&local_name);

  /* free our copy */
  free(name_copy);
  name_copy = NULL;

  /* get our host name to tell remote end who we are */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) < 0) {
    SPAWN_ERR("Failed gethostname()");
    free(ch_name);
    close(fd);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* tell remote end who we are */
  int64_t len, len_net;
  len = (int64_t) (strlen(hostname) + 1);
  spawn_pack_uint64(&len_net, len);
  if (reliable_write(ch_name, fd, &len_net, 8) != SPAWN_SUCCESS) {
    SPAWN_ERR("Failed to write length of name to %s", ch_name);
    free(ch_name);
    close(fd);
    return SPAWN_NET_CHANNEL_NULL;
  }
  if (reliable_write(ch_name, fd, hostname, (size_t)len) != SPAWN_SUCCESS) {
    SPAWN_ERR("Failed to write name to %s", ch_name);
    free(ch_name);
    close(fd);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* allocate TCP-specific channel data */
  spawn_chdata* chdata = (spawn_chdata*) SPAWN_MALLOC(sizeof(spawn_chdata));
  chdata->fd = fd;

  /* allocate a channel structure */
  spawn_net_channel* ch = (spawn_net_channel*) SPAWN_MALLOC(sizeof(spawn_net_channel));

  ch->type = SPAWN_NET_TYPE_TCP;
  ch->name = ch_name;
  ch->data = (void*)chdata;

  return ch;
}

spawn_net_channel* spawn_net_accept_tcp(const spawn_net_endpoint* ep)
{
  /* get pointer to TCP-specific endpoint data structure */
  spawn_epdata* epdata = (spawn_epdata*) ep->data;

  /* get listening socket */
  int listenfd = epdata->fd;

  /* accept an incoming connection request */
  struct sockaddr incoming_addr;
  socklen_t incoming_len = sizeof(incoming_addr);
  int fd = accept(listenfd, &incoming_addr, &incoming_len);
  if (fd < 0) {
    SPAWN_ERR("Failed to connect to %s (connect() errno=%d %s)", ep->name, errno, strerror(errno));
    return SPAWN_NET_CHANNEL_NULL;
  }

  if (spawn_net_set_tcp_nodelay(fd)) {
    close(fd);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* TODO: authenticate remote side */

  /* create temporary remote name for errors until we read real name */
  char* tmp_remote_name = spawn_net_get_remote_sockname(fd, "remote");

  /* read length of name from remote side */
  uint64_t len, len_net;
  if (reliable_read(tmp_remote_name, fd, &len_net, 8) != SPAWN_SUCCESS) {
    SPAWN_ERR("Failed to read length of name from %s", tmp_remote_name);
    spawn_free(&tmp_remote_name);
    close(fd);
    return SPAWN_NET_CHANNEL_NULL;
  }
  spawn_unpack_uint64(&len_net, &len);

  /* allocate memory and read remote name */
  char* remote = (char*) SPAWN_MALLOC((size_t)len);
  if (reliable_read(tmp_remote_name, fd, remote, (size_t)len) != SPAWN_SUCCESS) {
    SPAWN_ERR("Failed to read name from %s", tmp_remote_name);
    spawn_free(&remote);
    spawn_free(&tmp_remote_name);
    close(fd);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* free temporary name */
  spawn_free(&tmp_remote_name);

  /* get local socket name */
  char* local_name  = spawn_net_get_local_sockname(fd);
  char* remote_name = spawn_net_get_remote_sockname(fd, remote);
  char* ch_name = SPAWN_STRDUPF("%s --> %s", local_name, remote_name);
  spawn_free(&remote_name);
  spawn_free(&local_name);
  spawn_free(&remote);

  /* allocate TCP-specific channel data */
  spawn_chdata* chdata = (spawn_chdata*) SPAWN_MALLOC(sizeof(spawn_chdata));
  chdata->fd = fd;

  /* allocate channel structure */
  spawn_net_channel* ch = (spawn_net_channel*) SPAWN_MALLOC(sizeof(spawn_net_channel));

  /* set channel parameters */
  ch->type = SPAWN_NET_TYPE_TCP;
  ch->name = ch_name;
  ch->data = (void*)chdata;

  return ch;
}

int spawn_net_disconnect_tcp(spawn_net_channel** pch)
{
  /* check that we got a valid pointer */
  if (pch == NULL) {
      return SPAWN_FAILURE;
  }

  /* get pointer to channel */
  spawn_net_channel* ch = *pch;

  /* get pointer to TCP-specific channel data */
  spawn_chdata* chdata = (spawn_chdata*) ch->data;

  if (chdata != NULL) {
    /* close the socket */
    int fd = chdata->fd;
    if (fd > 0) {
      close(fd);
    }
  }

  /* free the TCP-specific channel data */
  spawn_free(&ch->data);

  /* free the name string */
  spawn_free(&ch->name);

  /* free channel structure */
  spawn_free(&ch);

  /* set caller's pointer to NULL */
  *pch = SPAWN_NET_CHANNEL_NULL;

  return SPAWN_SUCCESS;
}

int spawn_net_read_tcp(const spawn_net_channel* ch, void* buf, size_t size)
{
  /* get pointer to TCP-specific channel data */
  spawn_chdata* chdata = (spawn_chdata*) ch->data;

  /* close the socket */
  int fd = chdata->fd;
  if (fd > 0) {
    return reliable_read(ch->name, fd, buf, size);
  }
  return SPAWN_SUCCESS;
}

int spawn_net_write_tcp(const spawn_net_channel* ch, const void* buf, size_t size)
{
  /* get pointer to TCP-specific channel data */
  spawn_chdata* chdata = (spawn_chdata*) ch->data;

  /* write to socket */
  int fd = chdata->fd;
  if (fd > 0) {
    return reliable_write(ch->name, fd, buf, size);
  }
  return SPAWN_SUCCESS;
}

int spawn_net_waitany_tcp(int num, const spawn_net_channel** chs, int* index)
{
  /* bail out if channel array is empty */
  if (chs == NULL) {
      return SPAWN_FAILURE;
  }

  /* create fdset for checking read conditions */
  fd_set readfds;
  FD_ZERO(&readfds);

  /* count number of active file descriptors */
  int count = 0;

  /* we'll set this to the highest socket number which is active */
  int highest_fd;

  /* add each active file descriptor to the set */
  int i;
  for (i = 0; i < num; i++) {
    /* get pointer to channel */
    const spawn_net_channel* ch = chs[i];

    /* skip NULL channels */
    if (ch == SPAWN_NET_CHANNEL_NULL) {
        continue;
    }

    /* get pointer to TCP-specific channel data */
    spawn_chdata* chdata = (spawn_chdata*) ch->data;

    /* get the file descriptor */
    int fd = chdata->fd;

    /* add the descriptor to the read set */
    FD_SET(fd, &readfds);

    /* if count is 0, initialize highest_fd,
     * otherwise check whether this fd is higher in value,
     * we need to track the highest fd for use in select */
    if (count == 0 || fd > highest_fd) {
      highest_fd = fd;
    }

    /* increase our count of active file descriptors */
    count++;
  }

  /* if all channels are NULL, we succeeded,
   * but we can't set the index */
  if (count == 0) {
    *index = -1;
    return SPAWN_SUCCESS;
  }

  /* otherwise, call select to find a file descriptor ready for reading */
  int rc = select((highest_fd + 1), &readfds, NULL, NULL, NULL);
  if (rc == -1) {
    SPAWN_ERR("Failed to select file descriptor errno=%d %s", errno, strerror(errno));
    return SPAWN_FAILURE;
  }

  /* select succeeded, find the matching file descriptor */
  for (i = 0; i < num; i++) {
    /* get pointer to channel */
    const spawn_net_channel* ch = chs[i];

    /* skip NULL channels */
    if (ch == SPAWN_NET_CHANNEL_NULL) {
        continue;
    }

    /* get pointer to TCP-specific channel data */
    spawn_chdata* chdata = (spawn_chdata*) ch->data;

    /* get the file descriptor */
    int fd = chdata->fd;

    /* check whether fd is ready, set index and break if so */
    if (FD_ISSET(fd, &readfds)) {
        *index = i;
        break;
    }
  }

  return SPAWN_SUCCESS;
}
