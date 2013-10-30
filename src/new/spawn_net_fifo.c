#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

/* to get PIPE_BUF */
#include <limits.h>

#include "spawn_internal.h"

/* This transport can be used for procs on the same node.  As a single
 * receiving endpoint, each process creates a FIFO named pipe that it
 * opens as O_RDONLY.  It encodes the file name of the pipe as its
 * endpoint name.  In spawn_net_connect, any process may open the
 * pipe for writing.  The read end is opened as O_NONBLOCK so that
 * a process does not block in open waiting for a writer when
 * opening the pipe.  This also means that a read call may return
 * 0 or errno=EAGAIN if there is no data in the pipe.
 *
 * Messages are constructed as variable length packets, up to a max
 * size of PIPE_BUF.  Writes to the pipe are atomic so long as
 * the write size is no bigger than PIPE_BUF.  Beyond this size, bytes
 * written by one process may be interleaved with bytes written by
 * another process.  Messages larger than PIPE_BUF size are broken
 * into multiple packets all of which are are most PIPE_BUF bytes long.
 * POSIX ensures that PIPE_BUF is at least 512 bytes, but Linux uses
 * something like 4096.
 *
 * The packet consists of 3 uint64_t fields followed by 0 or more
 * (up to PIPE_BUF - 3*8) bytes of data payload.  The header fields
 * are written in network order and consist of:
 *   packet type,
 *   source id,
 *   and payload size in bytes.
 *
 * There are four packet types:
 *   PKT_CONNECT    - sent to request a new connection
 *   PKT_ACCEPT     - sent to accept a new connection
 *   PKT_MESSAGE    - carries message data
 *   PKT_DISCONNECT - sent to end a connection
 *
 * Since multiple procs may write to the endpoint, the receiver
 * uses the source id to distinguish among senders.  The sender
 * specifies a uint64_t id assigned to it by the receiver
 * when the connection was established.  Both the sender and receiver
 * cache this id as part of the spawn_net_channel state.  Thus, each
 * channel caches a read id (used to identify incoming messages as
 * belonging to the channel) and a write id (written as source field
 * on outgoing messages of that channel).
 *
 * When requesting a new connection, the requestor sends the id the
 * acceptor should use when sending messages back to the requestor
 * on the accepted connection.  It also sends the endpoint name of
 * the requestor's pipe.  Both items are packed as payload data in a
 * PKT_CONNECT message.  When accepting a connection, the acceptor
 * sends a PKT_ACCEPT message to the requestor whose data payload
 * specifies the id that the requestor should when sending messages
 * to the acceptor on the accepted channel.  The PKT_DISCONNECT
 * message has no payload.  Thus the different messages look
 * like the following:
 *
 * Connect: PKT_CONNECT, src ignored, size, data=(id, pipe name)
 * Accept: PKT_ACCEPT, src id from connect, size, data=(id)
 * Message: PKT_MESSAGE, src id from connect or accept, size, data
 * Disconnect: PKT_DISCONNECT, src id from connect or accept, size=0
 *
 * Linux pipes have a limitted capacity.  Since multiple procs may
 * write to the pipe, a process eagerly reads packets from its pipe
 * and appends them to a queue in order to avoid head-of-queue deadlock.
 * The packet queue is searched when matching packets on a
 * spawn_net_read call.  A packet data structure provides access to
 * the header fields along with a pointer to the data payload (if any).
 * Functions exist to extract packets from the queue.  The pipe is
 * drained frequently in order to avoid blocking writers.
 *
 * If the read end of a pipe is closed while some procs still have
 * the pipe open for writing, the procs that can write to the pipe
 * are passed SIGPIPE.  In order to avoid this signal, the read end
 * of a pipe is not closed until all connections have been closed.
 * Two global reference counts are kept with each pipe.  One counts
 * the number of times a process has opened its endpoint with
 * spawn_net_open and decrements the count each time spawn_net_close
 * is called (g_open_count).  The second (g_writer_count) counts the
 * number of active connections, and it decrements each time a remote
 * task calls spawn_net_disconnect.  A pipe is not unlinked on
 * spawn_net_close until both counts are 0.  The final call to
 * spawn_net_close blocks until a process receives disconnect messages
 * from all open procs. */

/* using current API and FIFOs we can only have one receiving
 * FIFO, so store it as a global */
static char* g_name = NULL;    /* name of our read pipe */
static char* g_path = NULL;    /* path of our read pipe */
static int g_fd     = -1;      /* file descriptor of our read pipe */
static int g_open_count   = 0; /* number of times proc has opened read pipe */
static int g_writer_count = 0; /* number of procs holding active connections to read pipe */
static uint64_t g_next_id = 1; /* start assigning ids at 1, increment with each new connection */

/* packet header is 3 unit64_t fields: type, src, size */
const static size_t HDR_SIZE = 3 * 8;

/* TODO: could make this an enum */
/* packet types */
const static uint64_t PKT_NULL       = 0;
const static uint64_t PKT_CONNECT    = 1;
const static uint64_t PKT_ACCEPT     = 2;
const static uint64_t PKT_DISCONNECT = 3;
const static uint64_t PKT_MESSAGE    = 4;

typedef struct spawn_packet_t {
  uint64_t type; /* packet type */
  uint64_t src;  /* sender id */
  uint64_t size; /* payload size in bytes */
  char* data;    /* pointer to packet payload */
  struct spawn_packet_t* next; /* pointer used for queue linked list */
} spawn_packet;

/* packet queue */
static spawn_packet* queue_head = NULL;
static spawn_packet* queue_tail = NULL;

/* structure allocated and stored as extra state in spawn_net_endpoint */
typedef struct spawn_epdata_t {
  int fd; /* file descriptor of our read pipe */
} spawn_epdata;

/* structure allocated and stored as extra state in spawn_net_channel */
typedef struct spawn_chdata_t {
  int readfd;  /* file descriptor of pipe we read for this channel */
  int readid;  /* id to identify packets as belonging to this channel */
  int writefd; /* file descriptor of pipe we write to for this channel */
  int writeid; /* id to assign to packets we write */
  char* readname;  /* name of read pipe */
  char* writename; /* name of write pipe */
} spawn_chdata;

/* reads size bytes from file descriptor and stores in buf,
 * retries read on EINTR, returns SPAWN_SUCCESS if valid,
 * returns SPAWN_FAILURE and sets errno otherwise */
static int reliable_read(const char* name, int fd, void* buf, size_t size)
{
  /* read from fifo */
  size_t total = 0;
  char* ptr = (char*) buf;
  while (total < size) {
    size_t remaining = size - total;
    ssize_t count = read(fd, ptr, remaining);
    if (count > 0) {
      total += (size_t) count;
      ptr += count;
    } else if (count == 0) {
      /* TODO: do we need to worry about this meaning a pipe was closed? */
      /* treat a read of 0 bytes as meaning no data is available */
      errno = EAGAIN;
      return SPAWN_FAILURE;
    } else {
      /* got an error, look at errno to determine what to do next */
      if (errno != EAGAIN) {
        /* if EAGAIN, nothing is ready yet */
        return SPAWN_FAILURE;
      } else if (errno == EINTR) {
        /* if EINTR, retry the read */
        continue;
      } else {
        /* otherwise, we got some error we can't recover from */
        return SPAWN_FAILURE;
      }
    }
  }
  return SPAWN_SUCCESS;
}

/* blocking write size bytes in buf to file descriptor,
 * retries on EINTR or EAGAIN */
static int reliable_write(const char* name, int fd, const void* buf, size_t size)
{
  /* write to socket */
  size_t total = 0;
  char* ptr = (char*) buf;
  while (total < size) {
    size_t remaining = size - total;
    ssize_t count = write(fd, ptr, remaining);
    if (count > 0) {
      total += (size_t) count;
      ptr += count;
    } else if (count == 0) {
      SPAWN_ERR("Unexpected write of 0 bytes to fifo %s (write() errno=%d %s)", name, errno, strerror(errno));
      return SPAWN_FAILURE;
    } else {
      /* if EINTR or EAGAIN, retry */
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* otherwise, we got some error we can't recover from */
      SPAWN_ERR("Error writing fifo %s (write() errno=%d %s)", name, errno, strerror(errno));
      return SPAWN_FAILURE;
    }
  }
  return SPAWN_SUCCESS;
}

/* allocate and initialize fields of a new packet data structure */
static spawn_packet* packet_new()
{
  /* allocate a new packet */
  spawn_packet* p = (spawn_packet*) SPAWN_MALLOC(sizeof(spawn_packet));

  /* initialize fields */
  p->type = PKT_NULL;
  p->src  = 0;
  p->size = 0;
  p->data = NULL;
  p->next = NULL;

  return p;
}

/* free a packet data structure and its associated memory */
static int packet_free(spawn_packet** ppacket)
{
  /* don't need to do anything if we got a NULL value */
  if (ppacket == NULL) {
    return SPAWN_SUCCESS;
  }

  /* get pointer to packet */
  spawn_packet* p = *ppacket;
  if (p != NULL) {
    /* free memory associated with packet */
    spawn_free(&p->data);
  }

  /* free packet data structure */
  spawn_free(ppacket);

  return SPAWN_SUCCESS;
}

/* attempt to read one packet from pipe, returns newly allocated packet
 * if one exists, NULL otherwise */
static spawn_packet* packet_poll(const char* name, int fd)
{
  /* read the header */
  char* headerbuf = (char*) SPAWN_MALLOC(HDR_SIZE);
  if (reliable_read(name, fd, headerbuf, HDR_SIZE) != SPAWN_SUCCESS) {
    if (errno != EAGAIN) {
      /* TODO: abort? */
      SPAWN_ERR("Error reading fifo %s (read() errno=%d %s)", name, errno, strerror(errno));
    }
    spawn_free(&headerbuf);
    return NULL;
  }

  /* unpack size, packet type, and source */
  uint64_t type, src, size;
  char* ptr = headerbuf;
  ptr += spawn_unpack_uint64(ptr, &type);
  ptr += spawn_unpack_uint64(ptr, &src);
  ptr += spawn_unpack_uint64(ptr, &size);

  /* free the header buffer */
  spawn_free(&headerbuf);

  /* allocate space to hold packet data */
  size_t bytes = (size_t) size;
  char* buf = (char*) SPAWN_MALLOC(bytes);

  /* read packet payload */
  if (reliable_read(name, fd, buf, bytes) != SPAWN_SUCCESS) {
    SPAWN_ERR("Failed to read packet payload from %s", name);
    return NULL;
  }

  /* get a new packet structure */
  spawn_packet* p = packet_new();

  /* set packet fields */
  p->type = type;
  p->src  = src;
  p->size = size;
  p->data = buf;

  return p;
}

/* drain all packets from pipe and append to packet queue */
static void queue_progress()
{
  /* pull all incoming packets and append to queue */
  spawn_packet* p = packet_poll(g_name, g_fd);
  while (p != NULL) {
    if (p->type == PKT_DISCONNECT) {
      /* process disconnect messages immediately */

      /* decrement the number of writers and free packet */
      g_writer_count--;
      packet_free(&p);
    } else {
      /* otherwise, append packet to queue */

      /* set packet as new head if we don't have one */
      if (queue_head == NULL) {
        queue_head = p;
      }

      /* set packet as new tail */
      if (queue_tail != NULL) {
        queue_tail->next = p;
      }
      queue_tail = p;
    }

    /* look for next packet */
    p = packet_poll(g_name, g_fd);
  }

  return;
}

/* given pointer to current packet and one before it in queue,
 * extract current packet from queue */
static void queue_extract(spawn_packet* prev, spawn_packet* curr)
{
  if (queue_head == curr) {
    /* packet is at front of queue, update the head to point to
     * next packet */
    queue_head = curr->next;

    /* also update the tail if it happened to be the only packet */
    if (queue_head == NULL) {
      queue_tail = NULL;
    }
  } else {
    /* there is a packet in front of this one,
     * set its next pointer to the packet after the current one */
    prev->next = curr->next;

    /* if the current packet is the tail,
     * point the tail to the previous packet */
    if (queue_tail == curr) {
      queue_tail = prev;
    }
  }

  /* set current's next pointer to NULL */
  curr->next = NULL;

  return;
}

/* determine whether specified packet matches type and source id */
static int packet_match(spawn_packet* p, uint64_t type, uint64_t src)
{
  /* assume packet does not match */
  int match = 0;

  if (p->type == type) {
    /* this packet type matches the type we're looking for */
    if (type == PKT_CONNECT) {
      /* for connect packets, we ignore the source field (it's not valid) */
      match = 1;
    } else if (p->src == src) {
      /* the source matches the target source */
      match = 1;
    }
  }

  return match;
}

/* block until a matching packet is extracted from queue */
static spawn_packet* packet_read(const char* name, int fd, uint64_t type, uint64_t src)
{
  /* we set p to the matched packet when we find it */
  spawn_packet* p = NULL;

  /* first, pull off any packets that may be on the wire, we do this eagerly
   * to avoid blocking writers  */
  queue_progress();

  /* scan current queue for matching packet */
  spawn_packet* prev = NULL;
  spawn_packet* curr = queue_head;
  while (curr != NULL) {
    /* if packet matches, set pointer, extract it and break search */
    if (packet_match(curr, type, src)) {
      queue_extract(prev, curr);
      p = curr;
      break;
    }

    /* go to next packet */
    prev = curr;
    curr = curr->next;
  }

  /* if we still haven't found the packet, wait until it comes in */
  while (p == NULL) {
    /* remember old tail value */
    spawn_packet* tail = queue_tail;

    /* drain any new packets */
    queue_progress();

    /* if we got new items, current tail will be different than old tail */
    if (queue_tail != tail) {
      /* some new packets came in, so look through them */
      if (tail == NULL) {
        /* if queue was empty, start search from new head */
        prev = NULL;
        curr = queue_head;
      } else {
        /* if queue was not empty, new items are attached after old tail */
        prev = tail;
        curr = tail->next;
      }

      while (curr != NULL) {
        /* if packet matches, set pointer, extract it and break search */
        if (packet_match(curr, type, src)) {
          queue_extract(prev, curr);
          p = curr;
          break;
        }

        /* go to next packet */
        prev = curr;
        curr = curr->next;
      }
    }
  }

  return p;
}

/* create a named pipe FIFO and open it for reading */
spawn_net_endpoint* spawn_net_open_fifo()
{
  /* create fifo file if needed */
  if (g_name == NULL) {
    /* define a name for our fifo */
    pid_t pid = getpid();
    g_path = SPAWN_STRDUPF("/tmp/fifo.%lu", (unsigned long)pid);
    g_name = SPAWN_STRDUPF("FIFO:%s", g_path); 

    /* create fifo */
    int rc = mknod(g_path, S_IFIFO | 0600, (dev_t)0);
    if (rc < 0) {
      SPAWN_ERR("Failed to create fifo at '%s'", g_path);
      spawn_free(&g_path);
      spawn_free(&g_name);
      return SPAWN_NET_ENDPOINT_NULL;
    }

    /* open fifo for reading */
    g_fd = open(g_path, O_RDONLY | O_NONBLOCK);
    if (g_fd < 0) {
      SPAWN_ERR("Failed to open fifo at '%s'", g_path);
      unlink(g_path);
      spawn_free(&g_path);
      spawn_free(&g_name);
      return SPAWN_NET_ENDPOINT_NULL;
    }
  }

  /* increment our reference count */
  g_open_count++;

  /* allocate fifo-specific endpoint data */
  spawn_epdata* epdata = (spawn_epdata*) SPAWN_MALLOC(sizeof(spawn_epdata));

  /* record file descriptor of read end of FIFO */
  epdata->fd = g_fd;

  /* allocate endpoint structure */
  spawn_net_endpoint* ep = (spawn_net_endpoint*) SPAWN_MALLOC(sizeof(spawn_net_endpoint));

  /* store values in endpoint struct */
  ep->type = SPAWN_NET_TYPE_FIFO;
  ep->name = g_name;
  ep->data = (void*)epdata;

  return ep;
}

/* closed a named pipe opened for reading, unlink it if there are no
 * more references to it */
int spawn_net_close_fifo(spawn_net_endpoint** pep)
{
  /* check that we got a valid pointer */
  if (pep == NULL) {
    SPAWN_ERR("Endpoint is NULL");
    return SPAWN_FAILURE;
  }

  /* check that count is above 0 before we decrement */
  if (g_open_count <= 0) {
    SPAWN_ERR("Endpoint is not open");
    return SPAWN_FAILURE;
  }

  /* decrement reference */
  g_open_count--;

  /* delete FIFO if we hit 0 */
  if (g_open_count == 0) {
    /* wait until all writers have sent a disconnect message */
    while (g_writer_count > 0) {
      queue_progress();
    }

    /* close the socket */
    if (g_fd > 0) {
      close(g_fd);
    }

    /* delete the file */
    unlink(g_path);

    /* free the name and path strings */
    spawn_free(&g_name);
    spawn_free(&g_path);
  }

  /* get pointer to endpoint */
  spawn_net_endpoint* ep = *pep;
  if (ep != NULL) {
    /* free fifo-specific data */
    spawn_free(&ep->data);
  }

  /* free the endpoint structure */
  spawn_free(&ep);

  /* set caller's pointer to NULL */
  *pep = SPAWN_NET_ENDPOINT_NULL;

  return SPAWN_SUCCESS;
}

/* open a named pipe for writing */
static int open_for_write(const char* name)
{
  /* verify that the address string starts with correct prefix */
  if (strncmp(name, "FIFO:", 5) != 0) {
    SPAWN_ERR("Endpoint name is not FIFO format %s", name);
    return -1;
  }

  /* get path, advance past FIFO: */
  const char* path = name;
  path += 5;

  /* open fifo for writing */
  int fd = open(path, O_WRONLY);
  if (fd < 0) {
    SPAWN_ERR("Failed to open FIFO for writing %s", path);
    return -1;
  }

  return fd;
}

/* send connection request to named endpoint, wait for
 * accept message as reply and return new connection */
spawn_net_channel* spawn_net_connect_fifo(const char* name)
{
  /* open FIFO for writing */
  int writefd = open_for_write(name);
  if (writefd < 0) {
    SPAWN_ERR("Failed to connect to %s", name);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* define channel name */
  char* ch_name = SPAWN_STRDUPF("%s <--> %s", g_name, name);

  /* allocate buffer to send connect message to remote side,
   * payload consists of uint64_t of write id and our FIFO name */
  uint64_t size = 8 + strlen(g_name) + 1;
  size_t bufsize = HDR_SIZE + (size_t) size;
  char* buf = (char*) SPAWN_MALLOC(bufsize);

  /* pack our connect message into buffer */
  char* ptr = buf;
  ptr += spawn_pack_uint64(ptr, PKT_CONNECT);
  ptr += spawn_pack_uint64(ptr, (uint64_t)-1);
  ptr += spawn_pack_uint64(ptr, size);

  /* get free id */
  uint64_t readid = g_next_id;
  g_next_id++;

  /* pack writeid and FIFO name */
  ptr += spawn_pack_uint64(ptr, readid);
  strcpy(ptr, g_name);

  /* send our connect message across fifo */
  if (reliable_write(name, writefd, buf, bufsize) != SPAWN_SUCCESS) {
    SPAWN_ERR("Failed to write connect message to %s", ch_name);
    spawn_free(&buf);
    spawn_free(&ch_name);
    close(writefd);
    return SPAWN_NET_CHANNEL_NULL;
  }
  spawn_free(&buf);

  /* wait for accept message to come back */
  spawn_packet* p = packet_read(g_name, g_fd, PKT_ACCEPT, readid);
  if (p == NULL) {
    SPAWN_ERR("Failed to read accept message from %s", ch_name);
    spawn_free(&ch_name);
    close(writefd);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* read our id from payload of accept message and free the packet */
  uint64_t writeid;
  spawn_unpack_uint64(p->data, &writeid);
  packet_free(&p);

  /* increase our writer count */
  g_writer_count++;

  /* allocate fifo-specific channel data */
  spawn_chdata* chdata = (spawn_chdata*) SPAWN_MALLOC(sizeof(spawn_chdata));

  /* record read and write file descriptors */
  chdata->readfd    = g_fd;
  chdata->readid    = readid;
  chdata->readname  = SPAWN_STRDUP(g_name);
  chdata->writefd   = writefd;
  chdata->writeid   = writeid;
  chdata->writename = SPAWN_STRDUP(name);

  /* allocate a channel structure */
  spawn_net_channel* ch = (spawn_net_channel*) SPAWN_MALLOC(sizeof(spawn_net_channel));

  ch->type = SPAWN_NET_TYPE_FIFO;
  ch->name = ch_name;
  ch->data = (void*)chdata;

  return ch;
}

/* accept a connection request and reply with accept message,
 * return newly created connection */
spawn_net_channel* spawn_net_accept_fifo(const spawn_net_endpoint* ep)
{
  /* get FIFO endpoint data */
  spawn_epdata* epdata = (spawn_epdata*) ep->data;
  if (epdata == NULL) {
      SPAWN_ERR("Endpoint missing FIFO data %s", ep->name);
      return SPAWN_NET_CHANNEL_NULL;
  }

  /* get listening descriptor */
  int listenfd = epdata->fd;

  /* read messages until we get an incoming connection request */
  spawn_packet* p = packet_read(ep->name, listenfd, PKT_CONNECT, (uint64_t)-1);
  if (p == NULL) {
    SPAWN_ERR("Failed to read CONNECT message on FIFO %s", ep->name);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* the payload consists of our writeid and the remote FIFO name */
  char* ptr = p->data;
  uint64_t writeid;
  ptr += spawn_unpack_uint64(ptr, &writeid);
  const char* name = ptr;

  /* connect to remote end */
  int writefd = open_for_write(name);
  if (writefd < 0) {
    SPAWN_ERR("Failed to open FIFO %s", name);
    packet_free(&p);
    return SPAWN_NET_CHANNEL_NULL;
  }

  /* create channel name */
  char* ch_name = SPAWN_STRDUPF("%s <--> %s", g_name, name);

  /* free the packet */
  packet_free(&p);

  /* get id for remote side and increment our id for the next connection */
  uint64_t readid = g_next_id;
  g_next_id++;

  /* allocate buffer for accept message */
  uint64_t payload_size = 8;
  size_t accept_bufsize = HDR_SIZE + (size_t) payload_size;
  char* accept_buf = (char*) SPAWN_MALLOC(accept_bufsize);

  /* prepare accept packet header */
  ptr = accept_buf;
  ptr += spawn_pack_uint64(ptr, PKT_ACCEPT); 
  ptr += spawn_pack_uint64(ptr, writeid); 
  ptr += spawn_pack_uint64(ptr, payload_size); 

  /* pack in id for remote side */
  ptr += spawn_pack_uint64(ptr, readid); 

  /* send accept packet */
  reliable_write(name, writefd, accept_buf, accept_bufsize);

  /* free buffer for accept message */
  spawn_free(&accept_buf);

  /* increase our writer count */
  g_writer_count++;

  /* allocate fifo-specific channel data */
  spawn_chdata* chdata = (spawn_chdata*) SPAWN_MALLOC(sizeof(spawn_chdata));

  /* record read and write file descriptors */
  chdata->readfd    = g_fd;
  chdata->readid    = readid;
  chdata->readname  = SPAWN_STRDUP(g_name);
  chdata->writefd   = writefd;
  chdata->writeid   = writeid;
  chdata->writename = SPAWN_STRDUP(name);

  /* allocate channel structure */
  spawn_net_channel* ch = (spawn_net_channel*) SPAWN_MALLOC(sizeof(spawn_net_channel));

  /* set channel parameters */
  ch->type = SPAWN_NET_TYPE_FIFO;
  ch->name = ch_name;
  ch->data = (void*)chdata;

  return ch;
}

/* send disconnect message on channel and close write end of pipe */
int spawn_net_disconnect_fifo(spawn_net_channel** pch)
{
  /* check that we got a valid pointer */
  if (pch == NULL) {
      return SPAWN_FAILURE;
  }

  /* get pointer to channel */
  spawn_net_channel* ch = *pch;
  if (ch != NULL) {
    /* get fifo-specific channel data */
    spawn_chdata* chdata = (spawn_chdata*) ch->data;
    if (chdata != NULL) {
      /* get write file descriptor */
      int fd = chdata->writefd;
      if (fd > 0) {
        /* allocate buffer for disconnect message */
        char* buf = (char*) SPAWN_MALLOC(HDR_SIZE);

        /* pack data into message */
        char* ptr = buf;
        ptr += spawn_pack_uint64(ptr, PKT_DISCONNECT);
        ptr += spawn_pack_uint64(ptr, (uint64_t)chdata->writeid);
        ptr += spawn_pack_uint64(ptr, (uint64_t)0);

        /* write data */
        reliable_write(chdata->writename, fd, buf, HDR_SIZE);

        /* free the buffer */
        spawn_free(&buf);

        /* close the socket */
        close(fd);
      }

      /* free read and write names */
      spawn_free(&chdata->readname);
      spawn_free(&chdata->writename);

      /* free the channel data */
      spawn_free(&ch->data);
    }

    /* free the name string */
    spawn_free(&ch->name);
  }

  /* free channel structure */
  spawn_free(&ch);

  /* set caller's pointer to NULL */
  *pch = SPAWN_NET_CHANNEL_NULL;

  return SPAWN_SUCCESS;
}

/* read sizes bytes of data from channel into buffer */
int spawn_net_read_fifo(const spawn_net_channel* ch, void* buf, size_t size)
{
  /* get FIFO channel data */
  spawn_chdata* chdata = (spawn_chdata*) ch->data;
  if (chdata == NULL) {
    return SPAWN_FAILURE;
  }

  /* get read name */
  const char* name = chdata->readname;

  /* get read file descriptor */
  int fd = chdata->readfd;

  /* read data a packet at a time */
  int rc = SPAWN_SUCCESS;
  size_t nread = 0;
  while (nread < size) {
    /* read a packet */
    spawn_packet* p = packet_read(name, fd, PKT_MESSAGE, chdata->readid);
    if (p == NULL) {
      SPAWN_ERR("Failed to read message packet from %s", name);
      return SPAWN_FAILURE;
    }

    /* copy data to user's buffer */
    size_t payload_size = (size_t) p->size;
    if (payload_size > 0) {
      char* ptr = (char*)buf + nread;
      memcpy(ptr, p->data, payload_size);
    }

    /* free the packet */
    packet_free(&p);

    /* go on to next part of message */
    nread += payload_size;
  }

  return rc;
}

/* write size bytes from buffer into channel */
int spawn_net_write_fifo(const spawn_net_channel* ch, const void* buf, size_t size)
{
  /* get FIFO channel data */
  spawn_chdata* chdata = (spawn_chdata*) ch->data;
  if (chdata == NULL) {
    return SPAWN_FAILURE;
  }

  /* get write name */
  const char* name = chdata->writename;

  /* get write file descriptor */
  int fd = chdata->writefd;

  /* create packet buffer,
   * we use max size of PIPE_BUF so reads/writes are atomic */
  size_t packet_bufsize = (size_t) PIPE_BUF;
  char* packet_buf = (char*) SPAWN_MALLOC(packet_bufsize);

  /* compute payload size */
  size_t payload_size = packet_bufsize - HDR_SIZE;

  /* break message up into packets and send each one */
  int rc = SPAWN_SUCCESS;
  size_t nwritten = 0;
  while (nwritten < size) {
    /* determine amount to write in this step */
    size_t bytes = (size - nwritten);
    if (bytes > payload_size) {
      bytes = payload_size;
    }

    /* compute size of this packet */
    size_t packet_size = HDR_SIZE + bytes;

    /* fill in header */
    char* ptr = packet_buf;
    ptr += spawn_pack_uint64(ptr, PKT_MESSAGE); 
    ptr += spawn_pack_uint64(ptr, chdata->writeid); 
    ptr += spawn_pack_uint64(ptr, bytes); 

    /* fill in data */
    char* data = (char*)buf + nwritten;
    memcpy(ptr, data, bytes);

    /* write packet */
    int tmp_rc = reliable_write(name, fd, packet_buf, packet_size);

    /* go to next part of message */
    nwritten += bytes;
  }

  /* free packet buffer */
  spawn_free(&packet_buf);

  return rc;
}
