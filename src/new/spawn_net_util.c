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

void spawn_net_write_str(const spawn_net_channel* ch, const char* str)
{
    /* get length of string */
    size_t bytes = 0;
    if (str != NULL) {
        bytes = strlen(str) + 1;
    }

    /* TODO: pack size in network order */
    /* send the length of the string */
    uint64_t len = (uint64_t) bytes;
    spawn_net_write(ch, &len, sizeof(uint64_t));

    /* send the string */
    if (bytes > 0) {
        spawn_net_write(ch, str, bytes);
    }
}

char* spawn_net_read_str(const spawn_net_channel* ch)
{
    /* TODO: pack size in network order */
    /* recv the length of the string */
    uint64_t len;
    spawn_net_read(ch, &len, sizeof(uint64_t));

    /* allocate space */
    size_t bytes = (size_t) len;
    char* str = SPAWN_MALLOC(bytes);

    /* recv the string */
    if (bytes > 0) {
        spawn_net_read(ch, str, bytes);
    }

    return str;
}

void spawn_net_write_strmap(const spawn_net_channel* ch, const strmap* map)
{
    /* allocate memory and pack strmap */
    size_t bytes = strmap_pack_size(map);
    void* buf = SPAWN_MALLOC(bytes);
    strmap_pack(buf, map);

    /* TODO: convert to network order */
    /* send size */
    uint64_t len = (uint64_t) bytes;
    spawn_net_write(ch, &len, sizeof(uint64_t));
  
    /* send map */
    if (bytes > 0) {
        spawn_net_write(ch, buf, bytes);
    }

    /* free buffer */
    spawn_free(&buf);
}

void spawn_net_read_strmap(const spawn_net_channel* ch, strmap* map)
{
    /* TODO: convert from network order */
    /* read size */
    uint64_t len;
    spawn_net_read(ch, &len, sizeof(uint64_t));

    if (len > 0) {
        /* allocate buffer */
        size_t bytes = (size_t) len;
        void* buf = SPAWN_MALLOC(bytes);

        /* read data */
        spawn_net_read(ch, buf, bytes);

        /* unpack map */
        strmap_unpack(buf, map);

        /* free buffer */
        spawn_free(&buf);
    }
}
