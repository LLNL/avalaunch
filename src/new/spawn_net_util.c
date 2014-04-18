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
    size_t size = 0;
    if (str != NULL) {
        size = strlen(str) + 1;
    }

    /* allocate memory to hold string (plus its size) */
    size_t bufsize = 8 + size;
    void* buf = SPAWN_MALLOC(bufsize);
    char* ptr = (char*) buf;

    /* pack length of the string */
    uint64_t size64 = (uint64_t) size;
    ptr += spawn_pack_uint64(ptr, size64);

    /* send size */
    spawn_net_write(ch, buf, 8);

    /* copy the string */
    if (size > 0) {
        memcpy(ptr, str, size);
    }

    /* send message */
    spawn_net_write(ch, (char*)buf + 8, size);

/* to issue a single write instead of two,
 * uncomment this line and comment the two writes above */
//    spawn_net_write(ch, buf, bufsize);

    /* free buffer */
    spawn_free(&buf);

    return;
}

char* spawn_net_read_str(const spawn_net_channel* ch)
{
    /* recv the length of the string */
    uint64_t len_net;
    if (spawn_net_read(ch, &len_net, sizeof(uint64_t)) != SPAWN_SUCCESS) {
        return NULL;
    }

    /* convert from network to host order */
    uint64_t len;
    spawn_unpack_uint64(&len_net, &len);

    /* allocate space */
    size_t bytes = (size_t) len;
    char* str = SPAWN_MALLOC(bytes);

    /* recv the string */
    if (bytes > 0) {
        if (spawn_net_read(ch, str, bytes) != SPAWN_SUCCESS) {
            spawn_free(&str);
            return NULL;
        }
    }

    return str;
}

void spawn_net_write_strmap(const spawn_net_channel* ch, const strmap* map)
{
    /* determine number of bytes needed to pack strmap */
    size_t size = strmap_pack_size(map);

    /* allocate memory to pack strmap (plus its size) */
    size_t bufsize = 8 + size;
    void* buf = SPAWN_MALLOC(bufsize);
    char* ptr = (char*) buf;

    /* pack size as header */
    uint64_t size64 = (uint64_t) size;
    ptr += spawn_pack_uint64(ptr, size64);
  
    /* send size */
    spawn_net_write(ch, buf, 8);

    /* pack strmap into buffer */
    ptr += strmap_pack(ptr, map);

    /* send map */
    spawn_net_write(ch, (char*)buf + 8, size);

/* to issue a single write instead of two,
 * uncomment this line and comment the two writes above */
//    spawn_net_write(ch, buf, bufsize);

    /* free buffer */
    spawn_free(&buf);

    return;
}

void spawn_net_read_strmap(const spawn_net_channel* ch, strmap* map)
{
    /* read size */
    uint64_t len_net;
    if (spawn_net_read(ch, &len_net, sizeof(uint64_t)) != SPAWN_SUCCESS) {
        return;
    }

    /* convert from network to host order */
    uint64_t len;
    spawn_unpack_uint64(&len_net, &len);

    if (len > 0) {
        /* allocate buffer */
        size_t bytes = (size_t) len;
        void* buf = SPAWN_MALLOC(bytes);

        /* read data */
        if (spawn_net_read(ch, buf, bytes) == SPAWN_SUCCESS) {
            /* unpack map */
            strmap_unpack(buf, map);
        }

        /* free buffer */
        spawn_free(&buf);
    }

    return;
}
