#ifndef SPAWN_UTIL_H
#define SPAWN_UTIL_H

#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

#include <endian.h>
#if __BYTE_ORDER == __LITTLE_ENDIAN 
#include <byteswap.h>
# define spawn_ntoh16(x) bswap_16(x)
# define spawn_hton16(x) bswap_16(x)
# define spawn_ntoh32(x) bswap_32(x)
# define spawn_hton32(x) bswap_32(x)
# define spawn_ntoh64(x) bswap_64(x)
# define spawn_hton64(x) bswap_64(x)
#else
# define spawn_ntoh16(x) (x)
# define spawn_hton16(x) (x)
# define spawn_ntoh32(x) (x)
# define spawn_hton32(x) (x)
# define spawn_ntoh64(x) (x)
# define spawn_hton64(x) (x)
#endif

/* packs uint64_t into buf, stores value in network order */
size_t spawn_pack_uint64(void* buf, uint64_t val);

/* unpacks uint64_t from buf, returns value in host order */
size_t spawn_unpack_uint64(const void* buf, uint64_t* val);

/* print error message */
#define SPAWN_ERR(...)  \
    do { spawn_err(__FILE__, __LINE__, __VA_ARGS__); } while (0)
void spawn_err(const char* file, int line, const char* format, ...);

/* print debug message */
#ifndef NDEBUG
# define SPAWN_DBG(...) \
    do { spawn_dbg(__FILE__, __LINE__, __VA_ARGS__); } while (0)
#else
# define SPAWN_DBG(...)
#endif
void spawn_dbg(const char* file, int line, const char* format, ...);

/* wrapper for all calls to exit */
void spawn_exit(int code);

/* allocate size bytes, returns NULL if size == 0,
 * fatal error if allocation fails */
#define SPAWN_MALLOC(X) spawn_malloc(X, __FILE__, __LINE__);
void* spawn_malloc(size_t size, const char* file, int line);

/* allocate a string, returns NULL if str == NULL,
 * fatal error if allocation fails */
#define SPAWN_STRDUP(X) spawn_strdup(__FILE__, __LINE__, X);
char* spawn_strdup(const char* file, int line, const char* str);

/* allocate a formatted string */
#define SPAWN_STRDUPF(X, ...) spawn_strdupf(__FILE__, __LINE__, X, __VA_ARGS__);
char* spawn_strdupf(const char* file, int line, const char* format, ...);

/* free memory and set caller's pointer to NULL,
 * it's ok to call with pptr == NULL or *pptr == NULL */
void spawn_free(void* pptr);

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_UTIL_H */
