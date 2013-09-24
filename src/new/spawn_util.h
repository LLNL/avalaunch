#ifndef SPAWN_UTIL_H
#define SPAWN_UTIL_H

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* print error message */
#define SPAWN_ERR(...) spawn_err(__FILE__, __LINE__, __VA_ARGS__);
void spawn_err(const char* file, int line, const char* format, ...);

/* wrapper for all calls to exit */
void spawn_exit(int code);

/* allocate size bytes, returns NULL if size == 0,
 * fatal error if allocation fails */
#define SPAWN_MALLOC(X) spawn_malloc(X, __FILE__, __LINE__);
void* spawn_malloc(size_t size, const char* file, int line);

/* free memory and set caller's pointer to NULL,
 * it's ok to call with pptr == NULL or *pptr == NULL */
void spawn_free(void* pptr);

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_UTIL_H */
