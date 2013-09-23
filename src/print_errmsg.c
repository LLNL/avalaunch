#include <errno.h>
#include <stdio.h>
#include <pthread.h>

/*
 * Thread safe way to print error message
 */
extern void
print_errmsg (char * prefix, int error)
{
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_lock(&mutex);
    fprintf(stderr, "%s: %s\n", prefix, strerror(error));
    pthread_mutex_unlock(&mutex);
}
