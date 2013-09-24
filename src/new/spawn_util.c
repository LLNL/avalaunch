#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "spawn_internal.h"

static char  my_prog[] = "mpispawn";
static char* my_host = NULL;
static pid_t my_pid;

/* initialize our hostname and pid if we don't have it */
static void get_name()
{
  if (my_host == NULL) {
    char hostname[1024];
    if (gethostname(hostname, sizeof(hostname)) == 0) {
      my_host = strdup(hostname);
    } else {
      my_host = strdup("NULLHOST");
    }
    my_pid = getpid();
  }
  return;
}

/* print error message */
void spawn_err(const char* file, int line, const char* format, ...)
{
  get_name();

  va_list args;
  char* str = NULL;

  /* check that we have a format string */
  if (format == NULL) {
    return;
  }

  /* compute the size of the string we need to allocate */
  va_start(args, format);
  int size = vsnprintf(NULL, 0, format, args) + 1;
  va_end(args);

  /* allocate and print the string */
  if (size > 0) {
    /* NOTE: we don't use spawn_malloc to avoid infinite loop */
    str = (char*) malloc(size);
    if (str == NULL) {
      /* error */
      return;
    }

    /* format message */
    va_start(args, format);
    vsnprintf(str, size, format, args);
    va_end(args);

    /* TODO: insert timestamp */

    /* print message */
    fprintf(stderr, "%s on %s:%d at TIME: %s @ %s:%d\n",
        my_prog, my_host, my_pid, str, file, line
    );
    fflush(stderr);

    free(str);
  }

  return;
}

/* wrapper to exit (useful to place debugger breakpoints) */
void spawn_exit(int code)
{
  /* TODO: may want to print message and sleep before exitting,
   * capturing under a debugger */
  exit(code);
}

/* allocate size bytes, returns NULL if size == 0,
 * fatal error if allocation fails */
void* spawn_malloc(size_t size, const char* file, int line)
{
  void* ptr = NULL;
  if (size > 0) {
    ptr = malloc(size);
    if (ptr == NULL) {
      /* error */
      spawn_err(file, line, "Failed to allocate %llu bytes", (unsigned long long) size);
      spawn_exit(1);
    }
  }
  return ptr;
}

/* free memory and set pointer to NULL */
void spawn_free(void* arg_pptr)
{
  void** pptr = (void**) arg_pptr;
  if (pptr != NULL) {
    /* get pointer to memory and call free if it's not NULL*/
    void* ptr = *pptr;
    if (ptr != NULL) {
      free(ptr);
    }

    /* set caller's pointer to NULL */
    *pptr = NULL;
  }
  return;
}

