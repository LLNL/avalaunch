#ifndef SESSION_H
#define SESSION_H 1

struct session_t;

struct session_t * session_init (int argc, char * argv[]);
int get_spawn_id (struct session_t * s);
int session_start (struct session_t * s);
void session_destroy (struct session_t *);

#endif
