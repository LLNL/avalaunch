#ifndef NODE_H
#define NODE_H 1

#include <child.h>

int node_initialize (void);
int node_finalize (void);
int node_get_id (char const * location);
struct child_s * node_launch (size_t id);

#endif
