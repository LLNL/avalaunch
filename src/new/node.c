#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <is_local_ipaddr.h>
#include <child.h>

static struct node {
    char const  * location;
    pid_t       pid;
    int         is_local;
    int         stdin;
    int         stdout;
    int         stderr;
} * node_table = NULL;

static size_t node_index = 0;
static size_t node_alloc = 0;

static struct trie {
    size_t edge[256];
} * trie_table = NULL;

static size_t trie_index = 0;
static size_t trie_alloc = 0;

static int
create_node (void)
{
    if (node_index == node_alloc) {
        size_t new_alloc = node_alloc ? node_alloc << 1 : 256;

        if (new_alloc < node_alloc) {
            return -1;
        }

        node_table = realloc(node_table, sizeof(struct node[new_alloc]));
        if (NULL == node_table) {
            return -1;
        }

        else {
            node_alloc = new_alloc;
        }
    }

    return node_index++;
}

static int
trie_create (void)
{
    if (trie_index == trie_alloc) {
        size_t new_alloc = trie_alloc ? trie_alloc << 1 : 256;

        if (new_alloc < trie_alloc) {
            return -1;
        }

        trie_table = realloc(trie_table, sizeof(struct trie[new_alloc]));
        if (NULL == trie_table) {
            return -1;
        }

        else {
            trie_alloc = new_alloc;
        }
    }

    memset(&trie_table[trie_index], 0, sizeof(struct trie));

    return trie_index++;
}

static struct trie *
trie_follow_edge (struct trie * root, int edge_index)
{
    if (0 == root->edge[edge_index]) {
        int trie_index = trie_create();

        if (0 > trie_index) {
            return NULL;
        }

        root->edge[edge_index] = trie_index;
    }

    return &trie_table[root->edge[edge_index]];
}

static inline int
trie_get_id (struct trie * t)
{
    return t->edge[0] - 1;
}

static inline void
trie_set_id (struct trie * t, size_t id)
{
    t->edge[0] = id + 1;
}

static struct trie *
trie_walk (struct trie * root, char const * location)
{
    int edge_index = *location;

    if (!edge_index) {
        return root;
    }

    if (0 == root->edge[edge_index]) {
        int trie_index = trie_create();

        if (0 > trie_index) {
            return NULL;
        }

        root->edge[edge_index] = trie_index;
    }

    return trie_walk(&trie_table[root->edge[edge_index]], ++location);
}

extern int
node_initialize (void)
{
    if (trie_table) {
        return -1;
    }

    return trie_create();
}

extern int
node_finalize (void)
{
    free(trie_table);
    trie_table = NULL;
    trie_index = 0;
    trie_alloc = 0;

    free(node_table);
    node_table = NULL;
    node_index = 0;
    node_alloc = 0;
}

extern int
node_get_id (char const * location)
{
    struct trie * t = trie_walk(trie_table, location);
    int id = t ? t->edge[0] : -1;

    if (0 == id) {
        /*
         * first time processing this location
         */
        struct trie * t_hostname, * t_ip;
        int id = create_node();
        int is_local;

        if (0 > id) {
            return -1;
        }

        t->edge[0] = id + 1;
#if 0
        is_local = is_local_ipaddr(location);
#else
        is_local = 0;
#endif
        node_table[id].location = location;
        node_table[id].is_local = is_local;
        printf("%s is %slocal\n", location, is_local ? "" : "not ");
    }

    return id;
}

extern struct child_s *
node_launch (size_t id)
{
    char const * command[][4] = {
        { "echo", "foo", "bar", NULL },
        { "ssh", node_table[id].location, "hostname", NULL }
    };

    return create_child(command[node_table[id].is_local ? 0 : 1]);
}

extern size_t
node_count (void)
{
    return node_index;
}
