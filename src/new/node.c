#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <is_local_ipaddr.h>
#include <print_errmsg.h>
#include <pollfds.h>

static struct node {
    char const  * location;
    pid_t       pid;
    int         is_local;
} * node_table = NULL;

static size_t node_index = 0;
static size_t node_alloc = 0;

static struct trie {
    size_t edge[256];
} * trie_table = NULL;

static size_t trie_index = 0;
static size_t trie_alloc = 0;

enum spawn_method {
    SPAWN_METHOD_FORK,
    SPAWN_METHOD_SSH,
};

static int create_node (void);
static int trie_create (void);
static struct trie * trie_walk (struct trie *, char const *);
static int create_process (size_t, enum spawn_method);
static void stdin_handler (size_t, int);
static void stdout_handler (size_t, int);
static void stderr_handler (size_t, int);

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
        
        node_table[id].location = location;
        node_table[id].is_local = is_local_ipaddr(location);
    }

    return id;
}

extern int
node_launch (size_t id)
{
    return create_process(id, node_table[id].is_local ? SPAWN_METHOD_FORK : SPAWN_METHOD_SSH);
}

extern size_t
node_count (void)
{
    return node_index;
}
    
static int
create_node (void)
{
    if (node_index == node_alloc) {
        size_t new_alloc = node_alloc ? node_alloc << 1 : 256;

        if (new_alloc < node_alloc) {
            return -1;
        }

        node_table = realloc(node_table, sizeof(struct node[new_alloc]));

        if (!node_table) {
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

static int
create_process (size_t id, enum spawn_method method)
{
    int pipe_stdin[2], pipe_stdout[2], pipe_stderr[2];
    pid_t cpid;

#if 0
    if (-1 == pipe(pipe_stdin)) {
        print_errmsg("create_process (pipe)", errno);
        return -1;
    }

    if (-1 == pipe(pipe_stdout)) {
        print_errmsg("create_process (pipe)", errno);
        return -1;
    }

    if (-1 == pipe(pipe_stderr)) {
        print_errmsg("create_process (pipe)", errno);
        return -1;
    }
#endif

    cpid = fork();

    if (-1 == cpid) {
        print_errmsg("create_process (fork)", errno);
        return -1;
    }

    else if (!cpid) {
#if 0
        /*
         * Child
         */
        dup2(pipe_stdin[0], STDIN_FILENO);
        dup2(pipe_stdout[1], STDOUT_FILENO);
        dup2(pipe_stderr[1], STDERR_FILENO);

        close(pipe_stdin[1]);
        close(pipe_stdout[0]);
        close(pipe_stderr[0]);
#endif

        /*
         * Why isn't the second parameter of execvp defined to be an array of
         * pointers to constant characters?
         */
        switch (method) {
            case SPAWN_METHOD_FORK:
                execlp("hostname", "hostname", (char *)NULL);
                break;
            case SPAWN_METHOD_SSH:
                execlp("ssh", "ssh", node_table[id].location, "hostname", (char *)NULL);
                break;
        }

        print_errmsg("create_child (execlp)", errno);
        _exit(EXIT_FAILURE);
    }

    else {
        struct pollfds_param stdin_param, stdout_param, stderr_param;

        node_table[id].pid = cpid;

#if 0
        stdin_param.fd = pipe_stdin[1];
        stdout_param.fd = pipe_stdout[0];
        stderr_param.fd = pipe_stderr[0];

        stdin_param.fd_handler = stdin_handler;
        stdout_param.fd_handler = stdout_handler;
        stderr_param.fd_handler = stderr_handler;

        fcntl(stdin_param.fd, F_SETFL, O_NONBLOCK);
        fcntl(stdout_param.fd, F_SETFL, O_NONBLOCK);
        fcntl(stderr_param.fd, F_SETFL, O_NONBLOCK);

        close(pipe_stdin[0]);
        close(pipe_stdout[1]);
        close(pipe_stderr[1]);

        pollfds_add(id, stdin_param, stdout_param, stderr_param);
#endif
    }

    return 0;
}

static void
stdin_handler (size_t id, int fd)
{
    return;
    printf("[%s] stdin>", node_table[id].location);
    fflush(stdout);
    dup2(fd, STDIN_FILENO);
}

static void
stdout_handler (size_t id, int fd)
{
    char buffer[80];
    int nread = 0;

    printf("[%s] stdout>", node_table[id].location);
    fflush(stdout);

    do {
        nread = read(fd, buffer, 80);

        if (0 == nread) {
            /* EOF */
        }

        else if (-1 == nread) {
            switch (errno) {
                case EAGAIN:
#if EWOULDBLOCK != EAGAIN
                case EWOULDBLOCK:
#endif
                    break;
                default:
                    print_errmsg("stdout_handler (read)", errno);
                    break;
            }
        }

        else {
            write(STDOUT_FILENO, buffer, nread);
        }
    } while (nread > 0);
}

static void
stderr_handler (size_t id, int fd)
{
    char buffer[80];
    int nread = 0;

    fprintf(stderr, "[%s] stderr>", node_table[id].location);

    do {
        nread = read(fd, buffer, 80);

        if (0 == nread) {
            /* EOF */
        }

        else if (-1 == nread) {
            switch (errno) {
                case EAGAIN:
#if EWOULDBLOCK != EAGAIN
                case EWOULDBLOCK:
#endif
                    break;
                default:
                    print_errmsg("stderr_handler (read)", errno);
                    break;
            }
        }

        else {
            write(STDERR_FILENO, buffer, nread);
        }
    } while (nread > 0);
}

