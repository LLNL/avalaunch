/*
 * Local headers
 */
#include <unistd.h>
#include <spawn_internal.h>
#include <strmap.h>
#include <node.h>
#include <print_errmsg.h>

/*
 * System headers
 */
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/utsname.h>

typedef struct spawn_tree_struct {
    int rank;                      /* our global rank (0 to ranks-1) */
    int ranks;                     /* number of nodes in tree */
    spawn_net_channel* parent_ch;  /* channel to our parent */
    int children;                  /* number of children we have */
    int* child_ranks;              /* global ranks of our children */
    spawn_net_channel** child_chs; /* channels to children */
} spawn_tree;

struct session_t {
    char const * spawn_exe;       /* executable path */
    char const * spawn_parent;    /* name of our parent's endpoint */
    char const * spawn_id;        /* id given to us by parent, we echo this back on connect */
    char const * ep_name;         /* name of our endpoint */
    spawn_net_endpoint* ep;       /* our endpoint */
    spawn_tree * tree;            /* data structure that tracks tree info */
    strmap* params;               /* spawn parameters sent from parent after connect */
};

static int call_stop_event_handler = 0;
static int call_node_finalize = 0;

void session_destroy (struct session_t *);

/* allocates a string and returns current working dir,
 * caller should later free string with spawn_free */
static char* spawn_getcwd()
{
    /* TODO: exit on error */
    size_t len = 128;
    char* cwd = NULL;
    while (!cwd) {
        cwd = SPAWN_MALLOC(len);
        if (NULL == getcwd(cwd, len)) {
            switch (errno) {
                case ERANGE:
                    spawn_free(&cwd);
                    len <<= 1;

                    if (len < 128) {
                        return NULL;
                    }
                    break;
                default:
                    SPAWN_ERR("getcwd() errno=%d %s", errno, strerror(errno));
                    break;
            }
        }
    }
    return cwd;
}

/* returns hostname in a string, caller responsible
 * for freeing with spawn_free */
static char* spawn_hostname()
{
    struct utsname buf;
    uname(&buf);
    char* name = SPAWN_STRDUP(buf.nodename);
    return name;
}

static spawn_tree* tree_new()
{
    spawn_tree* t = (spawn_tree*) SPAWN_MALLOC(sizeof(spawn_tree));

    t->rank        = -1;
    t->ranks       = -1;
    t->parent_ch   = SPAWN_NET_CHANNEL_NULL;
    t->children    = 0;
    t->child_ranks = NULL;
    t->child_chs   = NULL;

    return t;
}

static void tree_delete(spawn_tree** pt)
{
    if (pt == NULL) {
        return;
    }

    spawn_tree* t = *pt;

    /* free off each child channel if we have them */
    int i;
    for (i = 0; i < t->children; i++) {
        spawn_net_disconnect(&(t->child_chs[i]));
    }

    /* free child ids and channels */
    spawn_free(&(t->child_ranks));
    spawn_free(&(t->child_chs));

    /* free connection to parent */
    spawn_net_disconnect(&(t->parent_ch));

    /* free tree structure itself */
    spawn_free(pt);
}

static void tree_create_kary(int rank, int ranks, int k, spawn_tree* t)
{
    int i;

    /* compute the maximum number of children this task may have */
    int max_children = k;

    /* prepare data structures to store our parent and children */
    t->rank  = rank;
    t->ranks = ranks;

    if (max_children > 0) {
        t->child_ranks  = (int*) SPAWN_MALLOC(max_children * sizeof(int));
        t->child_chs  = (spawn_net_channel**) SPAWN_MALLOC(max_children * sizeof(spawn_net_channel));
    }

    for (i = 0; i < max_children; i++) {
        t->child_chs[i] = SPAWN_NET_CHANNEL_NULL;
    }

    /* find our parent rank and the ranks of our children */
    int size = 1;
    int tree_size = 0;
    while (1) {
        /* determine whether we're a parent in the current round */
        if (tree_size <= rank && rank < tree_size + size) {
            /* we're a parent in this round, compute ranks of first and last child */
            int group_id = rank - tree_size;
            int offset_rank = tree_size + size;
            int first_child = offset_rank + group_id * k;
            int last_child = first_child + (k - 1);

            /* compute number of children */
            t->children = 0;
            if (first_child < ranks) {
                /* if our first child is within range,
                 * check that our last child is too */
                if (last_child >= ranks) {
                    last_child = ranks - 1;
                }
               t->children = last_child - first_child + 1;
            }

            /* record ranks of our children */
            for (i = 0; i < t->children; i++) {
                t->child_ranks[i] = first_child + i;
                //t->child_chs[i] = NULL_CHANNEL;
            }

            /* break the while loop */
            break;
        }

        /* go to next round */
        tree_size += size;
        size *= k;
    }

    SPAWN_DBG("Rank %d has %d children", t->rank, t->children);
    for (i = 0; i < t->children; i++) {
        SPAWN_DBG("Rank %d: Child %d of %d has rank=%d", t->rank, (i + 1), t->children, t->child_ranks[i]);
    }
}

/* searches user's path for command executable, returns full path
 * in newly allocated string, returns NULL if not found */
char* path_search(const char* command)
{
    char* path = NULL;

    /* check that we got a real string for the command */
    if (command == NULL) {
        return path;
    }

    /* if command starts with '/', it's already absolute */
    if (command[0] == '/') {
        path = SPAWN_STRDUP(command);
        return path;
    }

    /* get user's path */
    const char* path_env = getenv("PATH");
    if (path_env == NULL) {
        /* $PATH is not set, bail out */
        return path;
    }

    /* make a copy of path to run strtok */
    char* path_env_copy = SPAWN_STRDUP(path_env);

    /* search entries in path, breaking on ':' */
    const char* prefix = strtok(path_env_copy, ":");
    while (prefix != NULL) {
        /* create path to candidate item */
        path = SPAWN_STRDUPF("%s/%s", prefix, command);

        /* break if we find an executable */
        if (access(path, X_OK) == 0) {
            /* TODO: should we run realpath here? */
            break;
        }

        /* otherwise, free the item and try the next
         * path entry */
        spawn_free(&path);
        prefix = strtok(NULL, ":");
    }

    /* free copy of path */
    spawn_free(&path_env_copy);

    return path;
}

/* serialize an array of values into a newly allocated string */
static char* serialize_to_str(const strmap* map, const char* key_count, const char* key_prefix)
{
    int i;

    /* determine number of arguments */
    const char* count_str = strmap_get(map, key_count);
    if (count_str == NULL) {
        SPAWN_ERR("Failed to read count key `%s'", key_count);
        return NULL;
    }
    int count = atoi(count_str);

    /* total up space to serialize */
    size_t size = 0;
    for (i = 0; i < count; i++) {
        /* get next value */
        const char* val = strmap_getf(map, "%s%d", key_prefix, i);
        if (val == NULL) {
            SPAWN_ERR("Missing key `%s%d'", key_prefix, i);
            return NULL;
        }

        /* the +1 is for a single space between entries,
         * and a terminating NUL after last */
        size += strlen(val) + 1;
    }

    /* allocate space to serialize into */
    char* str = (char*) SPAWN_MALLOC(size);

    /* walk through map again and copy values into buffer */
    char* ptr = str;
    for (i = 0; i < count; i++) {
        /* get value and its length */
        const char* val = strmap_getf(map, "%s%d", key_prefix, i);
        size_t len = strlen(val);

        /* copy value to buffer */
        memcpy(ptr, val, len);
        ptr += len;

        /* tack on single space or terminating NUL */
        if (i < count-1) {
            *ptr = ' ';
        } else {
            *ptr = '\0';
        }
        ptr++;
    }

    return str;
}

/* given a remote host, exec rsh or ssh of specified exe in named
 * current working directory, using provided arguments and env
 * variables.  The shell type is selected by the SH key, which
 * in turn is set via the MV2_SPAWN_SH variable. */
static int exec_remote(
    const char* host,
    const strmap* params,
    const char* cwd,
    const char* exe,
    const strmap* argmap,
    const strmap* envmap)
{
    /* get name of remote shell */
    const char* shname = strmap_get(params, "SH");
    if (shname == NULL) {
        SPAWN_ERR("Failed to read name of remote shell from SH key");
        return 1;
    }

    /* determine whether to use rsh or ssh */
    if (strcmp(shname, "rsh") != 0 &&
        strcmp(shname, "ssh") != 0)
    {
        SPAWN_ERR("Unknown launch remote shell: `%s'", shname);
        return 1;
    }

    /* lookup paths to env and remote sh commands from params */
    const char* envpath = strmap_get(params, "env");
    const char* shpath  = strmap_get(params, shname);
    if (envpath == NULL) {
        SPAWN_ERR("Path to env command not set");
        return 1;
    }
    if (shpath == NULL) {
        SPAWN_ERR("Path to sh command not set");
        return 1;
    }

    /* create strings for environment variables and arguments */
    char* envstr = serialize_to_str(envmap, "ENVS", "ENV");
    char* argstr = serialize_to_str(argmap, "ARGS", "ARG");

    /* create command to execute with shell */
    char* app_command = SPAWN_STRDUPF("cd %s && %s %s %s",
        cwd, envpath, envstr, argstr);

    /* exec process, we only return on error */
    execl(shpath, shname, host, app_command, (char*)0);
    SPAWN_ERR("Failed to exec program (execl errno=%d %s)", errno, strerror(errno));

    /* clean up in case we do happen to fall through */
    spawn_free(&app_command);
    spawn_free(&argstr);
    spawn_free(&envstr);

    return 1;
}

/* exec sh shell to run specified exe in named current working
 * directory, using provided arguments and env variables */
static int exec_shell(
    const strmap* params,
    const char* cwd,
    const char* exe,
    const strmap* argmap,
    const strmap* envmap)
{
    /* lookup paths to env and sh commands from params */
    const char* envpath = strmap_get(params, "env");
    const char* shpath  = strmap_get(params, "sh");
    if (envpath == NULL) {
        SPAWN_ERR("Path to env command not set");
        return 1;
    }
    if (shpath == NULL) {
        SPAWN_ERR("Path to sh command not set");
        return 1;
    }

    /* create strings for environment variables and arguments */
    char* envstr = serialize_to_str(envmap, "ENVS", "ENV");
    char* argstr = serialize_to_str(argmap, "ARGS", "ARG");

    /* create command to execute with shell */
    char* app_command = SPAWN_STRDUPF("cd %s && %s %s %s",
        cwd, envpath, envstr, argstr);

    /* exec process, we only return on error */
    execl(shpath, "sh", "-c", app_command, (char*)0);
    SPAWN_ERR("Failed to exec program (execl errno=%d %s)", errno, strerror(errno));

    /* clean up in case we do happen to fall through */
    spawn_free(&app_command);
    spawn_free(&argstr);
    spawn_free(&envstr);

    return 1;
}

/* directly exec specified exe in named current working
 * directory, using provided arguments and env variables */
static int exec_direct(
    const strmap* params,
    const char* cwd,
    const char* exe,
    const strmap* argmap,
    const strmap* envmap)
{
    int i;

    /* TODO: setup stdin and friends */

    /* TODO: copy environment from current process? */

    /* change to specified working directory (exec'd process will
     * inherit this) */
    if (chdir(cwd) != 0) {
        SPAWN_ERR("Failed to change directory to `%s' (errno=%d %s)", cwd, errno, strerror(errno));
        return 1;
    }

    /* determine number of arguments */
    const char* args_str = strmap_get(argmap, "ARGS");
    if (args_str == NULL) {
        SPAWN_ERR("Failed to read ARGS key");
        return 1;
    }
    int args = atoi(args_str);

    /* allocate memory for argv array (one extra for terminating NULL) */
    char** argv = (char**) SPAWN_MALLOC((args + 1) * sizeof(char*));

    /* fill in argv array and set last entry to NULL */
    for (i = 0; i < args; i++) {
        argv[i] = strmap_getf(argmap, "ARG%d", i);
    }
    argv[args] = (char*) NULL;

    /* determine number of environment variables */
    const char* envs_str = strmap_get(envmap, "ENVS");
    if (envs_str == NULL) {
        SPAWN_ERR("Failed to read ENVS key");
        spawn_free(&argv);
        return 1;
    }
    int envs = atoi(envs_str);

    /* allocate memory for envp array (one extra for terminating NULL) */
    char** envp = (char**) SPAWN_MALLOC((envs + 1) * sizeof(char*));

    /* fill in envp array and set last entry to NULL */
    for (i = 0; i < envs; i++) {
        envp[i] = strmap_getf(envmap, "ENV%d", i);
    }
    envp[envs] = (char*) NULL;

    /* exec process, we only return on error */
    execve(exe, argv, envp);
    SPAWN_ERR("Failed to exec program (execve errno=%d %s)", errno, strerror(errno));

    /* clean up in case we do happen to fall through */
    spawn_free(&envp);
    spawn_free(&argv);

    return 1;
}

/* fork process, child execs specified command */
static int temp_launch(
    const char* host,
    const strmap* params,
    const char* cwd,
    const char* exe,
    const strmap* argmap,
    const strmap* envmap)
{
#if 0
    int pipe_stdin[2], pipe_stdout[2], pipe_stderr[2];

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

    pid_t cpid = fork();

    if (-1 == cpid) {
        SPAWN_ERR("create_process (fork() errno=%d %s)", errno, strerror(errno));
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

        /* TODO: execlp searches the user's path looking for the launch command,
         * so this could create a bunch of traffic on the file system if there
         * are lots of extra entries in user's path */
        if (host == NULL) {
            /* local launch, use sh or just direct launch */
            const char* local = strmap_get(params, "LOCAL");
            if (local == NULL) {
                SPAWN_ERR("Failed to read LOCAL key");
            } else {
                if (strcmp(local, "sh") == 0) {
                    exec_shell(params, cwd, exe, argmap, envmap);
                } else if (strcmp(local, "direct") == 0) {
                    exec_direct(params, cwd, exe, argmap, envmap);
                } else {
                    SPAWN_ERR("Unknown LOCAL key value `%s'", local);
                }
            }
        } else {
            exec_remote(host, params, cwd, exe, argmap, envmap);
        }

        SPAWN_ERR("create_child (execlp errno=%d %s)", errno, strerror(errno));
        _exit(EXIT_FAILURE);
    }

    else {
        //struct pollfds_param stdin_param, stdout_param, stderr_param;

//        t.child_pids[index] = cpid;

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

int get_spawn_id(struct session_t* s)
{
    if (s->spawn_id == NULL) {
        /* I am the root of the tree */
        return 0;
    }
    return atoi(s->spawn_id);
}

static void signal_to_root(const struct session_t* s)
{
    spawn_tree* t = s->tree;
    int children = t->children;

    /* doesn't really matter what we send yet */
    char signal = 'A';

    /* wait for signal from all children */
    int i;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_read(ch, &signal, sizeof(char));
    }

    /* forward signal to parent */
    if (t->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_write(t->parent_ch, &signal, sizeof(char));
    }

    return;
}

static void signal_from_root(const struct session_t* s)
{
    spawn_tree* t = s->tree;
    int children = t->children;

    /* doesn't really matter what we send yet */
    char signal = 'A';

    /* wait for signal from parent */
    if (t->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_read(t->parent_ch, &signal, sizeof(char));
    }

    /* forward signal to children */
    int i;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_write(ch, &signal, sizeof(char));
    }

    return;
}

static void bcast_strmap(strmap* map, const spawn_tree* t)
{
    /* read map from parent, if we have one */
    spawn_net_channel* p = t->parent_ch;
    if (p != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_read_strmap(p, map);
    }

    /* send map to children */
    int i;
    int children = t->children;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_write_strmap(ch, map);
    }

    return;
}

static void allgather_strmap(strmap* map, const spawn_tree* t)
{
    /* gather input from children */
    int i;
    int children = t->children;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_read_strmap(ch, map);
    }

    /* forward map to parent */
    spawn_net_channel* p = t->parent_ch;
    if (p != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_write_strmap(p, map);
    }

    /* broadcast map from root */
    bcast_strmap(map, t);

    return;
}

static void ring_scan(strmap* input, strmap* output, const spawn_tree* t)
{
    int i;
    int children = t->children;
    spawn_net_channel* parent_ch = t->parent_ch;

    /* allocate strmap for each child */
    strmap** maps  = (strmap**) SPAWN_MALLOC(children * sizeof(strmap*));
    for (i = 0; i < children; i++) {
        maps[i] = strmap_new();
    }

    /* gather input from children if we have any */
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_read_strmap(ch, maps[i]);
    }

    /* compute our leftmost and right most address */
    const char* leftmost  = strmap_get(input, "LEFT");
    const char* rightmost = NULL;
    for (i = 0; i < children; i++) {
        /* scan children left-to-right for leftmost value */
        if (leftmost == NULL) {
            leftmost = strmap_get(maps[i], "LEFT");
        }

        /* scan children right-to-left for rightmost value */
        if (rightmost == NULL) {
            int right_index = children - 1 - i;
            rightmost = strmap_get(maps[right_index], "RIGHT");
        }
    }
    if (rightmost == NULL) {
        rightmost = strmap_get(input, "RIGHT");
    }

    strmap* recv = strmap_new();
    if (parent_ch != SPAWN_NET_CHANNEL_NULL) {
        /* construct strmap to send to parent */
        strmap* send = strmap_new();
        if (leftmost != NULL && rightmost != NULL) {
            strmap_set(send, "LEFT",  leftmost);
            strmap_set(send, "RIGHT", rightmost);
        }
        spawn_net_write_strmap(parent_ch, send);
        strmap_delete(&send);

        /* receive map from parent */
        spawn_net_read_strmap(parent_ch, recv);
    } else {
        /* we are the root, so build recv,
         * note that we wrap the ends to create a ring */
        if (leftmost != NULL && rightmost != NULL) {
            strmap_set(recv, "LEFT",  rightmost);
            strmap_set(recv, "RIGHT", leftmost);
        }
    }

    /* TODO: handle empty maps */

    /* send output to each child */
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];

        /* construct strmap to send to child */
        strmap* send = strmap_new();

        const char* left;
        if (i == 0) {
            /* first child uses right address from us, its parent */
            left = strmap_get(input, "RIGHT");
        } else {
            /* otherwise, use right address of child's left sibling */
            left = strmap_get(maps[i-1], "RIGHT");
        }
        strmap_set(send, "LEFT", left);

        const char* right;
        if (i < children-1) {
            /* get left address on child's right sibling */
            right = strmap_get(maps[i+1], "LEFT");
        } else {
            /* for last child, get the address our parent says is our right */
            right = strmap_get(recv, "RIGHT");
        }
        strmap_set(send, "RIGHT", right);

        spawn_net_write_strmap(ch, send);
        strmap_delete(&send);
    }

    /* set left address in our output */
    const char* left  = strmap_get(recv, "LEFT");
    strmap_set(output, "LEFT", left);

    /* set right address in our output */
    const char* right = strmap_get(recv, "RIGHT");
    if (children > 0) {
        /* use left address of our first child if we have one */
        right = strmap_get(maps[0], "LEFT");
    }
    strmap_set(output, "RIGHT", right);

    /* delete map from parent */
    strmap_delete(&recv);

    /* delete strmaps */
    for (i = 0; i < children; i++) {
        strmap_delete(&maps[i]);
    }
    spawn_free(&maps);

    return;
}

static void ring_exchange(struct session_t* s, const strmap* params, const spawn_net_endpoint* ep)
{
    int i, tid, tid_ring;
    spawn_tree* t = s->tree;
    int rank = t->rank;

    /* wait for signal from root before we start exchange */
    if (!rank) { tid_ring = begin_delta("ring exchange"); }
    signal_from_root(s);

    /* get number of procs we should here from */
    const char* app_procs_str = strmap_get(params, "PPN");
    int numprocs = atoi(app_procs_str);

    /* get total number of procs in job */
    int ranks = t->ranks * numprocs;

    /* allocate a strmap for each child */
    strmap** maps = (strmap**) SPAWN_MALLOC(numprocs * sizeof(strmap*));
    for (i = 0; i < numprocs; i++) {
        maps[i] = strmap_new();
    }

    /* allocate a channel for each child */
    spawn_net_channel** chs = (spawn_net_channel**) SPAWN_MALLOC(numprocs * sizeof(spawn_net_channel*));

    /* wait for children to connect */
    if (!rank) { tid = begin_delta("ring accept"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        chs[i] = spawn_net_accept(ep);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for address from each child */
    if (!rank) { tid = begin_delta("ring read children"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        spawn_net_read_strmap(chs[i], maps[i]);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }


    /* compute scan on tree */
    if (!rank) { tid = begin_delta("ring scan"); }
    signal_from_root(s);

    strmap* output = strmap_new();

    /* get addresses of our left-most and right-most children */
    strmap* input = strmap_new();
    if (numprocs > 0) {
        const char* leftmost  = strmap_get(maps[0], "ADDR");
        const char* rightmost = strmap_get(maps[numprocs-1], "ADDR");
        strmap_set(input, "LEFT",  leftmost);
        strmap_set(input, "RIGHT", rightmost);
    }

    /* execute the scan */
    ring_scan(input, output, t);

    /* free the input */
    strmap_delete(&input);

    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* compute left and right addresses for each of our children */
    if (!rank) { tid = begin_delta("ring write children"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        /* since each spawn proc is creating the same number of tasks,
         * we can hardcode a child rank relative to the spawn rank */
        int child_rank = rank * numprocs + i;

        /* send init info */
        strmap* init = strmap_new();
        strmap_setf(init, "RANK=%d",  child_rank);
        strmap_setf(init, "RANKS=%d", ranks);

        const char* left;
        if (i == 0) {
            /* get the address our parent says is our left */
            left = strmap_get(output, "LEFT");
        } else {
            /* get rightmost address on left side */
            left = strmap_get(maps[i-1], "ADDR");
        }
        strmap_set(init, "LEFT", left);

        const char* right;
        if (i < numprocs-1) {
            /* get leftmost address on right side */
            right = strmap_get(maps[i+1], "ADDR");
        } else {
            /* get the address our parent says is our right */
            right = strmap_get(output, "RIGHT");
        }
        strmap_set(init, "RIGHT", right);

        spawn_net_write_strmap(chs[i], init);
        strmap_delete(&init);
    }

    /* delete strmap from parent */
    strmap_delete(&output);

    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* disconnect from each child */
    if (!rank) { tid = begin_delta("ring disconnect"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        spawn_net_disconnect(&chs[i]);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* delete each child strmap */
    for (i = 0; i < numprocs; i++) {
        strmap_delete(&maps[i]);
    }

    /* signal root to let it know PMI bcast has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid_ring); }

    return;
}

static void pmi_exchange(struct session_t* s, const strmap* params, const spawn_net_endpoint* ep)
{
    int i, tid, tid_pmi;
    int rank = s->tree->rank;

    /* wait for signal from root before we start PMI exchange */
    if (!rank) { tid_pmi = begin_delta("pmi exchange"); }
    signal_from_root(s);

    /* allocate strmap */
    strmap* pmi_strmap = strmap_new();

    /* get number of procs we should here from */
    const char* app_procs_str = strmap_get(params, "PPN");
    int numprocs = atoi(app_procs_str);

    /* get total number of procs in job */
    int ranks = s->tree->ranks * numprocs;

    /* get global jobid */
    int jobid = 0;

    /* allocate a channel for each child */
    spawn_net_channel** chs = (spawn_net_channel**) SPAWN_MALLOC(numprocs * sizeof(spawn_net_channel*));

    /* wait for children to connect */
    if (!rank) { tid = begin_delta("pmi accept"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        chs[i] = spawn_net_accept(ep);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* send init data to children */
    if (!rank) { tid = begin_delta("pmi init info"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        /* since each spawn proc is creating the same number of tasks,
         * we can hardcode a child rank relative to the spawn rank */
        int child_rank = rank * numprocs + i;

        /* send init info */
        strmap* init = strmap_new();
        strmap_setf(init, "RANK=%d",  child_rank);
        strmap_setf(init, "RANKS=%d", ranks);
        strmap_setf(init, "JOBID=%d", jobid);
        spawn_net_write_strmap(chs[i], init);
        strmap_delete(&init);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for BARRIER messages */
    if (!rank) { tid = begin_delta("pmi read children"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];
        char* cmd = spawn_net_read_str(ch);
        //printf("spawn %d received cmd %s from child %d\n", rank, cmd, i);
        spawn_net_read_strmap(ch, pmi_strmap);
        spawn_free(&cmd);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* allgather strmaps across spawn processes */
    if (!rank) { tid = begin_delta("pmi allgather"); }
    signal_from_root(s);
    allgather_strmap(pmi_strmap, s->tree);
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for signal before starting next phase */
    if (!rank) { tid = begin_delta("pmi write children"); }
    signal_from_root(s);

    /* send BARRIER message */
    char cmd_barrier[] = "BARRIER";
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];
        spawn_net_write_str(ch, cmd_barrier);
    }

    /* handle first GET request */
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];
        char* cmd = spawn_net_read_str(ch);
        char* key = spawn_net_read_str(ch);
        const char* val = strmap_get(pmi_strmap, key);
        //printf("cmd=%s key=%s val=%s\n", cmd, key, val);
        spawn_net_write_str(ch, val);
        spawn_free(&key);
        spawn_free(&cmd);
    }

    /* handle second GET request */
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];
        char* cmd = spawn_net_read_str(ch);
        char* key = spawn_net_read_str(ch);
        const char* val = strmap_get(pmi_strmap, key);
        //printf("cmd=%s key=%s val=%s\n", cmd, key, val);
        spawn_net_write_str(ch, val);
        spawn_free(&key);
        spawn_free(&cmd);
    }

    /* signal root to let it know PMI write has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for FINALIZE */
    if (!rank) { tid = begin_delta("pmi finalize"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];
        char* cmd = spawn_net_read_str(ch);
        //printf("spawn %d received cmd %s from child %d\n", rank, cmd, i);
        spawn_net_disconnect(&chs[i]);
        spawn_free(&cmd);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    spawn_free(&chs);

    /* signal root to let it know PMI bcast has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid_pmi); }

    if (rank == 0) {
        printf("PMI map:\n");
        strmap_print(pmi_strmap);
        printf("\n");
    }

    strmap_delete(&pmi_strmap);

    return;
}

static void app_start(struct session_t* s, const strmap* params)
{
    /* TODO: for each process group we start, we'll want to
     * create a data structure to record number, pids, comm
     * channels, and initial parameters, etc */

    int i, tid;
    int rank = s->tree->rank;

    /* read executable name and number of procs */
    const char* app_exe = strmap_get(params, "EXE");
    const char* app_dir = strmap_get(params, "CWD");
    const char* app_procs_str = strmap_get(params, "PPN");
    int numprocs = atoi(app_procs_str);

    /* TODO: bcast application executables */

    /* check flag for whether we should initiate PMI exchange */
    const char* use_pmi_str = strmap_get(params, "PMI");
    int use_pmi = atoi(use_pmi_str);

    /* check flag for whether we should initiate RING exchange */
    const char* use_ring_str = strmap_get(params, "RING");
    int use_ring = atoi(use_ring_str);

    /* check for flag on whether we should use FIFO */
    const char* use_fifo_str = strmap_get(params, "FIFO");
    int use_fifo = atoi(use_fifo_str);

    /* create endpoint for PMI */
    if (!rank) { tid = begin_delta("open init endpoint"); }
    signal_from_root(s);
    spawn_net_endpoint* ep = s->ep;
    const char* ep_name = spawn_net_name(ep);
    if (use_pmi || use_ring) {
      if (use_fifo) {
        ep = spawn_net_open(SPAWN_NET_TYPE_FIFO);
        ep_name = spawn_net_name(ep);
      }
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* launch app procs */
    if (!rank) { tid = begin_delta("launch app procs"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        strmap* argmap = strmap_new();
        strmap_setf(argmap, "ARG0=%s", app_exe);
        strmap_setf(argmap, "ARGS=%d", 1);

        strmap* envmap = strmap_new();
        strmap_setf(envmap, "ENV0=MV2_PMI_ADDR=%s", ep_name);
        strmap_setf(envmap, "ENVS=%d", 1);

        /* launch child process */
        temp_launch(NULL, s->params, app_dir, app_exe, argmap, envmap);

        strmap_delete(&envmap);
        strmap_delete(&argmap);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* execute PMI exchange */
    if (use_pmi) {
        pmi_exchange(s, params, ep);
    }
    if (use_ring) {
        ring_exchange(s, params, ep);
    }

    /* close PMI channels */
    if (!rank) { tid = begin_delta("close init endpoint"); }
    signal_from_root(s);
    if (use_pmi || use_ring) {
      if (use_fifo) {
        spawn_net_close(&ep);
      }
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    return;
}

/* given the name of a command, search for it in path,
 * and insert full path in strmap */
static void find_command(strmap* map, const char* cmd)
{
    char* path = path_search(cmd);
    if (path != NULL) {
        strmap_set(map, cmd, path);
    } else {
        strmap_set(map, cmd, cmd);
    }
    spawn_free(&path);
    return;
}

struct session_t *
session_init (int argc, char * argv[])
{
    struct session_t * s = SPAWN_MALLOC(sizeof(struct session_t));

    /* intialize session fields */
    s->spawn_exe    = NULL;
    s->spawn_parent = NULL;
    s->spawn_id     = NULL;
    s->ep_name      = NULL;
    s->ep           = SPAWN_NET_ENDPOINT_NULL;
    s->tree         = NULL;
    s->params       = NULL;

    /* initialize tree */
    s->tree = tree_new();

    /* create empty params strmap */
    s->params = strmap_new();

    /* TODO: perhaps move this if this operation is expensive */
    /* open our endpoint */
    s->ep = spawn_net_open(SPAWN_NET_TYPE_TCP);
    s->ep_name = spawn_net_name(s->ep);

    /* get our executable name */
    s->spawn_exe = SPAWN_STRDUP(argv[0]);

    char* value;

    /* check whether we have a parent */
    if ((value = getenv("MV2_SPAWN_PARENT")) != NULL) {
        /* we have a parent, record its address */
        s->spawn_parent = SPAWN_STRDUP(value);
    } else {
        /* no parent, we are the root, create parameters strmap */

        /* we include ourself as a host,
         * plus all hosts listed on command line */
        int hosts = argc;
        strmap_setf(s->params, "N=%d", hosts);
        
        /* list our own hostname as the first host */
        char* hostname = spawn_hostname();
        strmap_setf(s->params, "%d=%s", 0, hostname);
        spawn_free(&hostname);

        /* then copy in each host from the command line */
        int i;
        for (i = 1; i < argc; i++) {
            strmap_setf(s->params, "%d=%s", i, argv[i]);
        }

        /* TODO: read this in from command line or via some other method */
        /* specify degree of tree */
        if ((value = getenv("MV2_SPAWN_DEGREE")) != NULL) {
            int degree = atoi(value);
            strmap_setf(s->params, "DEG=%d", degree);
        } else {
            strmap_setf(s->params, "DEG=%d", 2);
        }
        /* TODO: check that degree is >= 2 */

        /* record the shell command (rsh or ssh) to start remote procs */
        if ((value = getenv("MV2_SPAWN_SH")) != NULL) {
            strmap_setf(s->params, "SH=%s", value);
        } else {
            strmap_setf(s->params, "SH=rsh");
        }
        value = strmap_get(s->params, "SH");
        if (strcmp(value, "ssh") != 0 &&
            strcmp(value, "rsh") != 0)
        {
            SPAWN_ERR("MV2_SPAWN_SH must be either \"ssh\" or \"rsh\"");
            _exit(EXIT_FAILURE);
        }

        /* detect whether we should use direct exec vs sh wrapper to
         * start local procs */
        value = getenv("MV2_SPAWN_LOCAL");
        if (value != NULL) {
            strmap_set(s->params, "LOCAL", value);
        } else {
            strmap_set(s->params, "LOCAL", "sh");
        }
        value = strmap_get(s->params, "LOCAL");
        if (strcmp(value, "sh")     != 0 &&
            strcmp(value, "direct") != 0)
        {
            SPAWN_ERR("MV2_SPAWN_LOCAL must be either \"sh\" or \"direct\"");
            _exit(EXIT_FAILURE);
        }

        /* search for following commands in advance if path search
         * optimization is enabled: ssh, rsh, sh, env */
        find_command(s->params, "ssh");
        find_command(s->params, "rsh");
        find_command(s->params, "sh");
        find_command(s->params, "env");

        printf("Spawn parameters map:\n");
        strmap_print(s->params);
        printf("\n");
    }

    /* get our name */
    if ((value = getenv("MV2_SPAWN_ID")) != NULL) {
        /* we have a parent, record its address */
        s->spawn_id = SPAWN_STRDUP(value);
    }

    return s;
}

int
session_start (struct session_t * s)
{ 
    int i, tid, tid_tree;
    int nodeid;

#if 0
    if (node_initialize()) {
        session_destroy(s);
        return -1;
    }

    call_node_finalize = 1;
#endif

    if (start_event_handler()) {
        session_destroy(s);
        return -1;
    }

    call_stop_event_handler = 1;

    /**********************
     * Create spawn tree
     **********************/

    tid_tree = begin_delta("unfurl tree");

    tid = begin_delta("connect back to parent");

    /* get pointer to spawn tree data structure */
    spawn_tree* t = s->tree;

    /* if we have a parent, connect back to him */
    if (s->spawn_parent != NULL) {
        /* connect to parent */
        t->parent_ch = spawn_net_connect(s->spawn_parent);

        /* send our id */
        spawn_net_write_str(t->parent_ch, s->spawn_id);

        /* read parameters */
        spawn_net_read_strmap(t->parent_ch, s->params);
    }

    end_delta(tid);

    /* identify our children */
    int children = 0;
    const char* hosts = strmap_get(s->params, "N");
    if (hosts != NULL) {
        /* get degree of tree */
        int degree = 2;
        const char* value = strmap_get(s->params, "DEG");
        if (value != NULL) {
            degree = atoi(value);
        }

        /* get our rank, we currently use our id as a rank */
        int rank = 0;
        if (s->spawn_id != NULL) {
            rank = atoi(s->spawn_id);
        }
        nodeid = rank;

        /* get number of ranks in tree */
        int ranks = atoi(hosts);

        /* create the tree and get number of children */
        if (!nodeid) { tid = begin_delta("tree_create_kary"); }
        tree_create_kary(rank, ranks, degree, t);
        if (!nodeid) { end_delta(tid); }

        children = t->children;
    }

    /* get the current working directory */
    char* spawn_cwd = spawn_getcwd();

    /* we'll map global id to local child id */
    strmap* childmap = strmap_new();

    /* launch children */
    if (!nodeid) { tid = begin_delta("launch children"); }
    for (i = 0; i < children; i++) {
        /* get rank of child */
        int child_rank = t->child_ranks[i];

        /* add entry to global-to-local id map */
        strmap_setf(childmap, "%d=%d", child_rank, i);

        /* lookup hostname of child from parameters */
        const char* host = strmap_getf(s->params, "%d", child_rank);
        if (host == NULL) {
            spawn_free(&spawn_cwd);
            session_destroy(s);
            return -1;
        }

        /* create structure for this child */
#if 0
        int node_id = node_get_id(host);
        if (node_id < 0) {
            spawn_free(&spawn_cwd);
            session_destroy(s);
            return -1;
        }
#endif

        strmap* argmap = strmap_new();
        strmap_setf(argmap, "ARG0=%s", s->spawn_exe);
        strmap_setf(argmap, "ARGS=%d", 1);

        strmap* envmap = strmap_new();
        strmap_setf(envmap, "ENV0=MV2_SPAWN_PARENT=%s", s->ep_name);
        strmap_setf(envmap, "ENV1=MV2_SPAWN_ID=%d", child_rank);
        strmap_setf(envmap, "ENVS=%d", 2);

        /* launch child process */
        temp_launch(host, s->params, spawn_cwd, s->spawn_exe, argmap, envmap);

        strmap_delete(&envmap);
        strmap_delete(&argmap);
    }
    if (!nodeid) { end_delta(tid); }
    spawn_free(&spawn_cwd);

    /*
     * This for loop will be in another thread to overlap and speed up the
     * startup.  This loop also will cause a hang if any nodes do not launch
     * and connect back properly.
     */
    if (!nodeid) { tid = begin_delta("accept child and send params"); }
    for (i = 0; i < children; i++) {
        /* TODO: once we determine which child we're accepting,
         * save pointer to connection in tree structure */
        /* accept child connection */
        spawn_net_channel* ch = spawn_net_accept(s->ep);

        /* read global id from child */
        char* str = spawn_net_read_str(ch);

        /* lookup local id from global id */
        const char* value = strmap_get(childmap, str);
        if (value == NULL) {
        }
        int index = atoi(value);

        spawn_free(&str);

        /* record channel for child */
        t->child_chs[index] = ch;

        /* send parameters to child */
        spawn_net_write_strmap(ch, s->params);
    }
    if (!nodeid) { end_delta(tid); }

    /* delete child global-to-local id map */
    strmap_delete(&childmap);

    /* signal root to let it know tree is done */
    signal_to_root(s);
    if (!nodeid) { end_delta(tid_tree); }

    /**********************
     * Gather endpoints of all spawns
     * (unnecessary, but interesting to measure anyway)
     **********************/

    /* wait for signal from root before we start spawn ep exchange */
    if (!nodeid) { tid = begin_delta("spawn endpoint exchange"); }
    signal_from_root(s);

    /* add our endpoint into strmap and do an allgather */
    strmap* spawnep_strmap = strmap_new();
    strmap_setf(spawnep_strmap, "%d=%s", s->tree->rank, s->ep_name);
    allgather_strmap(spawnep_strmap, s->tree);

    /* print map from rank 0 */
    if (nodeid == 0) {
        printf("Spawn endpoints map:\n");
        strmap_print(spawnep_strmap);
        printf("\n");
    }

    /* signal root to let it know spawn ep bcast has completed */
    signal_to_root(s);
    if (!nodeid) { end_delta(tid); }

    /* measure pack/unpack cost of strmap */
    if (nodeid == 0) {
        if (!nodeid) { tid = begin_delta("pack/unpack strmap x1000"); }
        for (i = 0; i < 1000; i++) {
            size_t pack_size = strmap_pack_size(spawnep_strmap);
            void* pack_buf = SPAWN_MALLOC(pack_size);

            strmap_pack(pack_buf, spawnep_strmap);

            strmap* tmpmap = strmap_new();
            strmap_unpack(pack_buf, tmpmap);
            strmap_delete(&tmpmap);

            spawn_free(&pack_buf);
        }
        if (!nodeid) { end_delta(tid); }
    }

    strmap_delete(&spawnep_strmap);

    /* measure cost of signal propagation */
    signal_from_root(s);
    if (!nodeid) { tid = begin_delta("signal costs x1000"); }
    for (i = 0; i < 1000; i++) {
        signal_to_root(s);
        signal_from_root(s);
    }
    if (!nodeid) { end_delta(tid); }

    /**********************
     * Create app procs
     **********************/

    /* create map to set/receive app parameters */
    strmap* appmap = strmap_new();

    /* for now, have the root fill in the parameters */
    if (s->spawn_parent == NULL) {
        /* set executable path */
        char* value = getenv("MV2_SPAWN_EXE");
        if (value != NULL) {
            strmap_set(appmap, "EXE", value);
        } else {
            strmap_set(appmap, "EXE", "/bin/hostname");
        }

        /* set current working directory */
        char* appcwd = spawn_getcwd();
        strmap_set(appmap, "CWD", appcwd);
        spawn_free(&appcwd);

        /* set number of procs each spawn should start */
        value = getenv("MV2_SPAWN_PPN");
        if (value != NULL) {
            strmap_set(appmap, "PPN", value);
        } else {
            strmap_set(appmap, "PPN", "1");
        }

        /* detect whether we should run PMI */
        value = getenv("MV2_SPAWN_PMI");
        if (value != NULL) {
            strmap_set(appmap, "PMI", value);
        } else {
            strmap_set(appmap, "PMI", "0");
        }

        /* detect whether we should run RING exchange */
        value = getenv("MV2_SPAWN_RING");
        if (value != NULL) {
            strmap_set(appmap, "RING", value);
        } else {
            strmap_set(appmap, "RING", "0");
        }

        /* detect whether we should use FIFO on node exchange */
        value = getenv("MV2_SPAWN_FIFO");
        if (value != NULL) {
            strmap_set(appmap, "FIFO", value);
        } else {
            strmap_set(appmap, "FIFO", "0");
        }

        /* print map for debugging */
        printf("Application parameters map:\n");
        strmap_print(appmap);
        printf("\n");
    }

    /* broadcast parameters to start app procs */
    if (!nodeid) { tid = begin_delta("broadcast app params"); }
    bcast_strmap(appmap, s->tree);
    signal_to_root(s);
    if (!nodeid) { end_delta(tid); }

    app_start(s, appmap);

    strmap_delete(&appmap);

    /**********************
     * Tear down
     **********************/
 
    /*
     * This is a busy wait.  I plan on using the state machine from mpirun_rsh
     * in the future which will save cpu with pthread_cond_signal and friends
     */
    /* wait for signal from root before we start to shut down */
    if (!nodeid) { tid = begin_delta("wait for completion"); }
    signal_from_root(s);
    while (children > get_num_exited());
    if (!nodeid) { end_delta(tid); }

    return 0;
}

void
session_destroy (struct session_t * s)
{
    spawn_free(&(s->spawn_id));
    spawn_free(&(s->spawn_parent));
    spawn_free(&(s->spawn_exe));
    spawn_net_close(&(s->ep));
    strmap_delete(&(s->params));
    tree_delete(&(s->tree));

    spawn_free(&s);

    if (call_stop_event_handler) {
        stop_event_handler();
    }

    if (call_node_finalize) {
        node_finalize();
    }
}
