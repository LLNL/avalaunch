/*
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-667270.
 * All rights reserved.
 * This file is part of the Avalaunch process launcher.
 * For details, see https://github.com/hpc/avalaunch
 * Please also read the LICENSE file.
*/

/*
 * Local headers
 */
#include <session.h>
#include <unistd.h>
#include <node.h>
#include <print_errmsg.h>
#include <hostfile/parser.h>

#include "spawn.h"

/*
 * System headers
 */
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/utsname.h>
#include <limits.h>
#include <getopt.h>

#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/wait.h>

#include <libgen.h>

/* needed to read library list from ELF headers */
#include "readlibs.h"

#define KEY_NET_TCP  "tcp"
#define KEY_NET_IBUD "ibud"
#define KEY_LOCAL_SHELL  "sh"
#define KEY_LOCAL_DIRECT "direct"
#define KEY_MPIR_SPAWN "spawn"
#define KEY_MPIR_APP   "app"

/*******************************
 * MPIR
 * http://www.mpi-forum.org/docs/docs.html
 ******************************/

/* since debugger process will set / read some of these variables
 * externally, we need to mark them as volatile to compiler */
#ifndef VOLATILE
#if defined(__STDC__) || defined(__cplusplus)
#define VOLATILE volatile
#else
#define VOLATILE
#endif
#endif

/* single entry for process table */
typedef struct {
    char *host_name;       /* hostname where process is running */
    char *executable_name; /* full path to executable (NUL-terminated) */
    int pid;               /* process id */
} MPIR_PROCDESC;

/* debugger sets this to 1 if process is launched under debugger
 * control */
VOLATILE int MPIR_being_debugged = 0;

/* pointer to process table, to be allocated and filled in by
 * application started process for debugger, consists of
 * MPIR_proctable_size entries of MPIR_PROCDESC structs */
MPIR_PROCDESC *MPIR_proctable = NULL;

/* number of entries in process table */
int MPIR_proctable_size = 0;

/* valid values to set MPIR_debug_state to */
#define MPIR_NULL 0
#define MPIR_DEBUG_SPAWNED 1
#define MPIR_DEBUG_ABORTING 2

/* application starter process sets this variable before calling
 * MPIR_Breakpoint to communicate with debugger */
VOLATILE int MPIR_debug_state = MPIR_NULL;

/* this variable lives in pmi.c and ring.c, the starter process
 * passes a key / environment variable to inform application
 * processes whether they should check this value before continuing
 * through "init", the debugger will set it to 1 when it attaches */
//VOLATILE int MPIR_debug_gate = 0;

/* root spawn will set this to 1 */
int MPIR_i_am_starter = 0;

/* we don't have message queues */
int MPIR_ignore_queues;

/* started process calls this routines to signal attached debugger */
void
MPIR_Breakpoint (void)
{
    return;
}

/*******************************
 * Structs and globals
 ******************************/

/* records info about the tree of spawn processes */
typedef struct spawn_tree_struct {
    int rank;                      /* our global rank (0 to ranks-1) */
    int ranks;                     /* number of nodes in tree */
    spawn_net_channel* parent_ch;  /* channel to our parent */
    int children;                  /* number of children we have */
    int* child_ranks;              /* global ranks of our children */
    spawn_net_channel** child_chs; /* channels to children */
    char** child_hosts;            /* host names where children are running */
    pid_t* child_pids;             /* pids of local processes that started children */
} spawn_tree;

typedef struct session_options_t {
    char const * hostfile;
    int error;
    int error_option;
} session_options;

/* records info for the session including a pointer to the tree of
 * spawn processes and a strmap of session parameters */
typedef struct session_t {
    char const* spawn_parent; /* name of our parent's endpoint */
    char const* spawn_id;     /* id given to us by parent, we echo this back on connect */
    char const* ep_name;      /* name of our endpoint */
    spawn_net_endpoint* ep;   /* our endpoint */
    spawn_tree* tree;         /* data structure that tracks tree info */
    strmap* params;           /* spawn parameters sent from parent after connect */
    strmap* name2group;       /* maps a group name to a process group pointer */
    strmap* pid2name;         /* maps a pid to a process group name */
    session_options options;
    strmap* appmap;           /* application exe and args extracted from command line */
} session;

/* We define a set of states to track valid PMI protocol usage for
 * each client of a process group.
 *
 * initial state: PMI_STATE_INIT
 *
 * PMI_STATE_INIT --> PMI_STATE_NORMAL when client sends PMI_INIT message
 *
 * PMI_STATE_NORMAL --> PMI_STATE_BARRIER when client sends PMI_BARRIER message
 * PMI_STATE_BARRIER --> PMI_STATE_NORMAL when send PMI_BARRIER to client
 *
 * PMI_STATE_NORMAL --> PMI_STATE_RING when client sends PMI_RING_IN message
 * PMI_STATE_RING --> PMI_STATE_NORMAL when send PMI_RING_OUT message to client
 *
 * PMI_STATE_NORMAL --> PMI_STATE_INIT when client sends PMI_FINALIZE message */

typedef enum pmi_states {
    PMI_STATE_INIT = 0,
    PMI_STATE_NORMAL,
    PMI_STATE_BARRIER,
    PMI_STATE_RING,
    PMI_STATE_FINAL,
} pmi_state;

/* records info about an application process group including
 * paramters used to start the processes, the number of processes
 * started by the owning spawn process and their pids */
typedef struct process_group_struct {
    char* name;      /* name of process group */
    strmap* params;  /* parameters specified to start process group */
    uint64_t size;   /* size of process group */
    uint64_t num;    /* number of children procs on the node */
    pid_t* pids;     /* list of children pids */
    uint64_t* ranks; /* group rank of each child */
    spawn_net_channel** chs; /* channels to children */
    strmap* file_map; /* list of files stored in ramdisk */
    uint64_t nconnected; /* hack: record number of connected children, used to assign ids */

    /* PMI-specific things */
    pmi_state* states;   /* records PMI state of each child */
    uint64_t barrier_count;  /* number of children that have sent barrier msg */
    uint64_t ring_count;     /* number of children that have sent ring input msg */
    uint64_t finalize_count; /* number of children that have sent finalize msg */
    strmap* commit_map; /* holds keys committed by children but not yet stored in global */
    strmap* global_map; /* holds committed keys after barrier */
    strmap* ring_map;   /* records data for a ring exchange */
} process_group;

/* TODO: need to map pid to process group */

static int call_stop_event_handler = 0;
static int call_node_finalize = 0;

static int copy_launcher = 0; /* set to 1 to copy launcher to /tmp while unfurling tree */

/*******************************
 * Utility routines
 ******************************/

/* allocates a string and returns current working dir,
 * caller should later free string with spawn_free */
static char *
spawn_getcwd (void)
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
static char *
spawn_hostname (void)
{
    struct utsname buf;
    int rc = uname(&buf);
    if (rc == -1) {
        SPAWN_ERR("Failed to get hostname (uname errno=%d %s)", errno, strerror(errno));
        return NULL;
    }
    char* name = SPAWN_STRDUP(buf.nodename);
    return name;
}

/* searches user's path for command executable, returns full path
 * in newly allocated string, returns NULL if not found */
static char *
spawn_path_search (const char * command)
{
    char* path = NULL;

    /* check that we got a real string for the command */
    if (command == NULL) {
        return path;
    }

    /* if we can resolve command as it is, return that */
    char pathbuf[PATH_MAX];
    char* newcmd = realpath(command, pathbuf);
    if (newcmd != NULL) {
        path = SPAWN_STRDUP(pathbuf);
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

    /* get absolute path name */
    if (path != NULL) {
        char pathbuf[PATH_MAX];
        realpath(path, pathbuf);
        spawn_free(&path);
        path = SPAWN_STRDUP(pathbuf);
    }

    /* free copy of path */
    spawn_free(&path_env_copy);

    return path;
}

/*******************************
 * Argument and environment variable routines
 ******************************/

/* return number of arguments set in map */
static int args_num(const strmap* map)
{
    int num = 0;
    const char* count = strmap_get(map, "ARGS");
    if (count != NULL) {
        num = atoi(count);
    }
    return num;
}

/* record program arguments in strmap */
static void args_capture(strmap* map, const char* argstr)
{
    /* initialize our id to current number of args */
    int num = args_num(map);

    /* make a copy of the argument string so we can manipulate it */
    char* copy = strdup(argstr);

    /* TODO: tokenize argstring by parsing quotes and such */
    char* arg = strtok(copy, " \t");

    /* add each token as a new argument */
    while (arg != NULL) {
        /* add argument */
        strmap_setf(map, "ARG%d=%s", num, arg);
        num++;

        /* get next token */
        arg = strtok(NULL, " \t");
    }

    /* record the count */
    strmap_setf(map, "ARGS=%d", num);

    /* free our copy */
    spawn_free(&copy);

    return;
}

/* append all variables in src to dst */
static void args_merge(strmap* dst, const strmap* src)
{
    /* get number of variables in each map */
    int dst_num = args_num(dst);
    int src_num = args_num(src);

    /* lookup key/value from src and store in dst*/
    int i;
    for (i = 0; i < src_num; i++) {
        const char* keyval = strmap_getf(src, "ARG%d", i);
        strmap_setf(dst, "ARG%d=%s", dst_num, keyval);
        dst_num++;
    }

    /* update number in dst */
    strmap_setf(dst, "ARGS=%d", dst_num);

    return;
}

extern char** environ;

/* return number of ENV variables set in map */
static int environ_num(const strmap* map)
{
    int num = 0;
    const char* count = strmap_get(map, "ENVS");
    if (count != NULL) {
        num = atoi(count);
    }
    return num;
}

/* capture environment in environ variable (see getenv or execl) */
static void environ_capture(strmap* map)
{
    /* initialize our id to number new variables */
    int num = environ_num(map);

    /* iterate over key=value entries in external environ variable */
    const char** ptr = environ;
    while (*ptr != NULL) {
        /* get a pointer to the key=value string */
        char* keyval = *ptr;

        /* add to envmap and increment our id */
        strmap_setf(map, "ENV%d=%s", num, keyval);
        num++;

        /* got to next key=value pair */
        ptr++;
    }

    /* record the count */
    strmap_setf(map, "ENVS=%d", num);

    return;
}

/* append all variables in src to dst */
static void environ_merge(strmap* dst, const strmap* src)
{
    /* get number of variables in each map */
    int dst_num = environ_num(dst);
    int src_num = environ_num(src);

    /* lookup key/value from src and store in dst*/
    int i;
    for (i = 0; i < src_num; i++) {
        const char* keyval = strmap_getf(src, "ENV%d", i);
        strmap_setf(dst, "ENV%d=%s", dst_num, keyval);
        dst_num++;
    }

    /* update number in dst */
    strmap_setf(dst, "ENVS=%d", dst_num);

    return;
}

/* return number of LIBS in map */
static int lib_num(strmap* map)
{
    int num = 0;
    const char* count = strmap_get(map, "LIBS");
    if (count != NULL) {
        num = atoi(count);
    }
    return num;
}

/*******************************
 * Routines that operate on spawn_trees
 ******************************/

static spawn_tree *
tree_new (void)
{
    spawn_tree* t = (spawn_tree*) SPAWN_MALLOC(sizeof(spawn_tree));

    t->rank        = -1;
    t->ranks       = -1;
    t->parent_ch   = SPAWN_NET_CHANNEL_NULL;
    t->children    = 0;
    t->child_ranks = NULL;
    t->child_chs   = NULL;
    t->child_hosts = NULL;
    t->child_pids  = NULL;

    return t;
}

static void
tree_disconnect (spawn_tree* t)
{
    if (t == NULL) {
        return;
    }

    /* free off each child channel if we have them */
    int i;
    for (i = 0; i < t->children; i++) {
        spawn_net_disconnect(&(t->child_chs[i]));
    }

    /* free connection to parent */
    spawn_net_disconnect(&(t->parent_ch));
}

static void
tree_delete (spawn_tree ** pt)
{
    if (pt == NULL) {
        return;
    }

    spawn_tree* t = *pt;

    /* free off each child channel if we have them */
    int i;
    for (i = 0; i < t->children; i++) {
        spawn_free(&(t->child_hosts[i]));
    }

    /* free child ids and channels */
    spawn_free(&(t->child_ranks));
    spawn_free(&(t->child_chs));
    spawn_free(&(t->child_hosts));
    spawn_free(&(t->child_pids));

    /* free tree structure itself */
    spawn_free(pt);
}

static void
tree_create_kary (int rank, int ranks, int k, spawn_tree* t)
{
    int i;

    /* compute the maximum number of children this task may have */
    int max_children = k;

    /* prepare data structures to store our parent and children */
    t->rank  = rank;
    t->ranks = ranks;

    if (max_children > 0) {
        t->child_ranks = (int*) SPAWN_MALLOC(max_children * sizeof(int));
        t->child_chs   = (spawn_net_channel**) SPAWN_MALLOC(max_children * sizeof(spawn_net_channel));
        t->child_hosts = (char**) SPAWN_MALLOC(max_children * sizeof(char*));
        t->child_pids  = (pid_t*) SPAWN_MALLOC(max_children * sizeof(pid_t));
    }

    for (i = 0; i < max_children; i++) {
        t->child_chs[i]   = SPAWN_NET_CHANNEL_NULL;
        t->child_hosts[i] = NULL;
        t->child_pids[i]  = -1;
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

/*******************************
 * Routines to fork/exec procs
 ******************************/

/* serialize an array of values into a newly allocated string */
static char *
serialize_to_str (const strmap * map, const char * key_count,
        const char * key_prefix)
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
static int
exec_remote (const char * host, const strmap * params, const char * cwd,
        const char * exe, const strmap * argmap, const strmap * envmap)
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
static int
exec_shell (const strmap * params, const char * cwd, const char * exe,
        const strmap * argmap, const strmap * envmap)
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
static int
exec_direct (const strmap * params, const char * cwd, const char * exe,
    const strmap * argmap, const strmap * envmap)
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
    int args = args_num(argmap);

    /* allocate memory for argv array (one extra for terminating NULL) */
    char** argv = (char**) SPAWN_MALLOC((args + 1) * sizeof(char*));

    /* fill in argv array and set last entry to NULL */
    for (i = 0; i < args; i++) {
        argv[i] = strmap_getf(argmap, "ARG%d", i);
    }
    argv[args] = (char*) NULL;

    /* determine number of environment variables */
    int envs = environ_num(envmap);

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
static pid_t
fork_proc (const char * host, const strmap * params, const char * cwd,
    const char * exe, const strmap * argmap, const strmap * envmap)
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

    if (cpid == -1) {
        /* fork failed */
        SPAWN_ERR("create_process (fork() errno=%d %s)", errno, strerror(errno));
        return cpid;
    }

    else if (cpid == 0) {
        /* fork succeeded, I'm child */

#if 0
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
                if (strcmp(local, KEY_LOCAL_SHELL) == 0) {
                    exec_shell(params, cwd, exe, argmap, envmap);
                } else if (strcmp(local, KEY_LOCAL_DIRECT) == 0) {
                    exec_direct(params, cwd, exe, argmap, envmap);
                } else {
                    SPAWN_ERR("Unknown LOCAL key value `%s'", local);
                }
            }
        } else {
            exec_remote(host, params, cwd, exe, argmap, envmap);
        }

        /* failed to exec, exit with failure code */
        SPAWN_ERR("create_child (execlp errno=%d %s)", errno, strerror(errno));
        _exit(EXIT_FAILURE);
    }

    else {
        /* fork succeeded, I'm parent */

#if 0
        //struct pollfds_param stdin_param, stdout_param, stderr_param;
        //
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

    return cpid;
}

/*******************************
 * Routines to remote copy launcher executable
 ******************************/

/* return number of files in file map */
static int pg_files_num(const process_group* pg)
{
    int count = 0;
    const char* count_str = strmap_get(pg->file_map, "N");
    if (count_str != NULL) {
        count = atoi(count_str);
    }
    return count;
}

/* return number of files in file map */
static void pg_files_append(process_group* pg, const char* file)
{
    /* get number of files in ramdisk */
    int file_count = pg_files_num(pg);

    /* append the file to the list */
    strmap_setf(pg->file_map, "%d=%s", file_count, file);
    file_count++;

    /* update the number of files */
    strmap_setf(pg->file_map, "N=%d", file_count);

    return;
}

/* given full path, return file size */
static ssize_t
get_file_size (const char * file)
{
    struct stat statbuf;
    stat(file, &statbuf);
    return (ssize_t) statbuf.st_size;
}

/* given path of the ramdisk file, delete it */
/* TODO: move this code under a top-level process cleanup function*/
static void
clear_from_ramdisk (process_group* pg)
{
    int i;

    /* delete files */
    int file_count = pg_files_num(pg);
    for (i = (file_count - 1); i >= 0; i--) {
        const char* file = strmap_getf(pg->file_map, "%d", i);
        if (file != NULL) {
            if (unlink(file)) {
                perror("Unable to cleanup tmp files");
            }
        }
    }

    /* TODO: delete directories */

    return;
}

static char TMPDIR[] = "/tmp/mpilaunch";

/* given full path of executable, copy it to memory */
static char *
write_to_ramdisk (const char* dir, const char * src, const char * buf, ssize_t size)
{
    /* create name for destination */
    char* src_copy = SPAWN_STRDUP(src);
    char* base = basename(src_copy);

    char* dst = SPAWN_STRDUPF("%s/%s", dir, base);
    spawn_free(&src_copy);

    /* open destination file for writing */
    int dstfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if (dstfd < 0) {
        SPAWN_ERR("Failed to open source file `%s' (open() errno=%d %s)", dst, errno, strerror(errno));
        spawn_free(&dst);
        return NULL;
    }

#if 0
    /* setup a hook to clear the ramdisk copies during at exit */
    if (atexit(clear_from_ramdisk)) {
        perror("atexit");
        exit(EXIT_FAILURE);
    }
#endif

    /* write block to destination */
    size_t towrite = (size_t) size;
    ssize_t nwrite = write(dstfd, buf, towrite);

    /* check for write error */
    if (nwrite != size) {
        SPAWN_ERR("Failed to write dest file `%s' (read() errno=%d %s)", dst, errno, strerror(errno));
        spawn_free(&dst);
        return NULL;
    }

    /* ensure bytes are written to disk */
    fsync(dstfd);

    /* close both files */
    close(dstfd);

    return dst;
}

/* given full path of executable, copy it to memory */
static ssize_t
read_to_mem (const char * src, ssize_t bufsize , char * buf)
{
    ssize_t nread = 0, tmpread = 0;

    /* open source file for reading */
    int srcfd = open(src, O_RDONLY);
    if (srcfd < 0) {
        SPAWN_ERR("Failed to open binary file `%s' (open() errno=%d %s)", src, errno, strerror(errno));
        return -1;
    }

    /* read from source file */
    while (nread < bufsize) {
        tmpread = read(srcfd, buf, bufsize);
        nread += tmpread;
        buf += tmpread;
    }

    /* bail out if we hit EOF */
    if (nread == 0) {
        return 0;
    }

    /* report any error */
    if (nread < 0) {
        SPAWN_ERR("Failed to read source file `%s' (read() errno=%d %s)", src, errno, strerror(errno));
        return -1;
    }

    close(srcfd);

    return nread;
}

/* given full path of executable, copy to tmp and return new name */
static char *
copy_to_tmp (const char* dir, const char * src)
{
    /* create name for destination */
    char* src_copy = SPAWN_STRDUP(src);
    char* base = basename(src_copy);
    char* dst = SPAWN_STRDUPF("%s/%s", dir, base);
    spawn_free(&src_copy);

    /* open source file for reading */
    int srcfd = open(src, O_RDONLY);
    if (srcfd < 0) {
        SPAWN_ERR("Failed to open source file `%s' (open() errno=%d %s)", src, errno, strerror(errno));
        spawn_free(&dst);
        return NULL;
    }

    /* open destination file for writing */
    int dstfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if (dstfd < 0) {
        SPAWN_ERR("Failed to open source file `%s' (open() errno=%d %s)", dst, errno, strerror(errno));
        close(srcfd);
        spawn_free(&dst);
        return NULL;
    }

    /* allocate buffer */
    size_t bufsize = 1024*1024;
    char* buf = (char*) SPAWN_MALLOC(bufsize);

    /* copy bytes from source to destination */
    while (1) {
        /* read block from source file */
        ssize_t nread = read(srcfd, buf, bufsize);

        /* bail out if we hit EOF */
        if (nread == 0) {
            break;
        }

        /* report any error */
        if (nread < 0) {
            SPAWN_ERR("Failed to read source file `%s' (read() errno=%d %s)", src, errno, strerror(errno));
            spawn_free(&dst);
            break;
        }

        /* write block to destination */
        size_t towrite = (size_t) nread;
        ssize_t nwrite = write(dstfd, buf, towrite);

        /* check for write error */
        if (nwrite != nread) {
            SPAWN_ERR("Failed to write dest file `%s' (read() errno=%d %s)", dst, errno, strerror(errno));
            spawn_free(&dst);
            break;
        }
    }

    /* free the buffer */
    spawn_free(&buf);

    /* ensure bytes are written to disk */
    fsync(dstfd);

    /* close both files */
    close(dstfd);
    close(srcfd);

    return dst;
}

/* fork process, child executes remote copy of file from local host to remote host */
static pid_t
copy_exe (const strmap * params, const char * host, const char * exepath)
{
    pid_t cpid = fork();

    if (cpid == -1) {
        SPAWN_ERR("create_process (fork() errno=%d %s)", errno, strerror(errno));
        return -1;
    } else if (cpid == 0) {
        /* we switch off SH=ssh/rsh to use scp/rcp */
        /* get name of remote shell */
        const char* shname = strmap_get(params, "SH");
        if (shname == NULL) {
            SPAWN_ERR("Failed to read name of remote shell from SH key");
            _exit(EXIT_FAILURE);
        }

        /* determine whether to use rsh or ssh */
        if (strcmp(shname, "rsh") != 0 &&
            strcmp(shname, "ssh") != 0)
        {
            SPAWN_ERR("Unknown remote shell: `%s'", shname);
            _exit(EXIT_FAILURE);
        }

        const char scp_key[] = "scp";
        const char rcp_key[] = "rcp";
        const char* key;
        if (strcmp(shname, "rsh") == 0) {
            key = rcp_key;
        } else if (strcmp(shname, "ssh") == 0) {
            key = scp_key;
        } else {
            SPAWN_ERR("Unknown remote shell: `%s'", shname);
            _exit(EXIT_FAILURE);
        }

        /* get path of remote copy command */
        const char* shpath = strmap_get(params, key);
        if (shpath == NULL) {
            SPAWN_ERR("Path to remote copy command not set");
            _exit(EXIT_FAILURE);
        }


        /* build destination file name */
        const char* dstpath = SPAWN_STRDUPF("%s:%s", host, exepath);

        /* exec process, we only return on error */
        execl(shpath, shpath, exepath, dstpath, (char*)0);
        SPAWN_ERR("Failed to exec program (execl errno=%d %s)", errno, strerror(errno));
        _exit(EXIT_FAILURE);
    }

    /* return pid to parent so it can wait on us to ensure
     * copy is complete */
    return cpid;
}

/*******************************
 * Communication over spawn tree
 ******************************/

int
get_spawn_id (session * s)
{
    if (s->spawn_id == NULL) {
        /* I am the root of the tree */
        return 0;
    }
    return atoi(s->spawn_id);
}

/* sends a synchronization signal up tree to root */
static void
signal_to_root (const session * s)
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

/* waits for synchronization signal to propagate down tree from root */
static void
signal_from_root (const session * s)
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

/* a type of reduction in which each spawn process adds its time to
 * the max time of all of its children and sends the sum to its parent,
 * an array of input values are provided along with labels to print
 * the results */
static void
print_critical_path (const session * s, int count, uint64_t * vals,
        char ** labels)
{
    spawn_tree* t = s->tree;
    int children = t->children;

    size_t bytes = count * sizeof(uint64_t);
    uint64_t* recv = (uint64_t*) SPAWN_MALLOC(bytes);
    uint64_t* max  = (uint64_t*) SPAWN_MALLOC(bytes);

    /* wait for data from all children */
    int i, j;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_read(ch, recv, bytes);

        /* compute max value across all children */
        for (j = 0; j < count; j++) {
            if (i == 0 || recv[j] > max[j]) {
                max[j] = recv[j];
            }
        }
    }

    /* add our time to max value */
    if (children > 0) {
        for (j = 0; j < count; j++) {
            max[j] += vals[j];
        }
    } else {
        for (j = 0; j < count; j++) {
            max[j] = vals[j];
        }
    }

    /* forward signal to parent */
    if (t->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_write(t->parent_ch, max, bytes);
    } else {
        /* print results if we're the root */
        for (j = 0; j < count; j++) {
            double time = (double)max[j] / 1000000000.0;
            printf("%s = %f\n", labels[j], time);
        }
    }

    spawn_free(&max);
    spawn_free(&recv);

    return;
}

static void
bcast (void * buf, size_t size, const spawn_tree * t)
{
    /* read bytes from parent, if we have one */
    spawn_net_channel* p = t->parent_ch;
    if (p != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_read(p, buf, size);
    }

    /* send bytes to children */
    int i;
    int children = t->children;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_write(ch, buf, size);
    }

    return;
}

/* broadcast string map from root to all procs in tree */
static void
bcast_strmap (strmap * map, const spawn_tree * t)
{
    /* root gets size of strmap */
    uint64_t len;
    spawn_net_channel* p = t->parent_ch;
    if (p == SPAWN_NET_CHANNEL_NULL) {
        size_t bytes = strmap_pack_size(map);
        len = (uint64_t) bytes;
    }

    /* TODO: convert to network order */
    /* broadcast size of map */
    bcast(&len, sizeof(uint64_t), t);

    /* now pack and broadcast the map */
    if (len > 0) {
        /* allocate buffer */
        size_t bufsize = (size_t) len;
        void* buf = SPAWN_MALLOC(bufsize);

        /* root packs strmap */
        if (p == SPAWN_NET_CHANNEL_NULL) {
            strmap_pack(buf, map);
        }

        /* broadcast the map */
        bcast(buf, bufsize, t);

        /* if we're not the root, unpack map into output map */
        if (p != SPAWN_NET_CHANNEL_NULL) {
            strmap_unpack(buf, map);
        }

        /* free buffer */
        spawn_free(&buf);
    }

    return;
}

/* combine strmaps as they travel up tree to root */
static void
gather_strmap (strmap * map, const spawn_tree * t)
{
    /* get number of children */
    int children = t->children;

    /* create an array to record sizes from children */
    size_t* sizes = (size_t*) SPAWN_MALLOC(children * sizeof(size_t));

    /* get size of our packed map */
    size_t bufsize = strmap_pack_size(map);

    /* add in sizes from children */
    int64_t i;
    for (i = 0; i < children; i++) {
        /* TODO: convert to network order */
        uint64_t size;
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_read(ch, &size, sizeof(uint64_t));
        sizes[i] = (size_t) size;
        bufsize += sizes[i];
    }

    /* send total size to parent */
    spawn_net_channel* p = t->parent_ch;
    if (p != SPAWN_NET_CHANNEL_NULL) {
        /* TODO: convert to network order */
        uint64_t size = (uint64_t) bufsize;
        spawn_net_write(p, &size, sizeof(uint64_t));
    }

    /* allocate buffer to hold incoming data */
    void* buf = SPAWN_MALLOC(bufsize);

    /* pack our map */
    char* ptr = (char*) buf;
    ptr += strmap_pack(buf, map);

    /* pack in map from each child */
    for (i = 0; i < children; i++) {
        /* read size from child and read map */
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_read(ch, ptr, sizes[i]);
        ptr += sizes[i];
    }

    /* forward map to parent, or unpack each map if we're the root */
    if (p != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_write(p, buf, bufsize);
    } else {
        /* we're the root, unpack each map into output map */
        ptr = (char*)buf;
        char* end = ptr + bufsize;
        while (ptr < end) {
            size_t bytes = strmap_unpack(ptr, map);
            ptr += bytes;
        }
    }

    /* free buffer */
    spawn_free(&buf);

    /* free size array */
    spawn_free(&sizes);

    return;
}

/* implement an allgather of strmap across all procs in tree */
static void
allgather_strmap (strmap * map, const spawn_tree * t)
{
    /* gather map to root */
    gather_strmap(map, t);

    /* broadcast map from root */
    bcast_strmap(map, t);

    return;
}

/* broadcast file from file system to /tmp using spawn tree,
 * returns name of file in /tmp (caller should free name) */
static char *
bcast_file (const char* dir, const char * file, const spawn_tree * t, process_group* pg)
{
    /* root spawn process reads file size */
    ssize_t bcast_size = 0;

    /* check for the chunk size to be used for BCAST_BIN */
    const char* use_bin_bcast_chunk_sz_str = strmap_get(pg->params, "BCAST_BIN_CHUNK_SZ");
    size_t use_bin_bcast_chunk_sz = atoi(use_bin_bcast_chunk_sz_str) * 1024 * 1024;

    /* get file size */
    ssize_t bufsize;
    if (t->rank == 0) {
        bufsize = get_file_size(file);
    }
    bcast(&bufsize, sizeof(ssize_t), t);

    /* TODO: read/bcast file in chunks */

    /* allocate buffer to hold file */
    void* buf = SPAWN_MALLOC(bufsize);

    /* root reads file from disk */
    if (t->rank == 0) {
        printf("bcasting %s\n", file);
        read_to_mem(file, bufsize, (char*)buf);
        /* TODO: check for errors */
    }

    /* bcast entire file at one go by default */
    if (!use_bin_bcast_chunk_sz) {
        use_bin_bcast_chunk_sz = bufsize;
    }

    /* bcast bytes from root with appropriate chunking */
    while (bcast_size < bufsize) {
//        fprintf(stdout,"Bcasted %ldMB\n", ((bcast_size) / (1024 * 1024 )));
        bcast((buf+bcast_size), use_bin_bcast_chunk_sz, t);
        bcast_size += use_bin_bcast_chunk_sz;
    }
                
    /* write file to ramdisk and get name of new file */
    char* newfile = write_to_ramdisk(dir, file, (char*)buf, bufsize);

    /* record name of ramdisk file in process group,
     * so we can delete it later */
    pg_files_append(pg, newfile);

    /* free buffer */
    spawn_free(&buf);

    return newfile;
}

/* With the ring exchange, each application process provides an address
 * as input via a string, and each gets back two strings, which are the
 * addresses provided by its left and right neighbors.  To implement
 * this we execute a double scan within the spawn net tree.  It's
 * broken into two steps.  The ring_scan function executes the scan
 * considering just the spawn processes, while the ring_exchange function
 * gathers data from and sends data to the application processes. */

/* As input we expect a strmap containing addresses of the leftmost
 * and rightmost addresses of the application procs the local spawn proc
 * launched.  The leftmost address is stored in "LEFT" and the rightmost
 * address is stored in "RIGHT".  If the spawn process did not start
 * any app procs, neither LEFT nor RIGHT should be set.
 *
 * As output, we provide a strmap that contains the addresses of procs
 * to the left and right sides that the local spawn process should link
 * to.
 *
 * A double scan operation is then executed across the spawn tree
 * to create a ring.  Spawn procs are ordered in the following way:
 *
 *   local spawn process, child1, child2, child3, ...
 *
 * where child1/2/3 are the children spawn processes the local process
 * is the parent to.
 *
 * To compute the leftmost and rightmost values to send to the parent
 * the LEFT address is set to the first LEFT value found (if any) by
 * scanning from left-to-right, and the RIGHT address is set to the
 * first RIGHT value found (if any) scanning right-to-left.  These
 * values represent the leftmost and rightmost addresses of the whole
 * subtree covered by the local spawn process and its children.
 *
 * When this reaches the root of the tree, the root creates a ring by
 * scanning the leftmost address to be the first RIGHT value found by
 * scanning right-to-left and setting the rightmost address to be the
 * first LEFT value scanning left-to-right.
 *
 * Then messages are sent back to each child in the tree.  For child i,
 * the LEFT address is set to be the RIGHT value of child i-1 and its
 * RIGHT address is set to be the LEFT value of child i+1.  For the
 * LEFT value of child 0, we use the RIGHT value of the local spawn proc. */

static void
ring_scan (strmap * input, strmap * output, const spawn_tree * t)
{
    int i;

    /* get number of children and channel to our parent */
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

/* Protocol between spawn and app proc:
 *   1) App proc connects to spawn proc
 *   2) App proc sends strmap to spawn proc containing ADDR key
 *   3) Spawn proc initializes LEFT/RIGHT strmap using ADDR values from children
 *   4) Spawn proc invokes ring_scan across spawn tree
 *   5) Spawn proc computes LEFT/RIGHT addresses for each child,
 *      sends these values along with RANK/RANKS to each child
 *   6) Spawn proc disconnects from each child */
static void
ring_exchange (session * s, const process_group * pg,
        const spawn_net_endpoint * ep)
{
    int i, tid, tid_ring;
    spawn_tree* t = s->tree;
    int rank = t->rank;

    /* wait for signal from root before we start exchange */
    if (!rank) { tid_ring = begin_delta("ring exchange"); }
    signal_from_root(s);

    /* get number of procs we should here from */
    int children = (int) pg->num;

    /* TODO: read ranks from process group */

    /* get total number of procs in job */
    int ranks = t->ranks * children;

    /* allocate a strmap for each child */
    strmap** maps = (strmap**) SPAWN_MALLOC(children * sizeof(strmap*));
    for (i = 0; i < children; i++) {
        maps[i] = strmap_new();
    }

    /* allocate a channel for each child */
    spawn_net_channel** chs = (spawn_net_channel**) SPAWN_MALLOC(children * sizeof(spawn_net_channel*));

    /* wait for children to connect */
    if (!rank) { tid = begin_delta("ring accept"); }
    signal_from_root(s);
    for (i = 0; i < children; i++) {
        chs[i] = spawn_net_accept(ep);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for address from each child */
    if (!rank) { tid = begin_delta("ring read children"); }
    signal_from_root(s);
    for (i = 0; i < children; i++) {
        //spawn_net_read_strmap(chs[i], maps[i]);

        int child_index;
        spawn_net_wait(0, NULL, children, chs, &child_index);
        spawn_net_read_strmap(chs[child_index], maps[child_index]);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }


    /* compute scan on tree */
    if (!rank) { tid = begin_delta("ring scan"); }
    signal_from_root(s);

    strmap* output = strmap_new();

    /* get addresses of our left-most and right-most children */
    strmap* input = strmap_new();
    if (children > 0) {
        const char* leftmost  = strmap_get(maps[0], "ADDR");
        const char* rightmost = strmap_get(maps[children-1], "ADDR");
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
    for (i = 0; i < children; i++) {
        /* since each spawn proc is creating the same number of tasks,
         * we can hardcode a child rank relative to the spawn rank */
        int child_rank = rank * children + i;

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
        if (i < children-1) {
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
    for (i = 0; i < children; i++) {
        spawn_net_disconnect(&chs[i]);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* delete each child strmap */
    for (i = 0; i < children; i++) {
        strmap_delete(&maps[i]);
    }

    /* signal root to let it know PMI bcast has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid_ring); }

    return;
}

/* TODO: support more general PMI usage */
/* TODO: support versioning info in case app procs use an older
 * PMI protocol */

/*******************************
 * Process groups
 ******************************/

/* allocate and initialize new process group structure */
static process_group *
process_group_new()
{
    process_group* pg = (process_group*) SPAWN_MALLOC(sizeof(process_group));
    pg->name   = NULL;
    pg->params = strmap_new();
    pg->size   = 0;
    pg->num    = 0;
    pg->pids   = NULL;
    pg->ranks  = NULL;
    pg->chs    = NULL;
    pg->nconnected = 0;
    pg->file_map   = strmap_new();
    pg->commit_map = strmap_new();
    pg->global_map = strmap_new();
    pg->ring_map   = strmap_new();
    return pg;
}

/* free resources associated with process group */
static void
process_group_delete (process_group ** ppg)
{
    if (ppg != NULL) {
        /* get pointer to process group */
        process_group* pg = *ppg;

        /* free the name of the group */
        spawn_free(&pg->name);

        /* delete paramters */
        strmap_delete(&pg->params);

        /* delete pids */
        spawn_free(&pg->pids);

        /* delete ranks */
        spawn_free(&pg->ranks);

        /* delete channels */
        spawn_free(&pg->chs);

        /* delete filemap */
        strmap_delete(&pg->file_map);

        /* delete PMI resources */
        strmap_delete(&pg->commit_map);
        strmap_delete(&pg->global_map);
        strmap_delete(&pg->ring_map);
    }

    /* free process group structure */
    spawn_free(ppg);

    return;
}

/* record mapping of group name to a pointer to its data structure,
 * some messages will contain the name of the group, and we use this
 * structure to quickly lookup the corresponding data structure */
static void
process_group_map_name (session * s, const char * name, process_group * pg)
{
    /* get reference to our name-to-structure map */
    strmap* map = s->name2group;
    if (map == NULL) {
        return;
    }

    /* record mapping of group name to process group pointer,
     * store group pointer as %p */
    void* ptr = (void*) pg;
    strmap_setf(map, "%s=%p", name, ptr);
    return;
}

/* return process group pointer from its name, returns NULL if not found */
static process_group *
process_group_by_name (session * s, const char * name)
{
    /* assume we won't find it */
    process_group* pg = NULL;

    /* get reference to our name-to-structure map */
    strmap* map = s->name2group;
    if (map == NULL) {
        return pg;
    }

    /* look up process group by its name */
    const char* pg_str = strmap_get(map, name);

    /* if we found something, decode value as void* and convert
     * to process group pointer */
    if (pg_str != NULL) {
        void* ptr;
        sscanf(pg_str, "%p", &ptr);
        pg = (process_group*) ptr;
    }

    return pg;
}

/* record mapping of pid to process group name, we'll use this
 * info in something like waitpid so that we can quickly identify
 * a process group given a pid */
static void
process_group_map_pid (session * s, process_group * pg, pid_t pid)
{
    /* get reference to our pid-to-group name map */
    strmap* map = s->pid2name;
    if (map == NULL) {
        return;
    }

    /* record mapping of pid to process group pointer,
     * store pid as unsigned long long %llu and group as pointer %p */
    const char* name = pg->name;
    unsigned long long id = (unsigned long long) pid;
    strmap_setf(map, "%llu=%s", id, name);
    return;
}

/* return process group name given a pid (member of group),
 * returns NULL if not found */
static const char *
process_group_by_pid (session * s, pid_t pid)
{
    /* get reference to our pid-to-group name map */
    strmap* map = s->pid2name;
    if (map == NULL) {
        return NULL;
    }

    /* we look up the process group by pid,
     * which we convert to unsigned long long */
    unsigned long long id  = (unsigned long long) pid;
    const char* group_name = strmap_getf(map, "%llu", id);
    return group_name;
}

/* given an input message of:
 *   MSG=PMI_INIT
 * reply with message of the form:
 *   RANK=rank, RANKS=ranks, JOBID=jobid */
static void handle_pmi_init(
    const session* s,
    process_group* pg,
    int child_id,
    const strmap* msg)
{
    /* it's an error to get a PMI_Init message if we're in
     * any state other than PMI_STATE_INIT */
    int state = (int) pg->states[child_id];
    if (state != PMI_STATE_INIT) {
        SPAWN_ERR("Recevied PMI_INIT message in invalid state=%d", state);
    }

    /* update state of child */
    pg->states[child_id] = PMI_STATE_NORMAL;

    /* get channel for this message */
    spawn_net_channel* ch = pg->chs[child_id];

    /* get rank of this process */
    int rank = (int) pg->ranks[child_id];

    /* get total number of procs in job */
    int ranks = (int) pg->size;

    /* get global jobid */
    int jobid = 0; //pg->group_id;

    /* send init info */
    strmap* map = strmap_new();
    strmap_setf(map, "RANK=%d",  rank);
    strmap_setf(map, "RANKS=%d", ranks);
    strmap_setf(map, "JOBID=%d", jobid);
    spawn_net_write_strmap(ch, map);
    strmap_delete(&map);

    return;
}

/* given an input message of:
 *   MSG=PMI_BARRIER
 *   key/values
 * merge key/values into our commit map, if we have received such a
 * message from all application procs and spawn tree children forward
 * such a message to our parent, if we are the root, send PMI_BCAST
 * message back down the tree */
static void handle_pmi_barrier(
    const session* s,
    process_group* pg,
    int child_id,
    const strmap* msg,
    int app_proc)
{
    /* get pointer to spawn tree */
    spawn_tree* t = s->tree;

    /* get channel to process that sent this message */
    spawn_net_channel* ch;
    if (app_proc) {
        /* message came from application process, check its state,
         * it's an error to get a PMI_Barrier message if we're in
         * any state other than PMI_STATE_NORMAL */
        int state = (int) pg->states[child_id];
        if (state != PMI_STATE_NORMAL) {
            SPAWN_ERR("Recevied PMI_BARRIER message in invalid state=%d", state);
        }

        /* update state of child */
        pg->states[child_id] = PMI_STATE_BARRIER;

        /* get channel for this message */
        ch = pg->chs[child_id];
    } else {
        /* message came from spawn process */
        ch = t->child_chs[child_id];
    }

    /* we should get a set of key/value pairs immediately following
     * a barrier message */
    strmap* map = strmap_new();
    spawn_net_read_strmap(ch, map);

    /* merge values from process into our commit map */
    strmap_merge(pg->commit_map, map);

    /* free the temporary map */
    strmap_delete(&map);

    /* if we have received barrier message from each app process and
     * each child process in spawn tree, forward barrier message to
     * our parent in the spawn tree */
    pg->barrier_count++;
    int total_count = pg->num + t->children;
    if (pg->barrier_count == total_count) {
        /* send to parent if we have one, otherwise send to children */
        if (t->rank > 0) {
            /* get channel to parent */
            ch = t->parent_ch;

            /* send barrier message to parent, which includes group name */
            map = strmap_new();
            strmap_set(map, "MSG", "PMI_BARRIER");
            strmap_set(map, "GROUP", pg->name);
            spawn_net_write_strmap(ch, map);
            strmap_delete(&map);

            /* send commit to parent */
            spawn_net_write_strmap(ch, pg->commit_map);
        } else {
            /* we're the root of the tree, bcast values back down,
             * build a bcast message */
            map = strmap_new();
            strmap_set(map, "MSG", "PMI_BCAST");
            strmap_set(map, "GROUP", pg->name);

            /* send bcast message followed by commit map
             * to each spawn tree child */
            int i;
            for (i = 0; i < t->children; i++) {
                 ch = t->child_chs[i];
                 spawn_net_write_strmap(ch, map);
                 spawn_net_write_strmap(ch, pg->commit_map);
            }

            /* send bcast message to each app process,
             * and set state back to PMI_STATE_NORMAL */
            for (i = 0; i < pg->num; i++) {
                 ch = pg->chs[i];
                 spawn_net_write_strmap(ch, map);
                 pg->states[i] = PMI_STATE_NORMAL;
            }

//strmap_print(pg->commit_map);
//printf("\n");

            /* merge key/values into our global map */
            strmap_merge(pg->global_map, pg->commit_map);

            /* delete bcast message */
            strmap_delete(&map);

            /* reset our barrier count */
            pg->barrier_count  = 0;
        }

        /* clear commit map */
        strmap_delete(&pg->commit_map);
        pg->commit_map = strmap_new();
    }

    return;
}

/* given an input message of:
 *   MSG=PMI_BCAST
 *   key/values
 * merge key/values into our global map, and forward same set of
 * messages to children in spawn tree and forward:
 *   MSG=PMI_BCAST
 * to application processes */
static void handle_pmi_bcast(
    const session* s,
    process_group* pg,
    int child_id,
    const strmap* msg)
{
    /* get pointer to spawn tree */
    spawn_tree* t = s->tree;

    /* get channel to parent */
    spawn_net_channel* ch = t->parent_ch;

    /* strmap of key/value pairs should follow a bcast message */
    strmap* map = strmap_new();
    spawn_net_read_strmap(ch, map);

    /* forward these messages to children */
    int i;
    for (i = 0; i < t->children; i++) {
         ch = t->child_chs[i];
         spawn_net_write_strmap(ch, msg);
         spawn_net_write_strmap(ch, map);
    }

    /* send bcast message to each app process,
     * and set state back to PMI_STATE_NORMAL */
    for (i = 0; i < pg->num; i++) {
         ch = pg->chs[i];
         spawn_net_write_strmap(ch, msg);
         pg->states[i] = PMI_STATE_NORMAL;
    }

    /* merge new key/values into our global map */
    strmap_merge(pg->global_map, map);

    /* delete the key/value map */
    strmap_delete(&map);

    /* reset our barrier count */
    pg->barrier_count  = 0;

    return;
}

/* given an input message of:
 *   MSG=PMI_GET, KEY=key
 * return:
 *   KEY=key, VAL=value
 * if that key is defined in our global map, and:
 *   KEY=key
 * otherwise */
static void handle_pmi_get(
    const session* s,
    process_group* pg,
    int child_id,
    const strmap* msg)
{
    /* it's an error to get a PMI_Get message if we're in
     * any state other than PMI_STATE_NORMAL */
    int state = (int) pg->states[child_id];
    if (state != PMI_STATE_NORMAL) {
        SPAWN_ERR("Recevied PMI_GET message in invalid state=%d", state);
    }

    /* get channel for this message */
    spawn_net_channel* ch = pg->chs[child_id];

    /* get key */
    const char* key = strmap_get(msg, "KEY");

    /* create a string map to send to child */
    strmap* map = strmap_new();

    /* lookup key in our global map */
    const char* value = strmap_get(pg->global_map, key);
    if (value != NULL) {
        strmap_set(map, "KEY", key);
        strmap_set(map, "VAL", value);
    } else {
        strmap_setf(map, "KEY", key);
    }

    /* send value to child */
    spawn_net_write_strmap(ch, map);

    /* delete map */
    strmap_delete(&map);

    return;
}

/* given an input message of:
 *   MSG=PMI_RING_OUT, LEFT=addr1, RIGHT=addr2
 * create and send messages to children */
static void handle_pmi_ring_out(
    const session* s,
    process_group* pg,
    int child_id,
    const strmap* msg)
{
    /* get pointer to spawn tree */
    spawn_tree* t = s->tree;

    /* compute number of procs we need to send to */
    int total_count = pg->num + t->children;

    /* TODO: if we have a lot of children, these double loops
     * could be costly, it may be better to allocate maps
     * for each children instead */
    /* allocate a map for each child */
    strmap** maps = (strmap**) SPAWN_MALLOC(total_count * sizeof(strmap*));

    /* initialize messages to all children */
    int i;
    for (i = 0; i < total_count; i++) {
        maps[i] = strmap_new();
        strmap_set(maps[i], "MSG", "PMI_RING_OUT");
        strmap_set(maps[i], "GROUP", pg->name);
    }

    /* TODO: use int64_t for count */
    /* get our starting count from the message */
    int count = 0;
    const char* count_str = strmap_get(msg, "COUNT");
    if (count_str != NULL) {
        count = atoi(count_str);
    } else {
        /* error */
    }

    /* iterate over all maps and set count and left neighbor */
    const char* left = strmap_get(msg, "LEFT");
    for (i = 0; i < total_count; i++) {
        /* store current count in output map */
        strmap_setf(maps[i], "COUNT=%d", count);

        /* if this map has a count, add it to our running total */
        const char* count_str = strmap_getf(pg->ring_map, "COUNT%d", i);
        if (count_str != NULL) {
            count += atoi(count_str);
        }

        /* if left is set, record its value in map to this child */
        if (left != NULL) {
            strmap_set(maps[i], "LEFT", left);
        }

        /* get right address from child, if it exists,
         * it will be the left neighbor of the next child,
         * otherwise, reuse the current left value */
        const char* next = strmap_getf(pg->ring_map, "RIGHT%d", i);
        if (next != NULL) {
            left = next;
        }
    }

    /* now set all right values (iterate backwards through children) */
    const char* right = strmap_get(msg, "RIGHT");
    for (i = (total_count - 1); i >= 0; i--) {
        /* if right is set, record its value in map to this child */
        if (right != NULL) {
            strmap_set(maps[i], "RIGHT", right);
        }

        /* get left address from child, if it exists,
         * it will be the right neighbor of the next child,
         * otherwise, reuse the current right value */
        const char* next = strmap_getf(pg->ring_map, "LEFT%d", i);
        if (next != NULL) {
            right = next;
        }
    }

    /* send messages to children in spawn tree,
     * we do this first to get the message down
     * the tree quickly */
    for (i = 0; i < t->children; i++) {
        /* get channel to spawn child */
        spawn_net_channel* ch = t->child_chs[i];

        /* send the map */
        int ring_id = pg->num + i;
        spawn_net_write_strmap(ch, maps[ring_id]);
    }

    /* now send messages to children app procs,
     * and set their state back to normal */
    for (i = 0; i < pg->num; i++) {
        spawn_net_channel* ch = pg->chs[i];
        spawn_net_write_strmap(ch, maps[i]);
        pg->states[i] = PMI_STATE_NORMAL;
    }

    /* delete maps */
    for (i = 0; i < total_count; i++) {
        strmap_delete(&maps[i]);
    }
    spawn_free(&maps);

    /* reset our ring count */
    pg->ring_count = 0;

    /* clear the ring map */
    strmap_delete(&pg->ring_map);
    pg->ring_map = strmap_new();

    return;
}

/* given an input message of:
 *   MSG=PMI_RING_IN, LEFT=addr1, RIGHT=addr2
 * wait for all such messages from all children
 * then send summary LEFT/RIGHT message to parent */
static void handle_pmi_ring_in(
    const session* s,
    process_group* pg,
    int child_id,
    const strmap* msg,
    int app_proc)
{
    /* get pointer to spawn tree */
    spawn_tree* t = s->tree;

    /* get channel to process that sent this message */
    spawn_net_channel* ch;
    int ring_id;
    if (app_proc) {
        /* message came from application process, check its state,
         * it's an error to get a PMI_RING_IN message if we're in
         * any state other than PMI_STATE_NORMAL */
        int state = (int) pg->states[child_id];
        if (state != PMI_STATE_NORMAL) {
            SPAWN_ERR("Recevied PMI_BARRIER message in invalid state=%d", state);
        }

        /* update state of child */
        pg->states[child_id] = PMI_STATE_RING;

        /* get channel for this message */
        ch = pg->chs[child_id];

        /* compute ring id for this message */
        ring_id = child_id;
    } else {
        /* message came from spawn process */
        ch = t->child_chs[child_id];

        /* compute ring id for this message,
         * children in spawn tree come after app procs */
        ring_id = pg->num + child_id;
    }

    /* record left address in ring map (if it exists) */
    const char* value = strmap_get(msg, "LEFT");
    if (value != NULL) {
        strmap_setf(pg->ring_map, "LEFT%d=%s", ring_id, value);
    }

    /* record right address in ring map (if it exists) */
    value = strmap_get(msg, "RIGHT");
    if (value != NULL) {
        strmap_setf(pg->ring_map, "RIGHT%d=%s", ring_id, value);
    }

    /* record count in ring map (if it exists) */
    value = strmap_get(msg, "COUNT");
    if (value != NULL) {
        strmap_setf(pg->ring_map, "COUNT%d=%s", ring_id, value);
    }

    /* if we have received ring input message from each app process and
     * each child process in spawn tree, forward ring input message to
     * our parent in the spawn tree */
    pg->ring_count++;
    int total_count = pg->num + t->children;
    if (pg->ring_count == total_count) {
        /* lookup leftmost address from all children */
        int i;
        const char* leftmost = NULL;
        for (i = 0; i < total_count; i++) {
            leftmost = strmap_getf(pg->ring_map, "LEFT%d", i);
            if (leftmost != NULL) {
                /* found one */
                break;
            }
        }

        /* lookup rightmost address from all children */
        const char* rightmost = NULL;
        for (i = (total_count - 1); i >= 0; i--) {
            rightmost = strmap_getf(pg->ring_map, "RIGHT%d", i);
            if (rightmost != NULL) {
                /* found one */
                break;
            }
        }

        /* TODO: use int64_t for count */
        /* total our count values across all children */
        int count = 0;
        for (i = 0; i < total_count; i++) {
            const char* count_str = strmap_getf(pg->ring_map, "COUNT%d", i);
            if (count_str != NULL) {
                count += atoi(count_str);
            }
        }

        /* send to parent if we have one, otherwise create ring output
         * message and start the broadcast */
        if (t->rank > 0) {
            /* get channel to parent */
            ch = t->parent_ch;

            /* send ring input message to parent, which includes group name */
            strmap* map = strmap_new();
            strmap_set(map, "MSG", "PMI_RING_IN");
            strmap_set(map, "GROUP", pg->name);
            if (leftmost != NULL) {
                strmap_set(map, "LEFT", leftmost);
            }
            if (rightmost != NULL) {
                strmap_set(map, "RIGHT", rightmost);
            }
            strmap_setf(map, "COUNT=%d", count);
            spawn_net_write_strmap(ch, map);
            strmap_delete(&map);
        } else {
            /* we're the root of the tree, bcast values back down,
             * build a bcast message */
            strmap* map = strmap_new();
            strmap_set(map, "MSG", "PMI_RING_OUT");
            strmap_set(map, "GROUP", pg->name);

            /* at the top level, we wrap the ends to create a ring,
             * setting the rightmost process to be the left neighbor
             * of the leftmost process */
            if (rightmost != NULL) {
                strmap_set(map, "LEFT", rightmost);
            }
            if (leftmost != NULL) {
                strmap_set(map, "RIGHT", leftmost);
            }

            /* we start the top of the tree at offset 0 */
            strmap_set(map, "COUNT", "0");

            /* simulate reception of a ring output msg */
            handle_pmi_ring_out(s, pg, -1, map);

            /* delete bcast message */
            strmap_delete(&map);
        }
    }

    return;
}

/* given an input message of:
 *   MSG=PMI_FINALIZE
 * disconnect from child and clear key/value maps if all children have
 * sent such a message */
static int handle_pmi_finalize(
    const session* s,
    process_group* pg,
    int child_id,
    const strmap* msg)
{
    /* assume that we haven't got finalize messages from all children */
    int finalized = 0;

    /* it's an error to get a PMI_Get message if we're in
     * any state other than PMI_STATE_NORMAL */
    int state = (int) pg->states[child_id];
    if (state != PMI_STATE_NORMAL) {
        SPAWN_ERR("Recevied PMI_FINALIZE message in invalid state=%d", state);
    }

    /* update state of child */
    pg->states[child_id] = PMI_STATE_FINAL;

    /* check whether all children sent us a finalize request */
    pg->finalize_count++;
    if (pg->finalize_count == pg->num) {
        /* change return code to indicate that all children
         * have sent a finalize message */
        finalized = 1;

        /* we've gotten a finalize from all children,
         * reset states in case procs call PMI_Init again */
        uint64_t i;
        for (i = 0; i < pg->num; i++) {
            pg->states[i] = PMI_STATE_INIT;
        }

        /* clear our counters */
        pg->barrier_count  = 0;
        pg->ring_count     = 0;
        pg->finalize_count = 0;

        /* free off memory holding key/value pairs */
        strmap_delete(&pg->commit_map);
        strmap_delete(&pg->global_map);
        pg->commit_map = strmap_new();
        pg->global_map = strmap_new();
    }

    return finalized;
}

static int send_close_async(const session* s)
{
    /* create our CLOSE_ASYNC message */
    strmap* map = strmap_new();
    strmap_set(map, "MSG", "CLOSE_ASYNC");

    /* get pointer to spawn tree */
    spawn_tree* t = s->tree;

    /* send message to our parent if we have one */
    if (t->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_write_strmap(t->parent_ch, map);
    }

    /* send message to each child */
    int i;
    for (i = 0; i < t->children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_write_strmap(ch, map);
    }

    /* delete the message */
    strmap_delete(&map);

    return;
}

static void authenticate_connection(const spawn_net_endpoint* ep, process_group* pg, int* child_id)
{
    /* accept the connection */
    spawn_net_channel* ch = spawn_net_accept(ep);

    /* assume a child is connecting to us, and pick an id for this child */
    int idx = pg->nconnected;
    pg->nconnected++;

    pg->chs[idx] = ch;

    /* TODO: need to authenticate connection */

    /* TODO: pick process group by name from incoming message */

    /* TODO: pick child id from value of incoming message */

    /* record id of connecting child */
    *child_id = idx;

    return;
}

/* accepts connections from process group and processes
 * PMI messages */
static void
pmi_exchange(
    session * s,
    process_group * pg,
    const spawn_net_endpoint * ep)
{
    int tid, tid_pmi;

    /* get pointer to spawn tree */
    const spawn_tree* t = s->tree;

    /* get our rank within spawn tree */
    int rank = t->rank;

    /* wait for children to connect */
    signal_to_root(s);
    if (!rank) { tid_pmi = begin_delta("pmi exchange"); }
    signal_from_root(s);

    /* get number of application procs we should here from */
    uint64_t children = pg->num;

    /* allocate a state variable for each child */
    pg->states = (pmi_state*) SPAWN_MALLOC(children * sizeof(pmi_state));

    /* initailize states */
    uint64_t i;
    for (i = 0; i < children; i++) {
        pg->states[i] = PMI_STATE_INIT;
    }

    /* initialize our PMI state counters */
    pg->barrier_count  = 0;
    pg->ring_count     = 0;
    pg->finalize_count = 0;

    /* allocate a channel for each child */
    pg->chs = (spawn_net_channel**) SPAWN_MALLOC(children * sizeof(spawn_net_channel*));
    for (i = 0; i < children; i++) {
        pg->chs[i] = SPAWN_NET_CHANNEL_NULL;
    }

    /* wait for children to connect */
#if 0
    if (!rank) { tid = begin_delta("app accept"); }
    signal_from_root(s);
    for (i = 0; i < children; i++) {
        pg->chs[i] = spawn_net_accept(ep);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }
#endif

    signal_from_root(s);
    if (!rank) { tid = begin_delta("app exchange"); }

    /* allocate an arrays to hold list of channels, process group
     * pointers, and child ids for all app procs and spawn tree
     * children */
    int channels = pg->num + 1 + t->children;
    spawn_net_channel** chs = (spawn_net_channel**) SPAWN_MALLOC(channels * sizeof(spawn_net_channel*));
    process_group** pgs     = (process_group**)     SPAWN_MALLOC(channels * sizeof(process_group*));
    int* ids                = (int*)                SPAWN_MALLOC(channels * sizeof(int));

    /* record channels to app procs */
    int index = 0;
    for (i = 0; i < pg->num; i++) {
        chs[index] = pg->chs[i];
        pgs[index] = pg;
        ids[index] = i;
        index++;
    }

    /* record channel to parent (SPAWN_NET_CHANNEL_NULL for root) */
    chs[index] = t->parent_ch;
    pgs[index] = NULL;
    ids[index] = t->children; /* use a bogus number for the child id */
    index++;

    /* record channel to each child in spawn tree */
    for (i = 0; i < t->children; i++) {
        chs[index] = t->child_chs[i];
        pgs[index] = NULL;
        ids[index] = i;
        index++;
    }

    /* compute number of CLOSE_ASYNC messages we'll receive */
    int need_to_close = 1; /* count one for ourself */
    if (t->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        need_to_close++;
    }
    need_to_close += t->children;

    /* we loop until we receive all CLOSE_ASYNC messages */
    while(1) {
        /* wait for incoming message or connection request */
        spawn_net_wait(1, &ep, channels, chs, &index);

        /* bail out if we got a bad index */
        if (index < 0) {
            break;
        }

        /* grab lock */

        /* if index points to endpoint, process incoming connection request */
        if (index == 0) {
            /* TODO: acquire process group and child id from incoming message */

            /* accept the connection for this process group */
            int child_id;
            authenticate_connection(ep, pg, &child_id);

            /* initialize channel for this child */
            chs[child_id] = pg->chs[child_id];

            continue;
        }

        /* otherwise, we got a message on a channel,
         * shift index to point into chs array */
        index--;

        /* get pointer to channel */
        spawn_net_channel* ch = chs[index];

        /* read message from channel */
        strmap* msg = strmap_new();
        spawn_net_read_strmap(ch, msg);

        /* TODO: look for error condition on read */

        /* When using TCP, select can indicate a file descriptor
         * ready with an EOF (read of 0 bytes) when the remote end
         * closes its socket.  This leads us to read en empty strmap,
         * so check for that and continue if we have an empty map */

        /* get message type */
        const char* type = strmap_get(msg, "MSG");
        if (type == NULL) {
            /* if we get here, assume that we failed to read a message,
             * and in that case, assume that we really got an EOF,
             * so don't wait on this channel anymore */
            chs[index] = SPAWN_NET_CHANNEL_NULL;

            strmap_delete(&msg);
            continue;
        }

        /* determine whether this message came from an app proc or
         * a proc in the spawn tree */
        int app_proc = (pgs[index] != NULL);

        /* set the process group based on the sender */
        process_group* msg_pg = pgs[index];

        /* override group if message contains a group key */
        const char* name = strmap_get(msg, "GROUP");
        if (name != NULL) {
            /* we have a group name, now look it up */
            msg_pg = process_group_by_name(s, name);
            if (msg_pg == NULL) {
                SPAWN_ERR("Failed to find group named %s", name);
            }
        }

        /* determine id of child process that sent the message,
         * note that this id is not unique by itself, but it is
         * when combined with a particular group */
        int child_id = ids[index];

#if 0
  printf("Rank %d: Received msg: app=%d pg=%s child=%d\n", rank, app_proc, pg->name, child_id);
  strmap_print(msg);
  printf("\n");
  fflush(stdout);
#endif

        /* select function to handle message based on message type */
        if (strcmp(type, "NULL") == 0) {

        } else if (strcmp(type, "PMI_GET") == 0) {
            handle_pmi_get(s, msg_pg, child_id, msg);

        } else if (strcmp(type, "PMI_BARRIER") == 0) {
            handle_pmi_barrier(s, msg_pg, child_id, msg, app_proc);

        } else if (strcmp(type, "PMI_BCAST") == 0) {
            handle_pmi_bcast(s, msg_pg, child_id, msg);

        } else if (strcmp(type, "PMI_RING_IN") == 0) {
            handle_pmi_ring_in(s, msg_pg, child_id, msg, app_proc);

        } else if (strcmp(type, "PMI_RING_OUT") == 0) {
            handle_pmi_ring_out(s, msg_pg, child_id, msg);

        } else if (strcmp(type, "PMI_INIT") == 0) {
            handle_pmi_init(s, msg_pg, child_id, msg);

        } else if (strcmp(type, "PMI_FINALIZE") == 0) {
            int finalized = handle_pmi_finalize(s, msg_pg, child_id, msg);
            if (finalized) {
                /* we've gotten a finalize message from each app
                 * process, close down our async channels in spawn
                 * tree */
                send_close_async(s);

                /* decrement our count by one (as if we sent a message
                 * to ourself) */
                need_to_close--;
            }

            /* TODO: we could blank out channel for app proc here */

        } else if (strcmp(type, "CLOSE_ASYNC") == 0) {
            /* if we receive a close async message, blank out
             * this channel so we don't read more messages from it */
            chs[index] = SPAWN_NET_CHANNEL_NULL;

            /* decrement the count by one */
            need_to_close--;

        } else if (strcmp(type, "KILL") == 0) {
            /* TODO: kill all procs in process group */
        }

        /* delete message */
        strmap_delete(&msg);

        /* check whether we've received CLOSE_ASYNC messages from everyone */
        if (need_to_close == 0) {
            break;
        }

        /* release unlock */
    }

    /* free the channel array */
    spawn_free(&chs);
    spawn_free(&pgs);
    spawn_free(&ids);

    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* disconnect from children */
    signal_to_root(s);
    if (!rank) { tid = begin_delta("app disconnect"); }
    signal_from_root(s);
    for (i = 0; i < children; i++) {
        spawn_net_disconnect(&pg->chs[i]);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    signal_to_root(s);
    if (!rank) { end_delta(tid_pmi); }

    return;
}

/* launch app process group witih the session according to params */
static process_group*
process_group_start (session* s, strmap* params)
{
    int i, tid;

    /* get our rank and number of procs in spawn tree */
    int rank  = s->tree->rank;
    int ranks = s->tree->ranks;

    /* get a new process group structure */
    process_group* pg = process_group_new();

    /* extract name from params and record in process group struct and
     * name-to-group map in session */
    const char* pg_name = strmap_get(params, "NAME");
    pg->name = SPAWN_STRDUP(pg_name);
    process_group_map_name(s, pg_name, pg);

    /* copy application parameters */
    strmap_merge(pg->params, params);
    
    /* read executable name and number of procs */
    const char* app_exe = strmap_get(params, "EXE");
    const char* app_dir = strmap_get(params, "CWD");
    const char* app_procs_str = strmap_get(params, "PPN");
    int children = atoi(app_procs_str);

    /* store total number of processes in group */
    uint64_t group_size = children * ranks;
    pg->size = group_size;

    /* record number of procs we'll start locally,
     * and allocate space to store pid of each process */
    pg->num   = children;
    pg->pids  = (pid_t*)    SPAWN_MALLOC(children * sizeof(pid_t));
    pg->ranks = (uint64_t*) SPAWN_MALLOC(children * sizeof(uint64_t));

    /* TODO: bcast application executables/libraries here */

    /* determine whether we're being debugged */
    int mpir_app = 0;
    const char* mpir_str = strmap_get(s->params, "MPIR");
    if (mpir_str != NULL && strcmp(mpir_str, KEY_MPIR_APP) == 0) {
        mpir_app = 1;
    }

    /* check flag for whether we should initiate PMI exchange */
    const char* use_pmi_str = strmap_get(params, "PMI");
    int use_pmi = atoi(use_pmi_str);

    /* check flag for whether we should initiate RING exchange */
    const char* use_ring_str = strmap_get(params, "RING");
    int use_ring = atoi(use_ring_str);

    /* check for flag on whether we should use FIFO */
    const char* use_fifo_str = strmap_get(params, "FIFO");
    int use_fifo = atoi(use_fifo_str);

    /* check for flag on whether we should use BCAST_BIN */
    const char* use_bin_bcast_str = strmap_get(params, "BCAST_BIN");
    int use_bin_bcast = atoi(use_bin_bcast_str);

    /* check for flag on whether we should use BCAST_LIB */
    const char* use_lib_bcast_str = strmap_get(params, "BCAST_LIB");
    int use_lib_bcast = atoi(use_lib_bcast_str);

    /* get directory for bcasting files */
    const char* bcast_dir = strmap_get(params, "BCAST_DIR");
    if (bcast_dir != NULL) {
        /* TODO: move this somewhere else */
        if (mkdir(TMPDIR, S_IRWXU|S_IRGRP|S_IXGRP)) {
            if (errno != EEXIST) {
                SPAWN_ERR("Failed to create directory: `%s' (%s)", TMPDIR, strerror(errno));
                exit(EXIT_FAILURE);
            }
        }

        /* got a bcast directory, create it */
        if (mkdir(bcast_dir, S_IRWXU|S_IRGRP|S_IXGRP)) {
            if (errno != EEXIST) {
                SPAWN_ERR("Failed to create directory: `%s' (%s)", bcast_dir, strerror(errno));
                exit(EXIT_FAILURE);
            }
        }
    }

    /* create endpoint for children to connect to */
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

    /* broadcast application libraries */
    if (use_lib_bcast) {
        if (!rank) { tid = begin_delta("bcast app libs"); }
        signal_from_root(s);
        int num_libs = lib_num(params);
        for (i = 0; i < num_libs; i++) {
            const char* libname = strmap_getf(params, "LIB%d", i);
            if (libname != NULL) {
                const char* newlib = bcast_file(TMPDIR, libname, s->tree, pg);
                spawn_free(&newlib);
            }
        }
        signal_to_root(s);
        if (!rank) { end_delta(tid); }

        /* TODO: HACK: override LD_LIBRARY_PATH,
         * proper way to do this is like SPINDLE does it with LD_AUDIT lib */
        char ld_lib_path[] = "LD_LIBRARY_PATH";
        size_t ld_lib_path_len = strlen(ld_lib_path);

        /* look for LD_LIBRARY_PATH in app params, and overwrite
         * it if set */
        int found_ld_lib_path = 0;
        int j;
        int num_envs = environ_num(params);
        for (j = 0; j < num_envs; j++) {
            const char* val = strmap_getf(params, "ENV%d", j);
            if (strncmp(val, ld_lib_path, ld_lib_path_len) == 0) {
                strmap_setf(params, "ENV%d=LD_LIBRARY_PATH=%s", j, TMPDIR);
                found_ld_lib_path = 1;
            }
        }

        /* if we didn't find it, add it to the end */
        if (! found_ld_lib_path) {
            strmap_setf(params, "ENV%d=LD_LIBRARY_PATH=%s", num_envs, TMPDIR);
            num_envs++;
            strmap_setf(params, "ENVS=%d", num_envs);
        }
    }

    /* bcast application binary */
    char* bcastname = NULL;
    if (use_bin_bcast) {
        if (!rank) { tid = begin_delta("bcast app binary"); }
        signal_from_root(s);
        bcastname = bcast_file(TMPDIR, app_exe, s->tree, pg);
        signal_to_root(s);
        if (!rank) { end_delta(tid); }

        /* exec binary from /tmp */
        app_exe = bcastname;
    }

    /* launch app procs */
    if (!rank) { tid = begin_delta("launch app procs"); }
    signal_from_root(s);

    for (i = 0; i < children; i++) {
        /* create map for arguments */
        strmap* argmap = strmap_new();
        strmap_setf(argmap, "ARG0=%s", app_exe);
        strmap_setf(argmap, "ARGS=%d", 1);

        /* copy rest of application args from params */
        args_merge(argmap, params);

        /* create map for env vars */
        strmap* envmap = strmap_new();

        /* define env variable specific to this process */
        int envs = environ_num(envmap);

        /* set MV2_PMI_ADDR so child process knows how to connect to us */
        strmap_setf(envmap, "ENV%d=MV2_PMI_ADDR=%s", envs, ep_name);
        envs++;

        /* set MPIR flag if we're debugging the application */
        if (mpir_app) {
            strmap_setf(envmap, "ENV%d=MV2_MPIR=1", envs);
            envs++;
        }

#if 0
        /* TODO: HACK: override LD_LIBRARY_PATH,
         * proper way to do this is like SPINDLE does it with LD_AUDIT lib */
        if (use_lib_bcast) {
            strmap_setf(params, "ENV%d=LD_LIBRARY_PATH=%s", envs, TMPDIR);
            envs++;
        }
#endif

        /* update the number of env vars */
        strmap_setf(envmap, "ENVS=%d", envs);

        /* copy global env vars from params */
        environ_merge(envmap, params);

        /* launch child process and record pid */
        pid_t pid = fork_proc(NULL, s->params, app_dir, app_exe, argmap, envmap);
        pg->pids[i] = pid;

        /* record mapping from pid to its process group,
         * we use this to determine which group to tear down when a
         * given pid fails */
        process_group_map_pid(s, pg, pid);

        /* TODO: allow for other assignments */
        /* assign group rank to this process */
        uint64_t child_rank = rank * children + i;
        pg->ranks[i] = child_rank;

        /* free maps */
        strmap_delete(&envmap);
        strmap_delete(&argmap);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* if user wants to debug app procs, gather pids and set MPIR variables */
    if (mpir_app) {
        /* gather host, pid, exe to root spawn process for debugging */
        if (!rank) { tid = begin_delta("gather app proc info"); }
        signal_from_root(s);
        char* hostname = spawn_hostname();

        strmap* procmap = strmap_new();
        for (i = 0; i < children; i++) {
            int child_rank = rank * children + i;
            strmap_setf(procmap, "H%d=%s",  child_rank, hostname);
            strmap_setf(procmap, "P%d=%ld", child_rank, pg->pids[i]);
            strmap_setf(procmap, "E%d=%s",  child_rank, app_exe);
        }

        gather_strmap(procmap, s->tree);

        if (rank == 0) {
            printf("App proc host, pid, exe map:\n");
            strmap_print(procmap);
            printf("\n");
        }

        spawn_free(&hostname);
        signal_to_root(s);
        if (!rank) { end_delta(tid); }

        /* now we have info on root to fill in MPIR proc table */
        if (rank == 0) {
            /* allocate space for proc table */
            MPIR_proctable_size = s->tree->ranks * children;
            MPIR_proctable = (MPIR_PROCDESC*) SPAWN_MALLOC(MPIR_proctable_size * sizeof(MPIR_PROCDESC));

            /* create a strmap so we can use the same character pointer
             * since we expect common hostnames and executable paths,
             * an optimization discussed in MPIR spec for debuggers */
            strmap* strcache = strmap_new();

            /* fill in proc table */
            for (i = 0; i < MPIR_proctable_size; i++) {
                /* get pointer to proc descriptor */
                MPIR_PROCDESC* desc = &MPIR_proctable[i];

                /* fill in host name */
                const char* host_str = strmap_getf(procmap, "H%d", i);
                const char* host_str2 = strmap_get(strcache, host_str);
                if (host_str2 == NULL) {
                    strmap_set(strcache, host_str, host_str);
                    host_str2 = strmap_get(strcache, host_str);
                }
                desc->host_name = (char*) host_str2;

                /* fill in exe name */
                const char* exe_str = strmap_getf(procmap, "E%d", i);
                const char* exe_str2 = strmap_get(strcache, exe_str);
                if (exe_str2 == NULL) {
                    strmap_set(strcache, exe_str, exe_str);
                    exe_str2 = strmap_get(strcache, exe_str);
                }
                desc->executable_name = (char*) exe_str2;

                /* TODO: careful with converting pid/integer here */
                /* fill in pid */
                const char* pid_str = strmap_getf(procmap, "P%d", i);
                int pid = atoi(pid_str);
                desc->pid = pid;
            }

            /* tell debugger we're ready for it to attach */
            MPIR_debug_state = MPIR_DEBUG_SPAWNED;
            MPIR_Breakpoint();

            /* free the cache of strings */
            strmap_delete(&strcache);
        }

        strmap_delete(&procmap);

        signal_from_root(s);
    }

    /* execute PMI exchange */
    if (use_pmi) {
        pmi_exchange(s, pg, ep);
    }
    if (use_ring) {
        ring_exchange(s, pg, ep);
    }

    /* close listening channel for children */
    if (!rank) { tid = begin_delta("close init endpoint"); }
    signal_from_root(s);
    if (use_pmi || use_ring) {
      if (use_fifo) {
        spawn_net_close(&ep);
      }
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* TODO: move this to process group or session cleanup step */

    /* free temporary binary name */
    spawn_free(&bcastname);

    return pg;
}

/*******************************
 * Manage sessions
 ******************************/

/* given the name of a command, search for it in path,
 * and insert full path in strmap */
static void
find_command (strmap * map, const char * cmd)
{
    char* path = spawn_path_search(cmd);
    if (path != NULL) {
        strmap_set(map, cmd, path);
    } else {
        strmap_set(map, cmd, cmd);
    }
    spawn_free(&path);
    return;
}

static void
process_options (session* s, int argc, char * argv[])
{
    session_options so = { .hostfile = NULL, .error = 0 };

    extern char* optarg;
    extern int optind, opterr, optopt;

    optind = 1;
    opterr = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"hostfile", 1, 0, 'h'},
        {0, 0, 0, 0}
    };

    while (1) {
        int c = getopt_long(
                    argc, argv, "h:",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'h':
                so.hostfile = SPAWN_STRDUP(optarg);
                break;
            default:
                so.error = -1;
                so.error_option = optopt;
                break;
        }
    }

    /* extract non-option values from command line */
    int numitems = argc - optind;
    if (numitems > 0) {
        /* we take the first item after options to be executable */
        const char* exename = argv[optind];
        optind++;

        /* record name of executable */
        strmap_setf(s->appmap, "EXENAME=%s", exename);

        /* record all other items as args to executable */

        /* initialize our id to current number of args */
        int num = args_num(s->appmap);
    
        /* add each token as a new argument */
        int i;
        for (i = optind; i < argc; i++) {
            /* add argument */
            strmap_setf(s->appmap, "ARG%d=%s", num, argv[i]);
            num++;
        }
    
        /* record the count */
        strmap_setf(s->appmap, "ARGS=%d", num);
    }

    s->options = so;

    return;
}

session *
session_init (int argc, char * argv[])
{
    session * s = SPAWN_MALLOC(sizeof(session));

    /* intialize session fields */
    s->spawn_parent = NULL;
    s->spawn_id     = NULL;
    s->ep_name      = NULL;
    s->ep           = SPAWN_NET_ENDPOINT_NULL;
    s->tree         = NULL;
    s->params       = NULL;
    s->name2group   = NULL;
    s->pid2name     = NULL;
    s->appmap       = NULL;

    /* initialize tree */
    s->tree = tree_new();

    /* create empty params strmap */
    s->params = strmap_new();

    /* create empty name-to-process group pointer map */
    s->name2group = strmap_new();

    /* create empty pid-to-process group name map */
    s->pid2name   = strmap_new();

    /* create empty map to hold application name and arguments */
    s->appmap = strmap_new();

    const char* value;

    /* check whether we have a parent */
    if ((value = getenv("MV2_SPAWN_PARENT")) != NULL) {
        /* we have a parent, record its address */
        s->spawn_parent = SPAWN_STRDUP(value);

        /* infer net type from parent's name and open endpoint */
        spawn_net_type type = spawn_net_infer_type(s->spawn_parent);
        s->ep = spawn_net_open(type);
        s->ep_name = spawn_net_name(s->ep);
    } else {
        strmap * hostmap = strmap_new();
        process_options(s, argc, argv);

        switch (s->options.error) {
            case -1:
                SPAWN_ERR("process_options detected an unrecognized option "
                        "[-%c]", s->options.error_option);
                _exit(EXIT_FAILURE);
            case -2:
                SPAWN_ERR("process_options detected an option with missing "
                        "argument [-%c]", s->options.error_option);
                _exit(EXIT_FAILURE);
            case -3:
                SPAWN_ERR("Failed to find executable");
                _exit(EXIT_FAILURE);
            default:
                SPAWN_DBG("process_options exited successfully "
                        "[hostfile = %s]\n", s->options.hostfile);
        }

        if (s->options.hostfile) {
            read_hostfile(s->options.hostfile, hostmap);
        }

        /* no parent, we are the root, create parameters strmap */

        /* we set the MPIR key if we are launched by debugger */
        if (MPIR_being_debugged) {
            /* tell MPIR that we are the main starter process */
            MPIR_i_am_starter = 1;

            /* determine whether user wants to debug spawn tree or app */
            if ((value = getenv("MV2_SPAWN_DBG")) != NULL) {
                if (strcmp(value, KEY_MPIR_SPAWN) != 0 &&
                    strcmp(value, KEY_MPIR_APP)   != 0)
                {
                    SPAWN_ERR("MV2_SPAWN_DBG must be either \"%s\" or \"%s\"",
                        KEY_MPIR_SPAWN, KEY_MPIR_APP);
                    _exit(EXIT_FAILURE);
                }
                strmap_set(s->params, "MPIR", value);
            } else {
                /* default to debug spawn tree if not specified */
                strmap_set(s->params, "MPIR", KEY_MPIR_SPAWN);
            }
        }

        /* check whether we should remote copy the launcher exe */
        if ((value = getenv("MV2_SPAWN_COPY")) != NULL) {
            copy_launcher = atoi(value);
        }
        strmap_setf(s->params, "COPY=%d", copy_launcher);

        /* first, compute and record launch executable name */
        char* spawn_orig = argv[0];
        char* spawn_path = spawn_path_search(spawn_orig);

        if (copy_launcher) {
            /* copy launcher executable to /tmp */
            char* spawn_path_tmp = copy_to_tmp("/tmp", spawn_path);
            strmap_set(s->params, "EXE", spawn_path_tmp);
            spawn_free(&spawn_path_tmp);
        } else {
            /* run launcher directly from its current location */
            strmap_set(s->params, "EXE", spawn_path);
        }

        spawn_free(&spawn_path);

        /* TODO: move endpoint open to session_start? */
        /* determine which type of endpoint we should open */
        spawn_net_type type = SPAWN_NET_TYPE_TCP;
        if ((value = getenv("MV2_SPAWN_NET")) != NULL) {
            if (strcmp(value, KEY_NET_TCP) == 0) {
                type = SPAWN_NET_TYPE_TCP;
            } else if (strcmp(value, KEY_NET_IBUD) == 0) {
                type = SPAWN_NET_TYPE_IBUD;
            } else {
                SPAWN_ERR("MV2_SPAWN_NET must be either \"%s\" or \"%s\"",
                    KEY_NET_TCP, KEY_NET_IBUD);
                _exit(EXIT_FAILURE);
            }
        }

        /* open our endpoint */
        s->ep = spawn_net_open(type);
        s->ep_name = spawn_net_name(s->ep);

        /* then copy in each host from the command line */
        const char* ptr_string = NULL;
        strmap* entrymap = NULL;
        size_t i, n = 1;

        for (i = 0; NULL != (ptr_string = strmap_getf(hostmap, "%d", i)); i++) {
            char * hostname = NULL;
            int multiplier = 1;

            sscanf(ptr_string, "%p", &entrymap);
            hostname = strmap_get(entrymap, "hostname");

            ptr_string = strmap_get(entrymap, "multiplier");
            if (ptr_string) {
                sscanf(ptr_string, "%d", &multiplier);
            }

            while (multiplier--) {
                strmap_setf(s->params, "%d=%s", n++, hostname);
            }
        }

        /* we include ourself as a host,
         * plus all hosts listed on command line */
        int hosts = n;
        strmap_setf(s->params, "N=%d", hosts);
        
        /* list our own hostname as the first host */
        char* hostname = spawn_hostname();
        strmap_setf(s->params, "%d=%s", 0, hostname);
        spawn_free(&hostname);

        /* TODO: read this in from command line or via some other method */
        /* specify degree of tree */
        if ((value = getenv("MV2_SPAWN_DEGREE")) != NULL) {
            int degree = atoi(value);
            strmap_setf(s->params, "DEG=%d", degree);
        } else {
            strmap_setf(s->params, "DEG=%d", 2);
        }
        /* TODO: check that degree is >= 2 */

        /* record the remote shell command (rsh or ssh) to start procs */
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

        /* detect whether we should use direct exec vs shell wrapper to
         * start local procs */
        value = getenv("MV2_SPAWN_LOCAL");
        if (value != NULL) {
            strmap_set(s->params, "LOCAL", value);
        } else {
            strmap_set(s->params, "LOCAL", KEY_LOCAL_DIRECT);
        }
        value = strmap_get(s->params, "LOCAL");
        if (strcmp(value, KEY_LOCAL_SHELL)  != 0 &&
            strcmp(value, KEY_LOCAL_DIRECT) != 0)
        {
            SPAWN_ERR("MV2_SPAWN_LOCAL must be either \"%s\" or \"%s\"",
                KEY_LOCAL_SHELL, KEY_LOCAL_DIRECT);
            _exit(EXIT_FAILURE);
        }

        /* search for following commands in advance if path search
         * optimization is enabled: ssh, rsh, sh, env */
        find_command(s->params, "ssh");
        find_command(s->params, "scp");
        find_command(s->params, "rsh");
        find_command(s->params, "rcp");
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

static uint64_t
time_diff (struct timespec * end, struct timespec * start)
{
    uint64_t sec  = (uint64_t) (end->tv_sec  - start->tv_sec);
    uint64_t nsec = (uint64_t) (end->tv_nsec - start->tv_nsec);
    uint64_t total = sec * 1000000000 + nsec;
    return total;
}

int
session_start (session * s)
{ 
    int i, tid, tid_tree;

    if (start_event_handler()) {
        session_destroy(s);
        return -1;
    }

    call_stop_event_handler = 1;

    /**********************
     * Create spawn tree
     **********************/
    struct timespec t_parent_connect_start,   t_parent_connect_end;
    struct timespec t_parent_params_start,    t_parent_params_end;
    struct timespec t_copy_launcher_start,    t_copy_launcher_end;
    struct timespec t_children_launch_start,  t_children_launch_end;
    struct timespec t_children_connect_start, t_children_connect_end;
    struct timespec t_children_params_start,  t_children_params_end;

    tid_tree = begin_delta("unfurl tree");

    tid = begin_delta("connect back to parent");

    /* get pointer to spawn tree data structure */
    spawn_tree* t = s->tree;

    /* if we have a parent, connect back to him */
    if (s->spawn_parent != NULL) {
        /* connect to parent */
        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_connect_start);
        t->parent_ch = spawn_net_connect(s->spawn_parent);
        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_connect_end);

        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_params_start);

        /* read our pid */
        pid_t pid = getpid();

        /* send our id */
        strmap* idmap = strmap_new();
        strmap_set(idmap, "ID", s->spawn_id);
        strmap_setf(idmap, "PID=%ld", (long) pid);
        spawn_net_write_strmap(t->parent_ch, idmap);
        strmap_delete(&idmap);

        /* read parameters */
        spawn_net_read_strmap(t->parent_ch, s->params);
        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_params_end);
    } else {
        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_connect_start);
        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_connect_end);
        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_params_start);
        clock_gettime(CLOCK_MONOTONIC_RAW, &t_parent_params_end);
    }

    end_delta(tid);

    /* identify our children */
    int nodeid = -1;
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

    /* determine whether we should copy launcher process to /tmp */
    const char* copy_str = strmap_get(s->params, "COPY");
    copy_launcher = atoi(copy_str);

    /* lookup spawn executable name */
    const char* spawn_exe = strmap_get(s->params, "EXE");

    /* get the current working directory */
    char* spawn_cwd = spawn_getcwd();

    /* we'll map global id to local child id */
    strmap* childmap = strmap_new();

    /* rcp/scp the launcher executable to /tmp on remote hosts */
    clock_gettime(CLOCK_MONOTONIC_RAW, &t_copy_launcher_start);
    if (copy_launcher) {
        if (!nodeid) { tid = begin_delta("copy launcher exe"); }
        pid_t* pids = (pid_t*) SPAWN_MALLOC(children * sizeof(pid_t));

        for (i = 0; i < children; i++) {
            /* get rank of child */
            int child_rank = t->child_ranks[i];

            /* lookup hostname of child from parameters */
            const char* host = strmap_getf(s->params, "%d", child_rank);
            if (host == NULL) {
                spawn_free(&spawn_cwd);
                session_destroy(s);
                return -1;
            }

            /* launch child process */
            pids[i] = copy_exe(s->params, host, spawn_exe);
        }

        /* wait for all copies to complete */
        for (i = 0; i < children; i++) {
            int status;
            waitpid(pids[i], &status, 0);
        }

        spawn_free(&pids);
        if (!nodeid) { end_delta(tid); }
    }
    clock_gettime(CLOCK_MONOTONIC_RAW, &t_copy_launcher_end);

    /* launch children */
    clock_gettime(CLOCK_MONOTONIC_RAW, &t_children_launch_start);
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

        /* build map of arguments */
        strmap* argmap = strmap_new();
        strmap_setf(argmap, "ARG0=%s", spawn_exe);
        strmap_setf(argmap, "ARGS=%d", 1);

        /* build map of environment variables */
        strmap* envmap = strmap_new();
        strmap_setf(envmap, "ENV0=MV2_SPAWN_PARENT=%s", s->ep_name);
        strmap_setf(envmap, "ENV1=MV2_SPAWN_ID=%d", child_rank);
        strmap_setf(envmap, "ENVS=%d", 2);

        /* launch child process */
        pid_t pid = fork_proc(host, s->params, spawn_cwd, spawn_exe, argmap, envmap);
        t->child_hosts[i] = SPAWN_STRDUP(host);
        t->child_pids[i]  = pid;

        /* free the maps */
        strmap_delete(&envmap);
        strmap_delete(&argmap);
    }
    if (!nodeid) { end_delta(tid); }
    spawn_free(&spawn_cwd);
    clock_gettime(CLOCK_MONOTONIC_RAW, &t_children_launch_end);

    /*
     * This for loop will be in another thread to overlap and speed up the
     * startup.  This loop also will cause a hang if any nodes do not launch
     * and connect back properly.
     */

    spawn_net_channel** chs = (spawn_net_channel**) SPAWN_MALLOC(children * sizeof(spawn_net_channel*));

    clock_gettime(CLOCK_MONOTONIC_RAW, &t_children_connect_start);
    if (!nodeid) { tid = begin_delta("accept children"); }
    for (i = 0; i < children; i++) {
        /* TODO: would be good to authenticate connections as they are
         * made.  However, for now we just accept as fast as possible */
        chs[i] = spawn_net_accept(s->ep);
    }
    if (!nodeid) { end_delta(tid); }
    clock_gettime(CLOCK_MONOTONIC_RAW, &t_children_connect_end);

    clock_gettime(CLOCK_MONOTONIC_RAW, &t_children_params_start);
    if (!nodeid) { tid = begin_delta("send params to children"); }
    for (i = 0; i < children; i++) {
        /* TODO: once we determine which child we're accepting,
         * save pointer to connection in tree structure */
        /* accept child connection */
        spawn_net_channel* ch = chs[i];

        /* read strmap from child */
        strmap* idmap = strmap_new();
        spawn_net_read_strmap(ch, idmap);

        /* read global id from child */
        const char* str = strmap_get(idmap, "ID");
        if (str == NULL) {
        }

        /* lookup local id from global id */
        const char* value = strmap_get(childmap, str);
        if (value == NULL) {
        }
        int index = atoi(value);

        /* read pid of child */
        const char* pid_str = strmap_get(idmap, "PID");
        if (pid_str == NULL) {
        }

        /* free map holding child's data */
        spawn_free(&idmap);

        /* record channel for child */
        t->child_chs[index] = ch;

        /* send parameters to child */
        spawn_net_write_strmap(ch, s->params);
    }
    if (!nodeid) { end_delta(tid); }
    clock_gettime(CLOCK_MONOTONIC_RAW, &t_children_params_end);

    spawn_free(&chs);

    /* delete child global-to-local id map */
    strmap_delete(&childmap);

    /* signal root to let it know tree is done */
    signal_to_root(s);
    if (!nodeid) { end_delta(tid_tree); }

    /**********************
     * Gather pids for all spawn procs
     * (unnecessary, but interesting to measure anyway)
     **********************/

    strmap* spawnproc_strmap = strmap_new();

    /* wait for signal from root before we start to gather proc info */
    if (!nodeid) { tid = begin_delta("gather spawn pids **"); }
    signal_from_root(s);
    pid_t pid = getpid();
    strmap_setf(spawnproc_strmap, "%d=%ld", s->tree->rank, (long)pid);
    gather_strmap(spawnproc_strmap, s->tree);
    signal_to_root(s);
    if (!nodeid) { end_delta(tid); }

    if (nodeid == 0) {
        printf("Spawn pid map:\n");
        strmap_print(spawnproc_strmap);
        printf("\n");
    }

    /* at this point we can fill in MPIR proc table for spawn procs */
    const char* mpir_str = strmap_get(s->params, "MPIR");
    if (mpir_str != NULL && strcmp(mpir_str, KEY_MPIR_SPAWN) == 0) {
        if (nodeid == 0) {
            /* allocate space for proc table */
            MPIR_proctable_size = s->tree->ranks;
            MPIR_proctable = (MPIR_PROCDESC*) SPAWN_MALLOC(MPIR_proctable_size * sizeof(MPIR_PROCDESC));

            /* fill in proc table */
            for (i = 0; i < MPIR_proctable_size; i++) {
                /* get pointer to proc descriptor */
                MPIR_PROCDESC* desc = &MPIR_proctable[i];

                /* fill in host name */
                const char* host = strmap_getf(s->params, "%d", i);
                desc->host_name = (char*) host;

                /* fill in exe name */
                desc->executable_name = (char*) spawn_exe;

                /* fill in pid */
                const char* pid_str = strmap_getf(spawnproc_strmap, "%d", i);
                int pid = atoi(pid_str);
                desc->pid = pid;
            }

            /* tell debugger we're ready for it to attach */
            MPIR_debug_state = MPIR_DEBUG_SPAWNED;
            MPIR_Breakpoint();
        }

        /* hold all procs on signal from root */
        signal_from_root(s);
    }

    strmap_delete(&spawnproc_strmap);

    /**********************
     * Gather endpoints of all spawns and measure some other costs
     * (unnecessary, but interesting to measure anyway)
     **********************/

    /* wait for signal from root before we start spawn ep exchange */
    if (!nodeid) { tid = begin_delta("spawn endpoint exchange **"); }
    signal_from_root(s);

    /* add our endpoint into strmap and do an allgather */
    strmap* spawnep_strmap = strmap_new();
    strmap_setf(spawnep_strmap, "%d=%s", s->tree->rank, s->ep_name);
    allgather_strmap(spawnep_strmap, s->tree);

    /* signal root to let it know spawn ep bcast has completed */
    signal_to_root(s);
    if (!nodeid) { end_delta(tid); }

    /* print map from rank 0 */
    if (nodeid == 0) {
        printf("Spawn endpoints map:\n");
        strmap_print(spawnep_strmap);
        printf("\n");
    }

#if 1
    /* measure pack/unpack cost of strmap */
    if (nodeid == 0) {
        if (!nodeid) { tid = begin_delta("pack/unpack strmap x1000 **"); }
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
#endif

    strmap_delete(&spawnep_strmap);

#if 1
    /* measure cost of signal propagation */
    signal_from_root(s);
    if (!nodeid) { tid = begin_delta("signal costs x1000 **"); }
    for (i = 0; i < 1000; i++) {
        signal_to_root(s);
        signal_from_root(s);
    }
    if (!nodeid) { end_delta(tid); }
#endif

    /**********************
     * Create app procs
     **********************/

    /* create map to set/receive app parameters */
    strmap* appmap = strmap_new();

    /* for now, have the root fill in the parameters */
    if (s->spawn_parent == NULL) {
        /* copy values recorded in session map */
        strmap_merge(appmap, s->appmap);

        /* create a name for this process group (unique to session) */
        char group_name[] = "GROUP_0";
        strmap_set(appmap, "NAME", group_name);

        /* set current working directory */
        char* appcwd = spawn_getcwd();
        strmap_set(appmap, "CWD", appcwd);
        spawn_free(&appcwd);

        /* set number of procs each spawn should start */
        char* value = getenv("MV2_SPAWN_PPN");
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
            strmap_set(appmap, "PMI", "1");
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
        
        /* detect whether we should binary bcasting */
        value = getenv("MV2_SPAWN_BIN_CHUNK_SIZE");
        if (value != NULL) {
            strmap_set(appmap, "BCAST_BIN_CHUNK_SZ", value);
        } else {
            strmap_set(appmap, "BCAST_BIN_CHUNK_SZ", "0");
        }

        /* define bcast directory */
        char* bcast_dir = SPAWN_STRDUPF("%s/%s", TMPDIR, group_name);
        strmap_set(appmap, "BCAST_DIR", bcast_dir);

        /* detect whether we should bcast app binary */
        value = getenv("MV2_SPAWN_BCAST_BIN");
        if (value != NULL) {
            strmap_set(appmap, "BCAST_BIN", value);
        } else {
            strmap_set(appmap, "BCAST_BIN", "0");
        }

        /* detect whether we should bcast app libs */
        value = getenv("MV2_SPAWN_BCAST_LIB");
        if (value != NULL) {
            strmap_set(appmap, "BCAST_LIB", value);
        } else {
            strmap_set(appmap, "BCAST_LIB", "0");
        }

        /* read value back out of appmap and interpret it,
         * need to know whether we're bcasting libs now
         * so we can determine the full path to each below */
        const char* use_lib_bcast_str = strmap_get(appmap, "BCAST_LIB");
        int use_lib_bcast = atoi(use_lib_bcast_str);

        /* set executable path */
        value = strmap_get(s->appmap, "EXENAME");
        if (value != NULL) {
            /* do the path search once in root */
            char* app_path = spawn_path_search(value);
            strmap_set(appmap, "EXE", app_path);

            /* if we're bcasting libs,
             * lookup and set paths to libs */
            if (use_lib_bcast) {
                int lib_rc = lib_capture(appmap, app_path);

                /* TODO: disable library bcast in this case? */
                /* if we failed to find a library, print error */
                if (lib_rc != 0) {
                    SPAWN_ERR("Failed to find at least one library");
                }
            }

            spawn_free(&app_path);
        }

        /* set environment variables for app procs */
        environ_capture(appmap);

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

    /* start the application processes if we have an executable */
    const char* appexe = strmap_get(appmap, "EXE");
    if (appexe != NULL) {
        process_group_start(s, appmap);
    }

    strmap_delete(&appmap);

    /* TODO: before we can delete the process group, we need to
     * ensure all procs have exited */

    /* TODO: print times for unfurl step */
    char* labels[] = {"parent connect", "parent params", "launcher copy", "children launch", "children connect", "children params"};
    uint64_t times[6];
    times[0] = time_diff(&t_parent_connect_end,   &t_parent_connect_start);
    times[1] = time_diff(&t_parent_params_end,    &t_parent_params_start);
    times[2] = time_diff(&t_copy_launcher_end,    &t_copy_launcher_start);
    times[3] = time_diff(&t_children_launch_end,  &t_children_launch_start);
    times[4] = time_diff(&t_children_connect_end, &t_children_connect_start);
    times[5] = time_diff(&t_children_params_end,  &t_children_params_start);
    print_critical_path(s, 6, times, labels);

    return 0;
}

void
session_destroy (session * s)
{
    int tid;

    /* get pointer to spawn tree data structure,
     * and get our rank */
    spawn_tree* t = s->tree;
    int nodeid = t->rank;

    /* TODO: perhaps delete this right after starting */
    /* if we copied launcher to /tmp, delete it now */
    if (copy_launcher) {
        /* lookup spawn executable name */
        const char* spawn_exe = strmap_get(s->params, "EXE");
        unlink(spawn_exe);
    }

    /* wait until we get the go ahead from root */
    signal_from_root(s);

    /* tear down our tree connections */
    if (!nodeid) { tid = begin_delta("disconnect tree (root cost)"); }
    tree_disconnect(s->tree);
    if (!nodeid) { end_delta(tid); }

    /* close our listening endpoint */
    if (!nodeid) { tid = begin_delta("close endpoint (root cost)"); }
    spawn_net_close(&(s->ep));
    if (!nodeid) { end_delta(tid); }

    /* wait for forked procs that rsh/ssh'd child avalaunch
     * procs on to remote nodes to exit.
     *
     * This is a busy wait.  I plan on using the state machine from mpirun_rsh
     * in the future which will save cpu with pthread_cond_signal and friends
     */
    /* wait for signal from root before we start to shut down */
    if (!nodeid) { tid = begin_delta("wait for children (root cost)"); }
    int children = t->children;
    while (children > get_num_exited());
    if (!nodeid) { end_delta(tid); }

    /* free tree data structure */
    tree_delete(&(s->tree));
    spawn_free(&(s->spawn_id));
    spawn_free(&(s->spawn_parent));

    strmap_delete(&(s->params));
    strmap_delete(&(s->name2group));
    strmap_delete(&(s->pid2name));
    strmap_delete(&(s->appmap));

    spawn_free(&(s->options.hostfile));

    spawn_free(&s);

    if (call_stop_event_handler) {
        stop_event_handler();
    }

    if (call_node_finalize) {
        node_finalize();
    }
}
