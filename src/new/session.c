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
    int children;                  /* number of children we have */
    int* child_ids;                /* global ranks of our children */
    spawn_net_channel** child_chs; /* channels to children */
} spawn_tree;

struct session_t {
    char const * spawn_exe;       /* executable path */
    char const * spawn_parent;    /* name of our parent's endpoint */
    char const * spawn_id;        /* id given to us by parent, we echo this back on connect */
    char const * ep_name;         /* name of our endpoint */
    spawn_net_endpoint* ep;       /* our endpoint */
    spawn_net_channel* parent_ch; /* channel to our parent (if we have one) */
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

    t->rank      = -1;
    t->ranks     = -1;
    t->children  = 0;
    t->child_ids = NULL;
    t->child_chs = NULL;

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
    spawn_free(&(t->child_ids));
    spawn_free(&(t->child_chs));

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
        t->child_ids  = (int*) SPAWN_MALLOC(max_children * sizeof(int));
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
                t->child_ids[i] = first_child + i;
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
        SPAWN_DBG("Rank %d: Child %d of %d has rank=%d", t->rank, (i + 1), t->children, t->child_ids[i]);
    }
}

static int temp_launch(
    spawn_tree* t,
    int index,
    const char* host,
    const char* sh,
    const char* command)
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
        char* args;
        if (strcmp(sh, "rsh") == 0) {
            args = SPAWN_STRDUP(host);
        } else if (strcmp(sh, "ssh") == 0) {
            args = SPAWN_STRDUP(host);
        } else if (strcmp(sh, "sh") == 0) {
            args = SPAWN_STRDUP("-c");
        } else {
            SPAWN_ERR("unknown launch shell: `%s'", sh);
            _exit(EXIT_FAILURE);
        }

        SPAWN_DBG("Rank %d: %s %s '%s'", t->rank, sh, args, command);
        execlp(sh, sh, args, command, (char *)NULL);
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
    if (s->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_write(s->parent_ch, &signal, sizeof(char));
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
    if (s->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_read(s->parent_ch, &signal, sizeof(char));
    }

    /* forward signal to children */
    int i;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_write(ch, &signal, sizeof(char));
    }

    return;
}

static void bcast_strmap(strmap* map, const struct session_t* s)
{
    spawn_tree* t = s->tree;
    int children = t->children;

    /* read map from parent, if we have one */
    if (s->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_read_strmap(s->parent_ch, map);
    }

    /* send map to children */
    int i;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_write_strmap(ch, map);
    }

    return;
}

static void allgather_strmap(strmap* map, const struct session_t* s)
{
    spawn_tree* t = s->tree;
    int children = t->children;

    /* gather input from children if we have any */
    int i;
    for (i = 0; i < children; i++) {
        spawn_net_channel* ch = t->child_chs[i];
        spawn_net_read_strmap(ch, map);
    }

    /* forward map to parent, and wait for response */
    if (s->parent_ch != SPAWN_NET_CHANNEL_NULL) {
        spawn_net_write_strmap(s->parent_ch, map);
    }

    /* broadcast map from root to tree */
    bcast_strmap(map, s);

    return;
}

static void pmi_exchange(struct session_t* s, const strmap* params)
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

    /* wait for signal from root before we start PMI exchange */
    if (!rank) { tid = begin_delta("pmi init"); }
    signal_from_root(s);

    signal_from_root(s);
    /* wait for children to connect */
    for (i = 0; i < numprocs; i++) {
        /* wait for connect */
        chs[i] = spawn_net_accept(s->ep);

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

    /* signal root to let it know PMI init has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for signal from root before we start PMI exchange */
    if (!rank) { tid = begin_delta("pmi read children"); }
    signal_from_root(s);

    /* wait for BARRIER messages */
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];
        char* cmd = spawn_net_read_str(ch);
        //printf("spawn %d received cmd %s from child %d\n", rank, cmd, i);
        spawn_net_read_strmap(ch, pmi_strmap);
        spawn_free(&cmd);
    }

    /* signal root to let it know PMI read has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for signal from root before we start PMI exchange */
    if (!rank) { tid = begin_delta("pmi allgather"); }
    signal_from_root(s);

    /* allgather strmaps across spawn processes */
    allgather_strmap(pmi_strmap, s);

    /* signal root to let it know PMI allgather has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for signal from root before we start PMI exchange */
    if (!rank) { tid = begin_delta("pmi write children"); }
    signal_from_root(s);

    /* wait for 2 GET messages from each child */
    char cmd_barrier[] = "BARRIER";
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];

        /* send BARRIER message */
        spawn_net_write_str(ch, cmd_barrier);

        /* handle first GET request */
        char* cmd = spawn_net_read_str(ch);
        char* key = spawn_net_read_str(ch);
        char* val = strmap_get(pmi_strmap, key);
        //printf("cmd=%s key=%s val=%s\n", cmd, key, val);
        spawn_net_write_str(ch, val);
        spawn_free(&key);
        spawn_free(&cmd);

        /* handle second GET request */
        cmd = spawn_net_read_str(ch);
        key = spawn_net_read_str(ch);
        val = strmap_get(pmi_strmap, key);
        //printf("cmd=%s key=%s val=%s\n", cmd, key, val);
        spawn_net_write_str(ch, val);
        spawn_free(&key);
        spawn_free(&cmd);
    }

    /* signal root to let it know PMI write has completed */
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* wait for signal from root before we start PMI exchange */
    if (!rank) { tid = begin_delta("pmi finalize"); }
    signal_from_root(s);

    /* wait for FINALIZE */
    for (i = 0; i < numprocs; i++) {
        spawn_net_channel* ch = chs[i];
        char* cmd = spawn_net_read_str(ch);
        //printf("spawn %d received cmd %s from child %d\n", rank, cmd, i);
        spawn_net_disconnect(&chs[i]);
        spawn_free(&cmd);
    }

    /* signal root to let it know PMI finalize has completed */
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

    char host[] = "";

    /* launch app procs */
    if (!rank) { tid = begin_delta("launch app procs"); }
    signal_from_root(s);
    for (i = 0; i < numprocs; i++) {
        /* launch child process */
        char* app_command = SPAWN_STRDUPF("cd %s && env MV2_PMI_ADDR=%s %s",
            app_dir, s->ep_name, app_exe);
        temp_launch(s->tree, i, host, "sh", app_command);
        spawn_free(&app_command);
    }
    signal_to_root(s);
    if (!rank) { end_delta(tid); }

    /* check flag for whether we should initiate PMI exchange */
    const char* use_pmi_str = strmap_get(params, "PMI");
    int use_pmi = atoi(use_pmi_str);
    if (use_pmi) {
        pmi_exchange(s, params);
    }

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
    s->parent_ch    = SPAWN_NET_CHANNEL_NULL;
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

        /* record the shell command (rsh or ssh) to start remote procs */
        if ((value = getenv("MV2_SPAWN_SH")) != NULL) {
            strmap_setf(s->params, "SH=%s", value);
        } else {
            strmap_setf(s->params, "SH=rsh");
        }

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

    /* if we have a parent, connect back to him */
    if (s->spawn_parent != NULL) {
        /* connect to parent */
        s->parent_ch = spawn_net_connect(s->spawn_parent);

        /* send our id */
        spawn_net_write_str(s->parent_ch, s->spawn_id);

        /* read parameters */
        spawn_net_read_strmap(s->parent_ch, s->params);
    }

    end_delta(tid);

    /* identify our children */
    spawn_tree* t = s->tree;
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

    /* get shell command we should use to launch children */
    const char* spawn_sh = strmap_get(s->params, "SH");

    /* get the current working directory */
    char* spawn_cwd = spawn_getcwd();

    /* we'll map global id to local child id */
    strmap* childmap = strmap_new();

    /* launch children */
    if (!nodeid) { tid = begin_delta("launch children"); }
    for (i = 0; i < children; i++) {
        /* get rank of child */
        int child_id = t->child_ids[i];

        /* add entry to global-to-local id map */
        strmap_setf(childmap, "%d=%d", child_id, i);

        /* lookup hostname of child from parameters */
        const char* host = strmap_getf(s->params, "%d", child_id);
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

        /* launch child process */
        char* spawn_command = SPAWN_STRDUPF("cd %s && env MV2_SPAWN_PARENT=%s MV2_SPAWN_ID=%d %s",
            spawn_cwd, s->ep_name, child_id, s->spawn_exe);
        //node_launch(node_id, spawn_command);
        temp_launch(t, i, host, spawn_sh, spawn_command);
        spawn_free(&spawn_command);
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
    allgather_strmap(spawnep_strmap, s);

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

        /* print map for debugging */
        printf("Application parameters map:\n");
        strmap_print(appmap);
        printf("\n");
    }

    /* broadcast parameters to start app procs */
    if (!nodeid) { tid = begin_delta("broadcast app params"); }
    bcast_strmap(appmap, s);
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
    spawn_net_disconnect(&(s->parent_ch));
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
