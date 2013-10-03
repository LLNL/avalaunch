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

struct session_t {
    char const * spawn_exe;      /* executable path */
    char const * spawn_parent;   /* name of our parent's endpoint */
    char const * spawn_id;       /* id given to us by parent, we echo this back on connect */
    char const * ep_name;        /* name of our endpoint */
    spawn_net_channel parent_ch; /* channel to our parent (if we have one) */
    spawn_net_endpoint ep;       /* our endpoint */
    strmap* params;              /* spawn parameters sent from parent after connect */
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

static void spawn_net_write_strmap(const spawn_net_channel* ch, const strmap* map)
{
    /* allocate memory and pack strmap */
    size_t bytes = strmap_pack_size(map);
    void* buf = SPAWN_MALLOC(bytes);
    strmap_pack(buf, map);

    /* send size */
    uint64_t len = (uint64_t) bytes;
    spawn_net_write(ch, &len, sizeof(uint64_t));
  
    /* send map */
    if (bytes > 0) {
        spawn_net_write(ch, buf, bytes);
    }

    /* free buffer */
    spawn_free(&buf);
}
  
static void spawn_net_read_strmap(const spawn_net_channel* ch, strmap* map)
{
    /* read size */
    uint64_t len;
    spawn_net_read(ch, &len, sizeof(uint64_t));

    if (len > 0) {
        /* allocate buffer */
        size_t bytes = (size_t) len;
        void* buf = SPAWN_MALLOC(bytes);

        /* read data */
        spawn_net_read(ch, buf, bytes);

        /* unpack map */
        strmap_unpack(buf, map);

        /* free buffer */
        spawn_free(&buf);
    }
}

static void spawn_net_write_str(const spawn_net_channel* ch, const char* str)
{
    /* get length of string */
    size_t bytes = 0;
    if (str != NULL) {
        bytes = strlen(str) + 1;
    }

    /* TODO: pack size in network order */
    /* send the length of the string */
    uint64_t len = (uint64_t) bytes;
    spawn_net_write(ch, &len, sizeof(uint64_t));

    /* send the string */
    if (bytes > 0) {
        spawn_net_write(ch, str, bytes);
    }
}

static char* spawn_net_read_str(const spawn_net_channel* ch)
{
    /* TODO: pack size in network order */
    /* recv the length of the string */
    uint64_t len;
    spawn_net_read(ch, &len, sizeof(uint64_t));

    /* allocate space */
    size_t bytes = (size_t) len;
    char* str = SPAWN_MALLOC(bytes);

    /* recv the string */
    if (bytes > 0) {
        spawn_net_read(ch, str, bytes);
    }

    return str;
}

typedef struct spawn_tree_struct {
  int ranks;
  int rank;
  int children;
  int* child_ids;
  spawn_net_channel* child_chs;
} spawn_tree;

static create_tree_kary(int rank, int ranks, int k, spawn_tree* t)
{
    /* compute the maximum number of children this task may have */
    int max_children = k;

    /* prepare data structures to store our parent and children */
    t->ranks = ranks;
    t->rank  = rank;

    if (max_children > 0) {
        t->child_ids  = (int*) SPAWN_MALLOC(max_children * sizeof(int));
        t->child_chs  = (spawn_net_channel*) SPAWN_MALLOC(max_children * sizeof(spawn_net_channel));
    }

    /* find our parent rank and the ranks of our children */
    int i;
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

    printf("Rank %d has %d children\n", t->rank, t->children);
    for (i = 0; i < t->children; i++) {
        printf("Rank %d: Child %d of %d has rank=%d\n", t->rank, (i + 1), t->children, t->child_ids[i]);
    }
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
    //s->parent_channel = NULL_CHANNEL;
    //s->ep         = NULL_EP;
    s->params       = NULL;

    /* create empty params strmap */
    s->params = strmap_new();

    /* TODO: perhaps move this if this operation is expensive */
    /* open our endpoint */
    spawn_net_open(SPAWN_NET_TYPE_TCP, &(s->ep));
    s->ep_name = spawn_net_name(&(s->ep));

    /* get our executable name */
    s->spawn_exe = SPAWN_STRDUP(argv[0]);

    char* value;

    /* check whether we have a parent */
    if ((value = getenv("MV2_SPAWN_PARENT")) != NULL) {
        /* we have a parent, record its address */
        s->spawn_parent = SPAWN_STRDUP(value);
    } else {
        /* we are the root, build list parameters */

        /* we include ourself as a host, plus all hosts listed on command line */
        int hosts = argc;
        strmap_setf(s->params, "N=%d", hosts);
        
        /* list our hostname as the first host */
        char* hostname = spawn_hostname();
        strmap_setf(s->params, "%d=%s", 0, hostname);
        spawn_free(&hostname);

        /* then copy in each host from the command line */
        int i;
        for (i = 1; i < argc; i++) {
            strmap_setf(s->params, "%d=%s", i, argv[i]);
        }
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
    int i;

    if (node_initialize()) {
        session_destroy(s);
        return -1;
    }

    call_node_finalize = 1;

    if (start_event_handler()) {
        session_destroy(s);
        return -1;
    }

    call_stop_event_handler = 1;

    /* if we have a parent, connect back to him */
    if (s->spawn_parent != NULL) {
        /* connect to parent */
        spawn_net_connect(s->spawn_parent, &(s->parent_ch));

        /* send our id */
        spawn_net_write_str(&(s->parent_ch), s->spawn_id);

        /* read parameters */
        spawn_net_read_strmap(&(s->parent_ch), s->params);

        strmap_print(s->params);
    }

    /* identify children */
    /* for now, we have a flat tree at root */
    spawn_tree t;
    int children = 0;
    const char* hosts = strmap_get(s->params, "N");
    if (hosts != NULL) {
        /* currently using our id as a rank */
        int rank = 0;
        if (s->spawn_id != NULL) {
            rank = atoi(s->spawn_id);
        }

        int ranks = atoi(hosts);
        create_tree_kary(rank, ranks, 3, &t);
        if (s->spawn_parent == NULL) {
            children = t.children;
        }
    }

    /* launch children */
    char* spawn_cwd = spawn_getcwd();
    for (i = 0; i < children; i++) {
        /* get rank of child */
        int child_id = t.child_ids[i];

        /* lookup hostname of child from parameters */
        const char* host = strmap_getf(s->params, "%d", child_id);
        if (host == NULL) {
            spawn_free(&spawn_cwd);
            session_destroy(s);
            return -1;
        }

        /* create structure for this child */
        int node_id = node_get_id(host);
        if (node_id < 0) {
            spawn_free(&spawn_cwd);
            session_destroy(s);
            return -1;
        }

        /* launch child process */
        char* spawn_command = SPAWN_STRDUPF("cd %s && env MV2_SPAWN_PARENT=%s MV2_SPAWN_ID=%d %s",
            spawn_cwd, s->ep_name, child_id, s->spawn_exe);
        node_launch(i, spawn_command);
        spawn_free(&spawn_command);
    }
    spawn_free(&spawn_cwd);

    /*
     * This for loop will be in another thread to overlap and speed up the
     * startup.  This loop also will cause a hang if any nodes do not launch
     * and connect back properly.
     */
    for (i = 0; i < children; i++) {
        /* accept child connection */
        spawn_net_channel ch;
        spawn_net_accept(&(s->ep), &ch);

        /* read id from child */
        char* str = spawn_net_read_str(&ch);
        printf("received %s\n", str);
        spawn_free(&str);

        /* send parameters to child */
        spawn_net_write_strmap(&ch, s->params);
    }

    /*
     * This is a busy wait.  I plan on using the state machine from mpirun_rsh
     * in the future which will save cpu with pthread_cond_signal and friends
     */
    while (children > get_num_exited());

    return 0;
}

void
session_destroy (struct session_t * s)
{
    spawn_free(&(s->spawn_id));
    spawn_free(&(s->spawn_parent));
    spawn_free(&(s->spawn_exe));
    strmap_delete(&s->params);
    spawn_free(&s);

    if (call_stop_event_handler) {
        stop_event_handler();
    }

    if (call_node_finalize) {
        node_finalize();
    }
}
