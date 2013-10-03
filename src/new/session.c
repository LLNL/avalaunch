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
    int is_root;
    int num_children;
    char const * ep_name;
    char const * parent_name;
    spawn_net_endpoint ep;
    spawn_net_channel ch;
    spawn_net_channel parent_ch;
    strmap* hosts;
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

struct session_t *
session_init (int argc, char * argv[])
{
    struct session_t * s = SPAWN_MALLOC(sizeof(struct session_t));

    s->is_root      = 0;
    s->num_children = 0;
    s->parent_name  = NULL;
    s->ep_name      = NULL;
    s->hosts        = NULL;

    s->hosts = strmap_new();

    spawn_net_open(SPAWN_NET_TYPE_TCP, &(s->ep));
    s->ep_name = spawn_net_name(&(s->ep));

    char* spawn_cwd = spawn_getcwd();

    char* spawn_command = SPAWN_STRDUPF("cd %s && env %s=%s %s",
            spawn_cwd, "MV2_SPAWN_PARENT", s->ep_name, argv[0]);

    if (node_initialize(spawn_command)) {
        session_destroy(s);
        return NULL;
    }

    call_node_finalize = 1;

    char* value;
    if ((value = getenv("MV2_SPAWN_PARENT")) != NULL) {
        s->parent_name = SPAWN_STRDUP(value);
    } else {
        int i, n;
        s->is_root = 1;

        strmap_setf(s->hosts, "N=%d", (argc - 1));
        for (i = 1, n = 0; i < argc; i++) {
            strmap_setf(s->hosts, "%d=%s", i, argv[i]);

            if (0 > node_get_id(argv[i])) {
                session_destroy(s);
                return NULL;
            }
        }
    }

    return s;
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

int
session_start (struct session_t * s)
{
    int i;

    if (start_event_handler()) {
        session_destroy(s);
        return -1;
    }

    call_stop_event_handler = 1;

    /* if we have a parent, connect back to him */
    if (s->parent_name != NULL) {
        /* connect to parent */
        spawn_net_connect(s->parent_name, &(s->parent_ch));

        /* send our hostname */
        int param_size;
        char* myhost = spawn_hostname();
        spawn_net_write_str(&(s->parent_ch), myhost);
        spawn_free(&myhost);

        /* read host list */
        spawn_net_read_strmap(&(s->parent_ch), s->hosts);

        strmap_print(s->hosts);
    }

    /* launch children */
    int n = node_count();
    for (i = 0; i < n; i++) {
        node_launch(i);
    }

    /*
     * This for loop will be in another thread to overlap and speed up the
     * startup.  This loop also will cause a hang if any nodes do not launch
     * and connect back properly.
     */
    for (i = 0; i < n; i++) {
        /* accept child connection */
        spawn_net_accept(&(s->ep), &(s->ch));

        /* read hostname from child */
        char* str = spawn_net_read_str(&(s->ch));
        printf("received %s\n", str);
        spawn_free(&str);

        /* send hostlist to child */
        spawn_net_write_strmap(&(s->ch), s->hosts);
    }

    /*
     * This is a busy wait.  I plan on using the state machine from mpirun_rsh
     * in the future which will save cpu with pthread_cond_signal and friends
     */
    while (n > get_num_exited());

    return 0;
}

void
session_destroy (struct session_t * s)
{
    spawn_free(&(s->parent_name));
    strmap_delete(&s->hosts);
    spawn_free(&s);

    if (call_stop_event_handler) {
        stop_event_handler();
    }

    if (call_node_finalize) {
        node_finalize();
    }
}
