/*
 * Local headers
 */
#include <spawn_internal.h>
#include <node.h>
#include <print_errmsg.h>
#include <spawn_util.h>

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
};

static int call_stop_event_handler = 0;
static int call_node_finalize = 0;
static int call_is_local_ipaddr_db_free = 0;

void session_destroy (struct session_t *);

struct session_t *
session_init (int argc, char * argv[])
{
    struct session_t * s = SPAWN_MALLOC(sizeof(struct session_t));
    char * spawn_cwd = NULL, * spawn_command = NULL;
    size_t len = 128;

    spawn_net_open(SPAWN_NET_TYPE_TCP, &(s->ep));
    s->ep_name = spawn_net_name(&(s->ep));

    while (!spawn_cwd) {
        spawn_cwd = SPAWN_MALLOC(len);
        if (NULL == getcwd(spawn_cwd, len)) {
            switch (errno) {
                case ERANGE:
                    spawn_free(&spawn_cwd);
                    len <<= 1;

                    if (len < 128) {
                        session_destroy(s);
                        return NULL;
                    }
                    break;
                default:
                    SPAWN_ERR("node_initialize (getcwd() errno=%d %s)", errno, strerror(errno));
                    break;
            }
        }

    }

    spawn_command = SPAWN_STRDUPF("cd %s && env %s=%s %s",
            spawn_cwd, "MV2_SPAWN_PARENT", s->ep_name, argv[0]);

    is_local_ipaddr_db_init();
    call_is_local_ipaddr_db_free = 1;

    if (start_event_handler()) {
        session_destroy(s);
        return NULL;
    }

    call_stop_event_handler = 1;

    if (node_initialize(spawn_command)) {
        session_destroy(s);
        return NULL;
    }

    call_node_finalize = 1;

    if (s->parent_name = getenv("MV2_SPAWN_PARENT")) {
        int param_size, str_len;
        struct utsname buf;

        uname(&buf);

        str_len = strlen(buf.nodename) + 1;
        s->is_root = 0;

        /*
         * do not allow hierarchical startup at the moment
         */
        s->num_children = 0;
        spawn_net_connect(s->parent_name, &(s->parent_ch));

        /*
         * just for kicks
         *
         * TODO: Connect to parent to obtain param_strmap
         * spawn_net_read(param_size);
         * param_buf = allocate memory(param_size);
         * spawn_net_read(param_buf);
         * strmap_unpack(param_buf, param_strmap);
         */
        spawn_net_write(&(s->parent_ch), &str_len, sizeof(int));
        spawn_net_write(&(s->parent_ch), buf.nodename, (size_t)str_len);
    }

    else {
        int i, n;
        s->is_root = 1;

        for (i = 1, n = 0; i < argc; i++) {
            if (0 > node_get_id(argv[i])) {
                session_destroy(s);
                return NULL;
            }
        }

        /*
         * TODO: create strmap and pack into buffer
         * create param_strmap
         * pack param_strmap into param_data buffer
         */
    }

    return s;
}

int
session_start (struct session_t * s)
{
    int i, n;

    for (i = 0, n = node_count(); i < n; i++) {
        node_launch(i);
    }

    /*
     * This for loop will be in another thread to overlap and speed up the
     * startup.  This loop also will cause a hang if any nodes do not launch
     * and connect back properly.
     */
    for (i = 0; i < n; i++) {
        char str[100];
        int str_len;

        spawn_net_accept(&(s->ep), &(s->ch));

        /*
         * just for kicks
         */
        spawn_net_read(&(s->ch), &str_len, sizeof(int));
        spawn_net_read(&(s->ch), str, (size_t)str_len);
        printf("received %s\n", str);
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
    spawn_free(&s);

    if (call_stop_event_handler) {
        stop_event_handler();
    }

    if (call_node_finalize) {
        node_finalize();
    }

    if (call_is_local_ipaddr_db_free) {
        is_local_ipaddr_db_free();
    }
}
