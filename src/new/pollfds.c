#define _GNU_SOURCE
#include <poll.h>
#include <signal.h>
#include <stdlib.h>
#include <errno.h>

#include <pollfds.h>

static struct pollfd * fds_table = NULL;
static struct fds_info {
    size_t id;
    void (* stdin_handler)(size_t, int);
    void (* stdout_handler)(size_t, int);
    void (* stderr_handler)(size_t, int);
} * fds_info_table = NULL;

static size_t fds_index = 0;
static size_t fds_alloc = 0;

static int
enlarge_array (void)
{
    size_t new_alloc = fds_alloc ? fds_alloc << 1 : 256;

    if (new_alloc < fds_alloc) {
        return -1;
    }

    fds_table = realloc(fds_table, sizeof(struct pollfd[new_alloc * 3]));
    fds_info_table = realloc(fds_info_table, sizeof(struct fds_info[new_alloc]));

    if (!(fds_table && fds_info_table)) {
        return -1;
    }

    else {
        fds_alloc = new_alloc;
    }

    return 0;
}

extern int
pollfds_add (size_t id, struct pollfds_param stdin_param, struct pollfds_param
        stdout_param, struct pollfds_param stderr_param)
{
    if (fds_index == fds_alloc) {
        if (enlarge_array()) {
            return -1;
        }
    }

    fds_table[fds_index * 3].fd                 = stdin_param.fd;
    fds_table[fds_index * 3 + 1].fd             = stdout_param.fd;
    fds_table[fds_index * 3 + 2].fd             = stderr_param.fd;

    fds_table[fds_index * 3].events             = POLLOUT;
    fds_table[fds_index * 3 + 1].events         = POLLIN;
    fds_table[fds_index * 3 + 2].events         = POLLIN;

    fds_info_table[fds_index].id                = id;
    fds_info_table[fds_index].stdin_handler     = stdin_param.fd_handler;
    fds_info_table[fds_index].stdout_handler    = stdout_param.fd_handler;
    fds_info_table[fds_index].stderr_handler    = stderr_param.fd_handler;

    return fds_index++;
}

extern int caught_signal;

extern void
pollfds_poll (void)
{
    sigset_t empty_sigmask;
    sigemptyset(&empty_sigmask);

    if (-1 == ppoll(fds_table, fds_index, NULL, &empty_sigmask)) {
        switch (errno) {
            case EINTR:
                /* we'll get here if a signal interrupts the
                 * poll call, set this flag so event_thread
                 * knows about it */
                caught_signal = 1;
                break;
            default:
                print_errmsg("pollfds_poll (ppoll)", errno);
                abort();
                break;
        }
    }
}

extern void
pollfds_process (void)
{
    int i, n;

    for (i = 0; i < fds_index; i++) {
        void (*fd_handler[])(size_t, int) = {
            fds_info_table[i].stdin_handler,
            fds_info_table[i].stdout_handler,
            fds_info_table[i].stderr_handler,
        };

        for (n = 0; n < 3; n++) {
            switch (fds_table[i * 3 + n].revents) {
                case POLLIN:
                case POLLOUT:
                    fd_handler[n](fds_info_table[i].id, fds_table[i].fd);
                    break;
                case POLLERR:
                case POLLHUP:
                    fds_table[n].fd = -1;
                    break;
                default:
                    break;
            }
        }
    }
}
