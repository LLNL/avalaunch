#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>

#include <print_errmsg.h>
#include <pollfds.h>

static sigset_t thread_sigmask;

static pthread_t thread_id;
static unsigned num_exited = 0;

static volatile sig_atomic_t got_SIGCHLD = 0;

static void
child_sig_handler (int sig)
{
    got_SIGCHLD = 1;
}

extern int
get_num_exited (void)
{
    return num_exited;
}

static void
event_handler (void * arg)
{
    int error, signal, status;
    struct sigaction sa;

    sa.sa_flags = 0;
    sa.sa_handler = child_sig_handler;
    sigemptyset(&sa.sa_mask);

    if (-1 == sigaction(SIGCHLD, &sa, NULL)) {
        print_errmsg("event_handler (sigaction)", error);
        /*
         * Replace abort with state change once state machine is
         * reintroduced.
         */
        abort();
    }

    for (;;) {
        pollfds_poll();
        pollfds_process();

        if (got_SIGCHLD) {
            got_SIGCHLD = 0;

            if (-1 == wait(&status)) {
                print_errmsg("event_handler (wait)", errno);
                abort();
            }

            else {
                printf("child exited (status = %d)\n", status);
                num_exited++;
            }
        }
    }
}

extern int
start_event_handler (void)
{
    int error;

    sigemptyset(&thread_sigmask);
    sigaddset(&thread_sigmask, SIGCHLD);

    /*
     * Block all signals handled by signal thread in calling thread.
     */
    error = pthread_sigmask(SIG_BLOCK, &thread_sigmask, NULL);
    if (error) {
        print_errmsg("event_handler (pthread_sigmask)", error);
        return -1;
    }

    error = pthread_create(&thread_id, NULL, &event_handler, NULL);
    if (error) {
        print_errmsg("event_handler (pthread_create)", error);
        return -1;
    }

    return 0;
}

extern void
stop_event_handler (void)
{
    extern pthread_t thread_id;
    void *return_value;

    pthread_cancel(thread_id);
    pthread_join(thread_id, &return_value);
}
