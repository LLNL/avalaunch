#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>

#include <print_errmsg.h>

static sigset_t thread_sigmask;
static sigset_t original_sigmask;

static pthread_t thread_id;
static unsigned num_exited = 0;

extern int
get_num_exited (void)
{
    return num_exited;
}

static void
event_handler (void * arg)
{
    int error, signal, status;

    for (;;) {
        if ((error = sigwait(&thread_sigmask, &signal))) {
            print_errmsg("event_handler (sigwait)", error);
            /*
             * Replace abort with state change once state machine is
             * reintroduced.
             */
            abort();
        }

        switch (signal) {
            case SIGCHLD:
                if (-1 == wait(&status)) {
                    print_errmsg("event_handler (wait)", errno);
                    abort();
                }

                else {
                    printf("child exited (status = %d)\n", status);
                    num_exited++;
                }
                break;
            default:
                break;
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
