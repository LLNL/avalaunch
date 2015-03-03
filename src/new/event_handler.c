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

#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>

#include <print_errmsg.h>
#include <pollfds.h>

#include <pthread.h>

static sigset_t thread_sigmask;

static pthread_t thread_id;
static unsigned num_exited = 0;

static volatile sig_atomic_t got_SIGCHLD = 0;

static signal_count = 0;
int caught_signal;

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
        caught_signal = 0;
        pollfds_poll();
        pollfds_process();

        /* if we got a signal, it may be because a child exited,
         * or it could be the signal from pthread_cancel? */
        if (caught_signal) {
            signal_count++;
//            printf("==============We got something %d==================\n", signal_count);  fflush(stdout);
//        }

//       if (got_SIGCHLD) {
            /* to get here, we got at least one SIGCHLD */
            got_SIGCHLD = 0;

            pid_t waited_pid;
            while (0 < (waited_pid = waitpid(-1, &status, WNOHANG | WUNTRACED |
                            WCONTINUED))) {
                if (WIFEXITED(status)) {
                    int rc = WEXITSTATUS(status);

                    if (rc) {
                        printf("child exited (status = %d) [pid: %d]\n", rc,
                                waited_pid);
                    }

                    num_exited++;
                }

                else if (WIFSIGNALED(status)) {
                    num_exited++;
                    printf("child exited (signal %d) [pid: %d]\n",
                            WTERMSIG(status), waited_pid);
                }

                else if (WIFSTOPPED(status)) {
                    printf("child stopped [pid: %d]\n", waited_pid);
                }

                else if (WIFCONTINUED(status)) {
                    printf("child continued [pid: %d]\n", waited_pid);
                }
            }

            if (-1 == waited_pid) {
                switch (errno) {
                    case ECHILD:
                        break;
                    default:
                        print_errmsg("event_handler (wait)", errno);
                        abort();
                }
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
