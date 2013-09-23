#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <stdlib.h>

#include <print_errmsg.h>

struct child_s {
    pid_t   pid;
    int     stdin;
    int     stdout;
    int     stderr;
} * rootp = NULL;

static int
compare_child (struct child_s const * ptr1, struct child_s const * ptr2)
{
    if (ptr1->pid < ptr2->pid) return -1;
    if (ptr1->pid > ptr2->pid) return 1;
    return 0;
}

extern struct child_s *
create_child (char const * command[])
{
    struct child_s * ptr = malloc(sizeof(struct child_s));
    int     pipe_stdin[2], pipe_stdout[2], pipe_stderr[2];
    pid_t   cpid;

    if (!ptr) {
        print_errmsg("create_child (malloc)", errno);
        return NULL;
    }

    if (-1 == pipe(pipe_stdin)) {
        print_errmsg("create_child (pipe)", errno);
        return NULL;
    }

    if (-1 == pipe(pipe_stdout)) {
        print_errmsg("create_child (pipe)", errno);
        return NULL;
    }

    if (-1 == pipe(pipe_stderr)) {
        print_errmsg("create_child (pipe)", errno);
        return NULL;
    }

    cpid = fork();

    if (-1 == cpid) {
        print_errmsg("create_child (fork)", errno);
        return NULL;
    }

    else if (!cpid) {
        /*
         * Child
         */
        dup2(pipe_stdin[0], STDIN_FILENO);
        dup2(pipe_stdout[1], STDOUT_FILENO);
        dup2(pipe_stderr[1], STDERR_FILENO);

        close(pipe_stdin[1]);
        close(pipe_stdout[0]);
        close(pipe_stderr[0]);

        execvp(command[0], command);
        print_errmsg("create_child (execvp)", errno);
        _exit(EXIT_FAILURE);
    }

    ptr->pid    = cpid;
    ptr->stdin  = pipe_stdin[1];
    ptr->stdout = pipe_stdout[0];
    ptr->stderr = pipe_stderr[0];

    close(pipe_stdin[0]);
    close(pipe_stdout[1]);
    close(pipe_stderr[1]);

    if (NULL == tsearch(ptr, &rootp, compare_child)) {
        print_errmsg("create_child (tsearch)", errno);
        return NULL;
    }

    return ptr;
}
