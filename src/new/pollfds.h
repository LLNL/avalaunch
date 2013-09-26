#ifndef POLLFDS_H
#define POLLFDS_H 1

struct pollfds_param {
    int fd;
    void (* fd_handler)(size_t, int);
};

extern int pollfds_add (size_t id, struct pollfds_param stdin_param, struct
        pollfds_param stdout_param, struct pollfds_param stderr_param);
extern void pollfds_poll (void);
extern void pollfds_process (void);

#endif
