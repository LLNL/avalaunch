#include <time.h>
#include <stdio.h>
#include <mpiimpl.h>
#include <search.h>

#define max_timestamps 200

static struct delta_t {
    struct timespec begin;
    struct timespec end;
    const char * label;
    int shift;
} timestamp_deltas[max_timestamps];

static size_t current_timestamp_delta = 0;
static size_t current_timestamp_shift = 0;

static struct timestamp_t {
    struct timespec ts;
    const char * label;
} timestamps[max_timestamps];

static size_t current_timestamp = 0;
static double delta[max_timestamps];

static double delta_avg[max_timestamps];
static double delta_min[max_timestamps];
static double delta_max[max_timestamps];

static size_t delta_index[max_timestamps];
static size_t delta_shift[max_timestamps];

int
begin_delta (const char * label)
{
    if (!(current_timestamp_delta < max_timestamps)) {
        return -1;
    }

    clock_gettime(CLOCK_MONOTONIC_RAW,
            &timestamp_deltas[current_timestamp_delta].begin);
    timestamp_deltas[current_timestamp_delta].label = label;
    timestamp_deltas[current_timestamp_delta].shift = current_timestamp_shift++;
    return current_timestamp_delta++;
}

void
end_delta (int delta_id)
{
    clock_gettime(CLOCK_MONOTONIC_RAW, &timestamp_deltas[delta_id].end);
    current_timestamp_shift--;
}

void
print_deltas (FILE * fd)
{
    size_t counter;

    fprintf(fd, "\nMPI Library Timing Info\n%40s%15s\n", "", "Time (sec)");

    for (counter = 0; counter < current_timestamp_delta; counter++) {
        struct timespec temp;

        temp.tv_nsec = timestamp_deltas[counter].end.tv_nsec -
            timestamp_deltas[counter].begin.tv_nsec;
        temp.tv_sec = timestamp_deltas[counter].end.tv_sec -
            timestamp_deltas[counter].begin.tv_sec;

        if (0 > temp.tv_nsec) {
            temp.tv_nsec += 1e9;
            temp.tv_sec -= 1;
        }

        fprintf(fd, "%*s%-*s %4lld.%.9lld\n",
                timestamp_deltas[counter].shift,
                "",
                35 - timestamp_deltas[counter].shift,
                timestamp_deltas[counter].label,
                (long long)temp.tv_sec,
                (long long)temp.tv_nsec);
    }

    fprintf(fd, "\n");

    return 0;
}

int
take_timestamp (const char * label)
{
    if (current_timestamp < max_timestamps) {
        int clock_error = clock_gettime(CLOCK_MONOTONIC_RAW,
                &timestamps[current_timestamp].ts);
        timestamps[current_timestamp].label = label;

        if (!clock_error) {
            current_timestamp++;
        }

        return clock_error;
    }

    else {
        return -2;
    }
}

static int
reduce_timestamps (MPID_Comm * comm_ptr, int pg_rank, int pg_size)
{
    int errflag = 0;

    if (current_timestamp < 1) {
        return;
    }

    MPIR_Reduce((void *)delta, (void *)delta_avg, current_timestamp,
            MPI_DOUBLE, MPI_SUM, 0, comm_ptr, &errflag);

    MPIR_Reduce((void *)delta, (void *)delta_min, current_timestamp,
            MPI_DOUBLE, MPI_MIN, 0, comm_ptr, &errflag);

    MPIR_Reduce((void *)delta, (void *)delta_max, current_timestamp,
            MPI_DOUBLE, MPI_MAX, 0, comm_ptr, &errflag);

    if (0 == pg_rank) {
        size_t counter;

        for (counter = 1; counter < current_timestamp; counter++) {
            delta_avg[counter - 1] /= pg_size;
        }
    }

    return errflag;
}

static int
process_timestamps (void)
{
    unsigned i, shift = 0;

    if (!hcreate(current_timestamp * 1.25)) {
        return -1;
    }

    for (i = 0; i < current_timestamp; i++) {
        ENTRY e, * ep;

        e.key = timestamps[i].label;
        e.data = (void *)i;

        if (NULL != (ep = hsearch(e, FIND))) {
            if (delta_index[(unsigned)ep->data]) {
                /*
                 * Only allow one pair for timing.  Maybe there should be a
                 * stack so that multiple pairs can be timed (maybe for timing
                 * within loops?)
                 */
                continue;
            }

            shift = delta_shift[(unsigned)ep->data];
            delta_index[(unsigned)ep->data] = i;
        }

        else {
            delta_shift[i] = shift++;
            delta_index[i] = 0;

            if (NULL == hsearch(e, ENTER)) {
                return -1;
            };
        }
    }

    return 0;
}

int
print_timestamps (FILE * fd)
{
    MPID_Comm * comm_ptr = NULL;
    size_t counter = 1;
    int pg_rank = 0;

    PMPI_Comm_rank(MPI_COMM_WORLD, &pg_rank);
    MPID_Comm_get_ptr(MPI_COMM_WORLD, comm_ptr);

    if (process_timestamps()) {
        return -1;
    }

    for (counter = 0; counter < current_timestamp; counter++) {
        struct timespec temp;

        temp.tv_nsec = timestamps[delta_index[counter]].ts.tv_nsec -
            timestamps[counter].ts.tv_nsec;
        temp.tv_sec = timestamps[delta_index[counter]].ts.tv_sec -
            timestamps[counter].ts.tv_sec;

        if (0 > temp.tv_nsec) {
            temp.tv_nsec += 1e9;
            temp.tv_sec -= 1;
        }

        delta[counter] = temp.tv_sec * 1e9 + temp.tv_nsec;
    }

    if (reduce_timestamps(comm_ptr, pg_rank, comm_ptr->local_size)) {
        return -1;
    }

    if (0 == pg_rank) {
        fprintf(fd, "\nMPI Library Timing Info\n%35s%15s%15s%15s\n",
                "", "MIN", "AVG", "MAX");

        for (counter = 0; counter < current_timestamp; counter++) {
            if (0 == delta_index[counter]) continue;

            fprintf(fd, "%*s%-*s %4lld.%.9lld %4lld.%.9lld %4lld.%.9lld\n",
                    delta_shift[counter], "", 35 -
                    delta_shift[counter], timestamps[counter].label,
                    ((long long)delta_min[counter]) / (long long)1e9,
                    ((long long)delta_min[counter]) % (long long)1e9,
                    ((long long)delta_avg[counter]) / (long long)1e9,
                    ((long long)delta_avg[counter]) % (long long)1e9,
                    ((long long)delta_max[counter]) / (long long)1e9,
                    ((long long)delta_max[counter]) % (long long)1e9);
        }

        fprintf(fd, "\n");
    }

    hdestroy();

    return 0;
}
