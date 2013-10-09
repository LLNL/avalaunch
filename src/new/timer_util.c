#include <time.h>
#include <stdio.h>
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

    fprintf(fd, "\nLauncher Profiling\n%40s%15s\n", "", "Time (sec)");

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
