#ifndef SPAWN_TIMER_H
#define SPAWN_TIMER_H 1

#include <stdio.h>

int take_timestamp (const char * label);
int print_timestamps (FILE * fd);

int begin_delta (const char * label);
void end_delta (int delta_id);
void print_deltas (FILE * fd);

#endif
