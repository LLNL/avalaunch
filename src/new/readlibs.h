#ifndef READLIBS_H_
#define READLIBS_H_

#include "spawn.h"

/* given a path to an executable, lookup its list of libraries
 * and record the full path to each in the given map */
int lib_capture(strmap* map, const char* file);

#endif /* READLIBS_H_ */
