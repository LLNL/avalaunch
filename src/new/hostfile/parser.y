/* parser for hostfile reader */

%{
/*
 * Copyright (c) 2001-2013, The Ohio State University. All rights
 * reserved.
 *
 * This file is part of the MVAPICH2 software package developed by the
 * team members of The Ohio State University's Network-Based Computing
 * Laboratory (NBCL), headed by Professor Dhabaleswar K. (DK) Panda.
 *
 * For detailed copyright and licensing information, please refer to the
 * copyright file COPYRIGHT in the top level MVAPICH2 directory.
 */

/*
 * Local headers
 */
#include <spawn_internal.h>

/*
 * System headers
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int yylex();

void yyerror (char const *s);
static int commit(void);
static void free_memory(void);
static void print_memory(void);

static strmap * hostmap = NULL;

struct entry_s {
    char const * hostname;
    char const * hca;
    int port;
    int multiplier;
} static current = {NULL, NULL, -1, 1};

static char const * hostfile = NULL;
static int lineno = 1;

%}

%union {
    size_t decimal;
    char * text;
}

%token <decimal> DECIMAL
%token <text> TEXT

%%

hostfile:   /* empty */
        |   hostfile line
;

line:   '\n'                            { lineno++; }
    |   hostname '\n'                   { lineno++; if(commit()) YYERROR; }
    |   error '\n'                      { lineno++; YYERROR; }
;

hostname:   TEXT                        { current.hostname = $1; }
        |   hostname ':' specifiers
;

specifiers: multiplier
          | multiplier ':' hca
          | hca
;

multiplier: DECIMAL                     { current.multiplier = $1; }
;

hca:    TEXT                            { current.hca = $1; }
   |    TEXT ':' DECIMAL                { current.hca = $1; current.port = $3; }
;

%%

extern FILE * hostfile_yyin;

static void
print_memory(void)
{
#ifndef DEBUG_HOSTFILE_READER
#define DEBUG_HOSTFILE_READER 0
#endif
#if DEBUG_HOSTFILE_READER
    if (hostmap) {
        char * ptr_string = NULL;
        strmap * entrymap = NULL;
        size_t i;

        strmap_print(hostmap);
        printf("\n");

        for (i = 0; NULL != (ptr_string = strmap_getf(hostmap, "%d", i)); i++) {
            sscanf(ptr_string, "%p", &entrymap);
            strmap_print(entrymap);
        }
    }
#endif
}

static int
commit(void)
{
    static int entry_number = 0;
    extern strmap * hostmap;
    strmap * entrymap = strmap_new();

    strmap_set(entrymap, "hostname", current.hostname);

    if (current.hca) {
        strmap_set(entrymap, "hca", current.hca);
    }

    if (current.multiplier > 1) {
        strmap_setf(entrymap, "multiplier=%d", current.multiplier);
    }

    if (current.port != -1) {
        strmap_setf(entrymap, "port=%d", current.port);
    }

    if (hostmap == NULL) {
        hostmap = strmap_new();
    }

    strmap_setf(hostmap, "%d=%p", entry_number++, entrymap);

    current.hostname = NULL;
    current.hca = NULL;
    current.port = -1;
    current.multiplier = 1;

    return 0;
}

extern int
read_hostfile(char const * pathname, strmap * hm)
{
    int rv, i;

    lineno = 1;

    hostfile = pathname;
    hostfile_yyin = fopen(hostfile, "r");

    if (hostfile_yyin == NULL) {
	SPAWN_ERR("Can't open hostfile `%s [%s]'", hostfile, strerror(errno));
	exit(EXIT_FAILURE);
    }

    rv = yyparse();

    if (rv) {
        print_memory();
        free_memory();
        fclose(hostfile_yyin);
        
        exit(EXIT_FAILURE);
    }

    if (!hostmap) {
        SPAWN_ERR("No host found in hostfile `%s'\n", hostfile);
        print_memory();
        free_memory();
        fclose(hostfile_yyin);
        exit(EXIT_FAILURE);
    }

    strmap_merge(hm, hostmap);

    print_memory();
    hostmap = NULL;
    fclose(hostfile_yyin);

    return rv;
}
    
static void
free_memory(void)
{
    if (hostmap) {
        strmap_node * node = strmap_node_first(hostmap);

        while (node != NULL) {
            strmap_node * ptr;
            sscanf(strmap_node_value(node), "%p", &ptr);
            strmap_delete(&ptr);
            node = strmap_node_next(node);
        }

        strmap_delete(&hostmap);
    }
}

void
yyerror (char const * s)
{
    extern char const * hostfile;
    extern int lineno;

    SPAWN_ERR("Error parsing hostfile `%s' line %d - %s\n", hostfile, lineno, s);
}
