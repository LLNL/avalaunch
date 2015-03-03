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

/*
 * Local headers
 */
#include <session.h>
#include <unistd.h>
#include <node.h>
#include <print_errmsg.h>
#include <hostfile/parser.h>

#include "spawn.h"

/*
 * System headers
 */
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/utsname.h>
#include <limits.h>
#include <getopt.h>

#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/wait.h>

#include <libgen.h>

/* for reading elf */
#include <elf.h>

/* TODO: get this data some other way */
/* hard code ld.so.conf paths */
static char ld_so_conf_paths[] = "/usr/lib64:/usr/lib:/lib64:/lib";

/*******************************
 * Routines that identify libs to bcast
 ******************************/

/* given an existing path string (which may be NULL),
 * append specified string to end of it, frees existing
 * string and allocates and returns a new string if necessary, e.g.,
 * path = lib_path_append(path, newsection); */
static char* lib_path_append(char* path, const char* append)
{
    if (path != NULL) {
        if (append != NULL) {
            /* path and append are both valid */
            char* newpath = SPAWN_STRDUPF("%s:%s", path, append);
            spawn_free(&path);
            return newpath;
        }

        /* append is NULL, but path is valid */
        return path;
    } else {
        if (append != NULL) {
            /* path is NULL, but append is not, copy append */
            char* newpath = SPAWN_STRDUP(append);
            return newpath;
        }

        /* both are NULL */
        return NULL;
    }
}

/* given a strmap of path strings, like RPATH, LD_LIBRARY_PATH, RUNPATH,
 * and LDSO, search for specified library file in these paths.  Note,
 * it's critical that we search in the same order as the loader. */
static char* lib_path_search(const char* lib, const strmap* paths)
{
    char* path = NULL;

    /* check that we got a real string for the library */
    if (lib == NULL) {
        return path;
    }

    /* if we can resolve library as it is, return that */
#if 0
    char pathbuf[PATH_MAX];
    char* newcmd = realpath(command, pathbuf);
    if (newcmd != NULL) {
        path = SPAWN_STRDUP(pathbuf);
        return path;
    }
#endif

    /* if lib name starts with '/', it's already absolute */
    if (lib[0] == '/') {
        path = SPAWN_STRDUP(lib);
        return path;
    }

    /* construct search path:
     * if RUNPATH is not set:
     *   RPATH, LD_LIBRARY_PATH, ld.so.cache
     * if RUNPATH is set:
     *   RUNPATH, LD_LIBRARY_PATH, RPATH, ld.so.cache */
    const char* rpath     = strmap_get(paths, "RPATH");
    const char* ldlibpath = strmap_get(paths, "LD_LIBRARY_PATH");
    const char* runpath   = strmap_get(paths, "RUNPATH");
    const char* ldsopath  = strmap_get(paths, "LDSO");

    /* construct list of paths to search */
    char* path_env = NULL;
    if (runpath == NULL) {
        /* run path is not set */
        path_env = lib_path_append(path_env, rpath);
        path_env = lib_path_append(path_env, ldlibpath);
    } else {
        /* runpath is set */
        path_env = lib_path_append(path_env, runpath);
        path_env = lib_path_append(path_env, ldlibpath);
        path_env = lib_path_append(path_env, rpath);
    }
    path_env = lib_path_append(path_env, ldsopath);
    if (path_env == NULL) {
        /* $PATH is not set, bail out */
        return path;
    }

    /* search entries in path, breaking on ':' */
    const char* prefix = strtok(path_env, ":");
    while (prefix != NULL) {
        /* create path to candidate item */
        path = SPAWN_STRDUPF("%s/%s", prefix, lib);

        /* break if we find a readable file */
        if (access(path, R_OK) == 0) {
            /* TODO: should we run realpath here? */
            break;
        }

        /* otherwise, free the item and try the next
         * path entry */
        spawn_free(&path);
        prefix = strtok(NULL, ":");
    }

#if 0
    /* get absolute path name */
    if (path != NULL) {
        char pathbuf[PATH_MAX];
        realpath(path, pathbuf);
        spawn_free(&path);
        path = SPAWN_STRDUP(pathbuf);
    }
#endif

    /* TODO: copy mapping of libname to file */

    /* free copy of path */
    spawn_free(&path_env);

    return path;
}

/* reliable read from file descriptor (retries, if necessary, until
 * hard error or retries are exhausted) */
static ssize_t reliable_read(int fd, void* buf, size_t size)
{
    size_t n = 0;
    int retries = 10;
    while (n < size)
    {
        ssize_t rc = read(fd, (char*) buf + n, size - n);
        if (rc > 0) {
            /* read was successful, although perhaps short,
             * add up bytes read so far */
            n += (size_t) rc;
        } else if (rc == 0) {
            /* we consider this as EOF, return number read so far */
            return (ssize_t) n;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                /* not fatal, retry the read */
                continue;
            }

            /* something worth printing an error about */
            retries--;
            if (retries == 0) {
                /* too many failed retries, give up */
                SPAWN_ERR("Giving up read: read(%d, %p, %ld) errno=%d %s",
                    fd, (char*) buf + n, size - n, errno, strerror(errno)
                );
                return rc;
            }
        }
    }
    return (ssize_t) size;
}

/* given offset and size of string table in file, read it in and add
 * entries to our strmap, each entry maps index (integer) to string */
static void lib_read_strmap(const char* file, int fd, off_t pos, size_t size, strmap* map)
{
    /* seek to start of string table */
    off_t newpos = lseek(fd, pos, SEEK_SET);
    if (newpos == (off_t)-1) {
        SPAWN_ERR("Failed to seek in file %s to %llu (errno=%d %s)",
            file, (unsigned long long) pos, errno, strerror(errno)
        );
        return;
    }

    /* read in string table */
    char* strbuf = (char*) SPAWN_MALLOC(size);
    ssize_t nread = reliable_read(fd, strbuf, size);
    if (nread != (ssize_t) size) {
        SPAWN_ERR("Failed to read pheader in file %s (errno=%d %s)",
            file, errno, strerror(errno)
        );
        spawn_free(&strbuf);
        return;
    }

    /* copy strings into map */
    char* str = strbuf;
    size_t strread = (size_t) (str - strbuf);
    while (strread < size) {
        /* TODO: make sure string doesn't run past end of buffer */

        /* copy string into map */
        strmap_setf(map, "%d=%s", (int) strread, str);

        /* advance to next string */
        size_t len = strlen(str);
        str += len + 1;
        strread = (int) (str - strbuf);
    }

    /* free buffer holding string table */
    spawn_free(&strbuf);

    return;
}

/* given a pointer to the elf header, lookup string table,
 * read it in, and return it as a newly allocated strmap */
static strmap* lib_get_strmap32(const char* file, int fd, const Elf32_Ehdr* elfhdr)
{
    /* TODO: can we use e_shstrndx as a shortcut,
     * lseek to (e_shoff + e_shstrndx * e_shentsize)? */

    /* create an empty map */
    strmap* map = strmap_new();

    /* lookup number of entries in section table and the size of each entry */
    Elf32_Half size = elfhdr->e_shentsize;
    Elf32_Half num  = elfhdr->e_shnum;
  
    /* seek to section table */
    off_t pos = (off_t) elfhdr->e_shoff;
    off_t newpos = lseek(fd, pos, SEEK_SET);
    if (newpos == (off_t)-1) {
        SPAWN_ERR("Failed to seek in file %s to %llu (errno=%d %s)",
            file, (unsigned long long) pos, errno, strerror(errno)
        );
        return map;
    }

    /* read in the section table */
    size_t sheader_bufsize = size * num;
    Elf32_Shdr* sheader_buf = (Elf32_Shdr*) SPAWN_MALLOC(sheader_bufsize);
    size_t nread = reliable_read(fd, sheader_buf, sheader_bufsize);
    if (nread != sheader_bufsize) {
        SPAWN_ERR("Failed to read pheader in file %s (errno=%d %s)",
            file, errno, strerror(errno)
        );
        spawn_free(&sheader_buf);
        return map;
    }

    /* iterate through entries in program header until we find SHT_STRTAB */
    int found = 0;
    Elf32_Off offset;
    Elf32_Xword bytes;
    int i;
    for (i = 0; i < num; i++) {
        Elf32_Shdr* ptr = &sheader_buf[i];
        Elf32_Word type = ptr->sh_type;
        if (type == SHT_STRTAB) {
            offset = ptr->sh_offset;
            bytes  = ptr->sh_size;
            found  = 1;
            break;
        }
    }

    /* bail out if this file does not have a dynamic section */
    if (! found) {
        spawn_free(&sheader_buf);
        return map;
    }

    /* record string table in map (offset to string) */
    lib_read_strmap(file, fd, offset, (size_t) bytes, map);

    /* free the section table */
    spawn_free(&sheader_buf);

    return map;
}

/* given a pointer to the elf header, lookup string table,
 * read it in, and return it as a newly allocated strmap */
static strmap* lib_get_strmap64(const char* file, int fd, const Elf64_Ehdr* elfhdr)
{
    /* TODO: can we use e_shstrndx as a shortcut,
     * lseek to (e_shoff + e_shstrndx * e_shentsize)? */

    /* create an empty map */
    strmap* map = strmap_new();

    /* lookup number of entries in section table and the size of each entry */
    Elf64_Half size = elfhdr->e_shentsize;
    Elf64_Half num  = elfhdr->e_shnum;
  
    /* seek to section table */
    off_t pos = (off_t) elfhdr->e_shoff;
    off_t newpos = lseek(fd, pos, SEEK_SET);
    if (newpos == (off_t)-1) {
        SPAWN_ERR("Failed to seek in file %s to %llu (errno=%d %s)",
            file, (unsigned long long) pos, errno, strerror(errno)
        );
        return map;
    }

    /* read in the section table */
    size_t sheader_bufsize = size * num;
    Elf64_Shdr* sheader_buf = (Elf64_Shdr*) SPAWN_MALLOC(sheader_bufsize);
    size_t nread = reliable_read(fd, sheader_buf, sheader_bufsize);
    if (nread != sheader_bufsize) {
        SPAWN_ERR("Failed to read pheader in file %s (errno=%d %s)",
            file, errno, strerror(errno)
        );
        spawn_free(&sheader_buf);
        return map;
    }

    /* iterate through entries in program header until we find SHT_STRTAB */
    int found = 0;
    Elf64_Off offset;
    Elf64_Xword bytes;
    int i;
    for (i = 0; i < num; i++) {
        Elf64_Shdr* ptr = &sheader_buf[i];
        Elf64_Word type = ptr->sh_type;
        if (type == SHT_STRTAB) {
            offset = ptr->sh_offset;
            bytes  = ptr->sh_size;
            found  = 1;
            break;
        }
    }

    /* bail out if this file does not have a dynamic section */
    if (! found) {
        spawn_free(&sheader_buf);
        return map;
    }

    /* record string table in map (offset to string) */
    lib_read_strmap(file, fd, offset, (size_t) bytes, map);

    /* free the section table */
    spawn_free(&sheader_buf);

    return map;
}

/* forward declare for recusion */
static int lib_lookup(const char* file, strmap* lib2file);

static int lib_read_needed32(const char* file, int fd, const Elf32_Ehdr* elfhdr, const strmap* map, strmap* lib2file)
{
    /* lookup number of entries in program header and the size of
     * each entry */
    Elf32_Half size = elfhdr->e_phentsize;
    Elf32_Half num  = elfhdr->e_phnum;

    /* seek to program header */
    off_t pos = elfhdr->e_phoff;
    off_t newpos = lseek(fd, pos, SEEK_SET);
    if (newpos == (off_t)-1) {
        SPAWN_ERR("Failed to seek in file %s to %llu (errno=%d %s)",
            file, (unsigned long long) pos, errno, strerror(errno)
        );
        return 1;
    }

    /* read in the program header */
    size_t pheader_bufsize = size * num;
    Elf32_Phdr* pheader_buf = (Elf32_Phdr*) SPAWN_MALLOC(pheader_bufsize);
    ssize_t nread = reliable_read(fd, pheader_buf, pheader_bufsize);
    if (nread != (ssize_t) pheader_bufsize) {
        SPAWN_ERR("Failed to read pheader in file %s (errno=%d %s)",
            file, errno, strerror(errno)
        );
        spawn_free(&pheader_buf);
        return 1;
    }

    /* iterate through entries in program header until we find PT_DYNAMIC */
    int dynamic = 0;
    Elf32_Off dynoff;
    int i;
    for (i = 0; i < num; i++) {
        Elf32_Phdr* ptr = &pheader_buf[i];
        Elf32_Word type = ptr->p_type;
        if (type == PT_DYNAMIC) {
            dynoff = ptr->p_offset;
            dynamic = 1;
            break;
        }
    }

    /* bail out if this file does not have a dynamic section */
    if (! dynamic) {
        spawn_free(&pheader_buf);
        return 0;
    }

    /* otherwise seek to start of dynamic section */
    pos = (off_t) dynoff;
    newpos = lseek(fd, pos, SEEK_SET);
    if (newpos == (off_t)-1) {
        SPAWN_ERR("Failed to seek in file %s to %llu (errno=%d %s)",
            file, (unsigned long long) pos, errno, strerror(errno)
        );
        spawn_free(&pheader_buf);
        return 1;
    }

    int libs = 0;
    strmap* libmap = strmap_new();
    strmap* paths  = strmap_new();

    /* hard code some ld.so.cache paths */
    strmap_set(paths, "LDSO", ld_so_conf_paths);

    const char* ldlibpath = getenv("LD_LIBRARY_PATH");
    if (ldlibpath != NULL) {
        strmap_setf(paths, "LD_LIBRARY_PATH=%s", ldlibpath);
    }

    /* read entries from the dynamic section until we find a DT_NULL
     * entry, record details for each DT_NEEDED entry */
    size_t dyn_bufsize = sizeof(Elf32_Dyn);
    Elf32_Dyn* dyn_buf = (Elf32_Dyn*) SPAWN_MALLOC(dyn_bufsize);
    do {
        /* read the next entry from the dynamic section */
        nread = reliable_read(fd, dyn_buf, dyn_bufsize);
        if (nread != (ssize_t) dyn_bufsize) {
            SPAWN_ERR("Failed to read dyn in file %s (errno=%d %s)",
                file, errno, strerror(errno)
            );
            spawn_free(&dyn_buf);
            spawn_free(&pheader_buf);
            return 1;
        }

        /* set RPATH if we find it */
        if (dyn_buf->d_tag == DT_RPATH) {
            Elf32_Addr d_ptr = dyn_buf->d_un.d_ptr;
            const char* path = strmap_getf(map, "%d", (int) d_ptr);
            strmap_setf(paths, "RPATH=%s", path);
        }

        /* set RUNPATH if we find it */
        if (dyn_buf->d_tag == DT_RUNPATH) {
            Elf32_Addr d_ptr = dyn_buf->d_un.d_ptr;
            const char* path = strmap_getf(map, "%d", (int) d_ptr);
            strmap_setf(paths, "RUNPATH=%s", path);
        }

        /* if we find a DT_NEEDED entry, print corresponding string */
        if (dyn_buf->d_tag == DT_NEEDED) {
            Elf32_Addr d_ptr = dyn_buf->d_un.d_ptr;
            const char* libname = strmap_getf(map, "%d", (int) d_ptr);
            strmap_setf(libmap, "%d=%s", libs, libname);
            libs++;
        }
    } while (dyn_buf->d_tag != DT_NULL);

    int newfiles = 0;
    strmap* newfilemap = strmap_new();

    /* iterate over each library and compute path */
    int rc = 0;
    for (i = 0; i < libs; i++) {
        const char* libname = strmap_getf(libmap, "%d", i);
        const char* existing = strmap_get(lib2file, libname);
        if (existing == NULL) {
            const char* pathname = lib_path_search(libname, paths);

            /* if we failed to find the library, print an error,
             * set our return code, and try the next one */
            if (pathname == NULL) {
                SPAWN_ERR("Failed to find library: %s", libname);
                rc = 1;
                continue;
            }

            strmap_set(lib2file, libname, pathname);

            /* add entry to recurse on this file */
            strmap_setf(newfilemap, "%d=%s", newfiles, pathname);
            newfiles++;

            spawn_free(&pathname);
        }
    }

    /* now do breadth-first search */
    for (i = 0; i < newfiles; i++) {
        const char* filename = strmap_getf(newfilemap, "%d", i);
        int tmp_rc = lib_lookup(filename, lib2file);
        if (tmp_rc != 0) {
            rc = tmp_rc;
        }
    }

    strmap_delete(&newfilemap);
    strmap_delete(&libmap);

    /* free memory buffers */
    spawn_free(&dyn_buf);
    spawn_free(&pheader_buf);

    return rc;
}

static int lib_read_needed64(const char* file, int fd, const Elf64_Ehdr* elfhdr, const strmap* map, strmap* lib2file)
{
    /* lookup number of entries in program header and the size of
     * each entry */
    Elf64_Half size = elfhdr->e_phentsize;
    Elf64_Half num  = elfhdr->e_phnum;

    /* seek to program header */
    off_t pos = elfhdr->e_phoff;
    off_t newpos = lseek(fd, pos, SEEK_SET);
    if (newpos == (off_t)-1) {
        SPAWN_ERR("Failed to seek in file %s to %llu (errno=%d %s)",
            file, (unsigned long long) pos, errno, strerror(errno)
        );
        return 1;
    }

    /* read in the program header */
    size_t pheader_bufsize = size * num;
    Elf64_Phdr* pheader_buf = (Elf64_Phdr*) SPAWN_MALLOC(pheader_bufsize);
    ssize_t nread = reliable_read(fd, pheader_buf, pheader_bufsize);
    if (nread != (ssize_t) pheader_bufsize) {
        SPAWN_ERR("Failed to read pheader in file %s (errno=%d %s)",
            file, errno, strerror(errno)
        );
        spawn_free(&pheader_buf);
        return 1;
    }

    /* iterate through entries in program header until we find PT_DYNAMIC */
    int dynamic = 0;
    Elf64_Off dynoff;
    int i;
    for (i = 0; i < num; i++) {
        Elf64_Phdr* ptr = &pheader_buf[i];
        Elf64_Word type = ptr->p_type;
        if (type == PT_DYNAMIC) {
            dynoff = ptr->p_offset;
            dynamic = 1;
            break;
        }
    }

    /* bail out if this file does not have a dynamic section */
    if (! dynamic) {
        spawn_free(&pheader_buf);
        return 0;
    }

    /* otherwise seek to start of dynamic section */
    pos = (off_t) dynoff;
    newpos = lseek(fd, pos, SEEK_SET);
    if (newpos == (off_t)-1) {
        SPAWN_ERR("Failed to seek in file %s to %llu (errno=%d %s)",
            file, (unsigned long long) pos, errno, strerror(errno)
        );
        spawn_free(&pheader_buf);
        return 1;
    }

    int libs = 0;
    strmap* libmap = strmap_new();
    strmap* paths  = strmap_new();

    /* hard code some ld.so.cache paths */
    strmap_set(paths, "LDSO", ld_so_conf_paths);

    const char* ldlibpath = getenv("LD_LIBRARY_PATH");
    if (ldlibpath != NULL) {
        strmap_setf(paths, "LD_LIBRARY_PATH=%s", ldlibpath);
    }

    /* read entries from the dynamic section until we find a DT_NULL
     * entry, record details for each DT_NEEDED entry */
    size_t dyn_bufsize = sizeof(Elf64_Dyn);
    Elf64_Dyn* dyn_buf = (Elf64_Dyn*) SPAWN_MALLOC(dyn_bufsize);
    do {
        /* read the next entry from the dynamic section */
        nread = reliable_read(fd, dyn_buf, dyn_bufsize);
        if (nread != (ssize_t) dyn_bufsize) {
            SPAWN_ERR("Failed to read dyn in file %s (errno=%d %s)",
                file, errno, strerror(errno)
            );
            spawn_free(&dyn_buf);
            spawn_free(&pheader_buf);
            return 1;
        }

        /* set RPATH if we find it */
        if (dyn_buf->d_tag == DT_RPATH) {
            Elf64_Addr d_ptr = dyn_buf->d_un.d_ptr;
            const char* path = strmap_getf(map, "%d", (int) d_ptr);
            strmap_setf(paths, "RPATH=%s", path);
        }

        /* set RUNPATH if we find it */
        if (dyn_buf->d_tag == DT_RUNPATH) {
            Elf64_Addr d_ptr = dyn_buf->d_un.d_ptr;
            const char* path = strmap_getf(map, "%d", (int) d_ptr);
            strmap_setf(paths, "RUNPATH=%s", path);
        }

        /* if we find a DT_NEEDED entry, print corresponding string */
        if (dyn_buf->d_tag == DT_NEEDED) {
            Elf64_Addr d_ptr = dyn_buf->d_un.d_ptr;
            const char* libname = strmap_getf(map, "%d", (int) d_ptr);
            strmap_setf(libmap, "%d=%s", libs, libname);
            libs++;
        }
    } while (dyn_buf->d_tag != DT_NULL);

    int newfiles = 0;
    strmap* newfilemap = strmap_new();

    /* iterate over each library and compute path */
    int rc = 0;
    for (i = 0; i < libs; i++) {
        const char* libname = strmap_getf(libmap, "%d", i);
        const char* existing = strmap_get(lib2file, libname);
        if (existing == NULL) {
            const char* pathname = lib_path_search(libname, paths);

            /* if we failed to find the library, print an error,
             * set our return code, and try the next one */
            if (pathname == NULL) {
                SPAWN_ERR("Failed to find library: %s", libname);
                rc = 1;
                continue;
            }

            strmap_set(lib2file, libname, pathname);

            /* add entry to recurse on this file */
            strmap_setf(newfilemap, "%d=%s", newfiles, pathname);
            newfiles++;

            spawn_free(&pathname);
        }
    }

    /* now do breadth-first search */
    for (i = 0; i < newfiles; i++) {
        const char* filename = strmap_getf(newfilemap, "%d", i);
        int tmp_rc = lib_lookup(filename, lib2file);
        if (tmp_rc != 0) {
            rc = tmp_rc;
        }
    }

    strmap_delete(&newfilemap);
    strmap_delete(&libmap);

    /* free memory buffers */
    spawn_free(&dyn_buf);
    spawn_free(&pheader_buf);

    return rc;
}

/* given a path to an executable, open the file and lookup
 * paths to its dependents libraries (as recorded in ELF data). */
static int lib_lookup(const char* file, strmap* lib2file)
{
    /* open the file for reading */
    int fd = open(file, O_RDONLY);
    if (fd < 0) {
        SPAWN_ERR("Failed to open file %s (errno=%d %s)",
            file, errno, strerror(errno)
        );
        return 1;
    }

    /* allocate space for ELF header */
    size_t header_bufsize = (sizeof(Elf32_Ehdr) < sizeof(Elf64_Ehdr)) ? sizeof(Elf64_Ehdr) : sizeof(Elf32_Ehdr);
    char* header_buf = (char*) SPAWN_MALLOC(header_bufsize);

    /* read in magic number and other fields to determine whether
     * this is an ELF file and if so whether it's 32 or 64-bit */
    ssize_t nread = reliable_read(fd, header_buf, EI_NIDENT);
    if (nread != (ssize_t) EI_NIDENT) {
        printf("Failed to read magic number info from file %s (errno=%d %s)\n", file, errno, strerror(errno));
        return 1;
    }

    /* check that the magic number matches */
    if (header_buf[EI_MAG0] != ELFMAG0 ||
        header_buf[EI_MAG1] != ELFMAG1 ||
        header_buf[EI_MAG2] != ELFMAG2 ||
        header_buf[EI_MAG3] != ELFMAG3)
    {
        printf("File %s is not ELF\n", file);
        return 1;
    }

    /* verify that we can process this elf file type */
    if (header_buf[EI_CLASS] != ELFCLASS32 &&
        header_buf[EI_CLASS] != ELFCLASS64)
    {
        printf("File %s is not ELF32 or ELF64\n", file);
        return 1;
    }

    /* now that we know the file type, read in the rest of the header */
    int rc = 0;
    if (header_buf[EI_CLASS] == ELFCLASS64) {
        size_t remainder = sizeof(Elf64_Ehdr) - EI_NIDENT;
        nread = reliable_read(fd, header_buf + EI_NIDENT, remainder);
        if (nread != (ssize_t) remainder) {
          printf("Failed to read magic number info from file %s (errno=%d %s)\n", file, errno, strerror(errno));
          return 1;
        }

        /* read string map from file */
        strmap* map = lib_get_strmap64(file, fd, (Elf64_Ehdr*) header_buf);

        /* read and print needed entries */
        rc = lib_read_needed64(file, fd, (Elf64_Ehdr*) header_buf, map, lib2file);

        /* free our string map */
        strmap_delete(&map);
    } else if (header_buf[EI_CLASS] == ELFCLASS32) {
        size_t remainder = sizeof(Elf32_Ehdr) - EI_NIDENT;
        nread = reliable_read(fd, header_buf + EI_NIDENT, remainder);
        if (nread != (ssize_t) remainder) {
          printf("Failed to read magic number info from file %s (errno=%d %s)\n", file, errno, strerror(errno));
          return 1;
        }
  
        /* read string map from file */
        strmap* map = lib_get_strmap32(file, fd, (Elf32_Ehdr*) header_buf);
  
        /* read and print needed entries */
        rc = lib_read_needed32(file, fd, (Elf32_Ehdr*) header_buf, map, lib2file);
  
        /* free our string map */
        strmap_delete(&map);
    }

    /* free the elf header */
    spawn_free(&header_buf);

    /* close the file */
    close(fd);

    return rc;
}

/* given a path to an executable, lookup its list of libraries
 * and record the full path to each in the given map */
int lib_capture(strmap* map, const char* file)
{
    /* create a map to record library name to its full path */
    strmap* lib2file = strmap_new();

    /* read ELF data in executable to map each library
     * to its full path */
    int rc = lib_lookup(file, lib2file);

    /* record the full path to each library in output
     * map, count up the total number of libs as we go */
    int libs = 0;
    strmap_node* node;
    for (node = strmap_node_first(lib2file);
         node != NULL;
         node = strmap_node_next(node))
    {
        /* get library name and its path */
        const char* lib  = strmap_node_key(node);
        const char* path = strmap_node_value(node);
        //printf("%s: %d: %s --> %s\n", file, libs, key, val);
        strmap_setf(map, "LIB%d=%s", libs, path);
        libs++;
    }

    /* record total number of libraries */
    strmap_setf(map, "LIBS=%d", libs);

    /* free map of library file name to its full path */
    strmap_delete(&lib2file);

    return rc;
}
