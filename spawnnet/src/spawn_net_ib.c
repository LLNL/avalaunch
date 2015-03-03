/*
 * Copyright (C) 1999-2001 The Regents of the University of California
 * (through E.O. Lawrence Berkeley National Laboratory), subject to
 * approval by the U.S. Department of Energy.
 *
 * Use of this software is under license. The license agreement is included
 * in the file MVICH_LICENSE.TXT.
 *
 * Developed at Berkeley Lab as part of MVICH.
 *
 * Authors: Bill Saphir      <wcsaphir@lbl.gov>
 *          Michael Welcome  <mlwelcome@lbl.gov>
 */

/* Copyright (c) 2001-2013, The Ohio State University. All rights
 * reserved.
 *
 * This file is part of the MVAPICH2 software package developed by the
 * team members of The Ohio State University's Network-Based Computing
 * Laboratory (NBCL), headed by Professor Dhabaleswar K. (DK) Panda.
 *
 * For detailed copyright and licensing information, please refer to the
 * copyright file COPYRIGHT in the top level MVAPICH2 directory.
 *
 */

/*
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-667277.
 * All rights reserved.
 * This file is part of the SpawnNet library.
 * For details, see https://github.com/hpc/spawnnet
 * Please also read this file: LICENSE.TXT.
*/

#include <pthread.h>
#include <limits.h>

#include "spawn_internal.h"
#include "spawn_net_ib_internal.h"

/* need to block SIGCHLD in comm_thread */
#include <signal.h>

/* need to increase MEMLOCK limit */
#include <sys/resource.h>

/* TODO: bury all of these globals in allocated memory */
static int64_t g_count_open = 0; /* number of active endpoint opens (spawn_net_open w/o close) */
static int64_t g_count_conn = 0; /* number of active VC connections (local + remote) */
static uint64_t g_ud_ep_id  = 0; /* next endpoint id to be assigned */

static mv2_proc_info_t proc;
static mv2_hca_info_t g_hca_info;
static ud_addr local_ep_info; /* caches lid and qpn of our UDQP */

/* Tracks an array of virtual channels.  With each new channel created,
 * the id is incremented.  Grows channel array as needed. */
static vc_t** g_ud_vc_info       = NULL; /* VC array */
static uint64_t g_ud_vc_infos    = 0;    /* capacity of VC array */
static uint64_t g_ud_vc_info_id  = 0;    /* next id to be assigned */

/* we create a vc to ourself just to send SHUTDOWN packet to recv thread */
static vc_t* g_vc_self = NULL;
static int g_shutdown = 0; /* recv thread will set this flag to one upon handling a shutdown message */

static int rdma_enable_hugepage = 1;
static int rdma_vbuf_secondary_pool_size = RDMA_VBUF_SECONDARY_POOL_SIZE;
static int rdma_max_inline_size = RDMA_DEFAULT_MAX_INLINE_SIZE;
static uint16_t rdma_default_ud_mtu = 2048;

static uint32_t rdma_default_max_ud_send_wqe = RDMA_DEFAULT_MAX_UD_SEND_WQE;
static uint32_t rdma_default_max_ud_recv_wqe = RDMA_DEFAULT_MAX_UD_RECV_WQE;
static uint32_t rdma_default_ud_sendwin_size = 400; /* Max number of outstanding buffers (waiting for ACK)*/
static uint32_t rdma_default_ud_recvwin_size = 2501; /* Max number of buffered out-of-order messages */
static long rdma_ud_progress_timeout  =   25000; /* Time (usec) until ACK status is checked (and ACKs sent) */
static long rdma_ud_min_retry_timeout =   50000; /* Min time (usec) to wait before resending */
static long rdma_ud_max_retry_timeout = 2000000; /* Max time (usec) to wait before resending */
static long rdma_ud_last_check;                  /* time at which we last checked for retries */
static uint16_t rdma_ud_max_retry_count = 1000;  /* max number of resends before tossing packet */
static uint16_t rdma_ud_max_ack_pending;         /* max number of recieves before forcing an ack */

static struct timespec cm_remain;
static struct timespec cm_timeout;

static pthread_t recv_thread;    /* thread that handles CQ events */
static pthread_t timeout_thread; /* thread that resends packets if timeout expires */
static int force_shutdown = 0;   /* flag indicating caller is in spawn_net_close_ib */

static int g_recv_busy_spin = 0;   /* whether we should busy spin or yield CPU while waiting */
static int g_recv_flag;            /* flag indicating whether main thread is waiting */
static pthread_cond_t g_recv_cond; /* condition variable to wait on incoming msg */

/*******************************************
 * interface to lock/unlock communication
 ******************************************/

/* this thread is used to ensure main thread and
 * UD progress thread don't step on each other */

static pthread_mutex_t comm_lock_object;

static inline void comm_lock(void)
{           
    int rc = pthread_mutex_lock(&comm_lock_object);
    if (rc != 0) {
        SPAWN_ERR("Failed to lock comm mutex (pthread_mutex_lock rc=%d %s)", rc, strerror(rc));
    }
    return;
}
            
static inline void comm_unlock(void)
{           
    int rc = pthread_mutex_unlock(&comm_lock_object);
    if (rc != 0) {
        SPAWN_ERR("Failed to unlock comm mutex (pthread_mutex_unlock rc=%d %s)", rc, strerror(rc));
    }
    return;
}

/*******************************************
 * Message queue functions
 ******************************************/

/* message queues manage various lists of vbufs */

enum {
    MSG_QUEUED_RECVWIN,
    MSG_IN_RECVWIN
};

static inline void ext_sendq_add(message_queue_t *q, vbuf *v)
{
    v->desc.next = NULL;
    if (q->head == NULL) {
        q->head = v;
    } else {
        q->tail->desc.next = v;
    }
    q->tail = v;
    q->count++;
}

/* adds vbuf to extended send queue, which tracks messages we will be
 * sending but haven't yet */
static inline void ext_window_add(message_queue_t *q, vbuf *v)
{
    v->extwin_msg.next = v->extwin_msg.prev = NULL;
    if (q->head == NULL) {
        q->head = v;
    } else {
        (q->tail)->extwin_msg.next = v;
    }
    q->tail = v;
    q->count++;
}

/* adds vbuf to the send queue, which tracks packets a VC
 * has submitted to the UD context */
static inline void send_window_add(message_queue_t* q, vbuf* v)
{
    /* record that vbuf is in the send window */
    v->in_sendwin = 1;

    /* set packet as last item */
    v->sendwin_msg.prev = q->tail;
    v->sendwin_msg.next = NULL;

    /* place packet at front of queue if it's empty,
     * otherwise update last item to point to this one */   
    if(q->head == NULL) {
        q->head = v;
    } else {
        (q->tail)->sendwin_msg.next = v;
    }

    /* update the tail to point to this packet,
     * and increase the count */
    q->tail = v;
    q->count++;

    return;
}

/* removes vbuf from send queue */
static inline void send_window_remove(message_queue_t* q, vbuf* v)
{
    /* get pointers to elements on either side of this vbuf */
    vbuf* prev = v->sendwin_msg.prev;
    vbuf* next = v->sendwin_msg.next;

    /* update head if packet is at start of list */
    if (q->head == v) {
        q->head = next;
    }

    /* update tail if packet is at end of list */
    if (q->tail == v) {
        q->tail = prev;
    }

    /* fix up list elements to skip this vbuf */
    if (prev != NULL) {
        prev->sendwin_msg.next = next;
    }
    if (next != NULL) {
        next->sendwin_msg.prev = prev;
    }

    /* decrease the length of the list */
    q->count--;

    /* cleanup linked list fields in vbuf */
    v->sendwin_msg.prev = NULL;
    v->sendwin_msg.next = NULL;

    /* mark that vbuf is no longer in send queue */
    v->in_sendwin = 0;

    return;
}

static inline void unack_queue_add(message_queue_t *q, vbuf *v)
{
    v->unack_msg.next = NULL;

    if (q->head == NULL) {
        q->head = v;
        v->unack_msg.prev = NULL;
    } else {
        (q->tail)->unack_msg.next = v;
        v->unack_msg.prev = q->tail;
    }

    q->tail = v;
    q->count++;
}

static inline void unack_queue_remove(message_queue_t *q, vbuf *v)
{
    vbuf *next = v->unack_msg.next;
    vbuf *prev = v->unack_msg.prev;

    if (prev == NULL) {
        q->head = next;
    } else {
        prev->unack_msg.next = next;
    }

    if (next == NULL) {
        q->tail = prev;
    } else {
        next->unack_msg.prev = prev;
    }
    v->unack_msg.next = v->unack_msg.prev = NULL;
    q->count--;
}

static inline int recv_window_add(message_queue_t *q, vbuf *v, int recv_win_start)
{
    /* clear next and previous pointers in vbuf */
    v->recvwin_msg.next = v->recvwin_msg.prev = NULL;

    /* insert vbuf into recv queue in order by its sequence number */
    if(q->head == NULL) {
        /* trivial insert if list is empty */
        q->head = q->tail = v;
    } else {
        /* otherwise, we have at least one item already in list,
         * get a pointer to current head */ 
        vbuf* cur_buf = q->head;

        /* if our sequence number is greater than start of window */
        if (v->seqnum > recv_win_start) {
            /* current seq num is higher than start seq number, */
            /* iterate until we find the first item in the list
             * whose sequence number is greater or equal to vbuf,
             * or until we hit first item whose seq wraps (less
             * than or equal to start seq num) */
            if (cur_buf->seqnum < recv_win_start) {
                /* first item already wraps */
            } else {
                /* otherwise, search */
                while (cur_buf != NULL &&
                       cur_buf->seqnum < v->seqnum &&
                       cur_buf->seqnum > recv_win_start)
                {
                    cur_buf = cur_buf->recvwin_msg.next;
                }
            }
        } else {
            /* vbuf seq num is less than or equal to start seq num,
             * iterate until we find the first item in the list
             * whose sequence number is greater or equal to vbuf,
             * or until we hit first item whose seq wraps (less */
            if (cur_buf->seqnum > recv_win_start) {
                /* first item in list is greater than start, iterate
                 * until we find an item that wraps and then keep
                 * going until we find one that is equal or greater
                 * than vbuf */
                while (cur_buf != NULL &&
                       ((cur_buf->seqnum >= recv_win_start) ||
                        (cur_buf->seqnum  < v->seqnum)))
                { 
                    cur_buf = cur_buf->recvwin_msg.next;
                }
            } else {
                /* first item already wraps, just iterate until
                 * we find an item equal or greater than vbuf */
                while (cur_buf != NULL &&
                       cur_buf->seqnum < v->seqnum)
                {
                    cur_buf = cur_buf->recvwin_msg.next;
                }
            }
        }

        /* check whether we found an item with a sequence number equal
         * to or after vbuf seq number */
        if (cur_buf != NULL) {
            /* check whether item in list matches seq number of vbuf */
            if (cur_buf->seqnum == v->seqnum) {
                /* we found a matching item already in the queue */
                return MSG_IN_RECVWIN;
            }

            /* otherwise current item is larger, so insert vbuf
             * just before it */
            vbuf* prev_buf = cur_buf->recvwin_msg.prev;
            v->recvwin_msg.prev = prev_buf;
            v->recvwin_msg.next = cur_buf;

            /* update list pointers */
            if (cur_buf == q->head) {
                /* item is at front of list, so update head to
                 * point to vbuf */
                q->head = v;
            } else {
                /* otherwise item is somewhere in the middle,
                 * so update next pointer of previous item */
                prev_buf->recvwin_msg.next = v;
            }
            cur_buf->recvwin_msg.prev = v;
        } else {
            /* all items in queue come before vbuf, so tack vbuf on end */
            v->recvwin_msg.next = NULL;
            v->recvwin_msg.prev = q->tail;
            q->tail->recvwin_msg.next = v;
            q->tail = v;
        }

        /* increment size of queue */
        q->count++;
    }

    /* return code to indicate we inserted vbuf in queue */
    return MSG_QUEUED_RECVWIN; 
}

/* remove item from head of recv queue */
static inline void recv_window_remove(message_queue_t *q)
{
    vbuf *next = (q->head)->recvwin_msg.next;
    q->head = next;
    if (next != NULL) {
        next->recvwin_msg.prev = NULL;
    } else {
        q->head = q->tail = NULL;
    }
    q->count--;
}

/*******************************************
 * Virutal channel functions
 ******************************************/

/* initialize UD VC */
static void vc_init(vc_t* vc)
{
    /* init vc state */
    vc->state = VC_STATE_INIT;
    vc->local_closed  = 0;
    vc->remote_closed = 0;

    /* init remote address info */
    vc->ah  = NULL;
    vc->qpn = UINT32_MAX;
    vc->lid = UINT16_MAX;

    /* init context ids */
    vc->readid  = UINT64_MAX;
    vc->writeid = UINT64_MAX;

    /* init sequence numbers */
    vc->seqnum_next_tosend = 0;
    vc->seqnum_next_torecv = 0;
    vc->seqnum_next_toack  = UINT16_MAX;
    vc->ack_need_tosend    = 0;
    vc->ack_pending        = 0;

    /* init message queues */
    MESSAGE_QUEUE_INIT(&(vc->send_window));
    MESSAGE_QUEUE_INIT(&(vc->ext_window));
    MESSAGE_QUEUE_INIT(&(vc->recv_window));
    MESSAGE_QUEUE_INIT(&(vc->app_recv_window));

    /* init profile counters */
    vc->cntl_acks          = 0; 
    vc->resend_count       = 0;
    vc->ext_win_send_count = 0;

    return;
}

/* allocate and initialize a new VC */
static vc_t* vc_alloc()
{
    /* get a new id */
    uint64_t id = g_ud_vc_info_id;

    /* increment our counter for next time */
    g_ud_vc_info_id++;

    /* check whether we need to allocate more vc strucutres */
    if (id >= g_ud_vc_infos) {
        /* increase capacity of array */
        if (g_ud_vc_infos > 0) {
            g_ud_vc_infos *= 2;
        } else {
            g_ud_vc_infos = 1;
        }

        /* allocate space to hold vc pointers */
        size_t vcsize = g_ud_vc_infos * sizeof(vc_t*);
        vc_t** vcs = (vc_t**) SPAWN_MALLOC(vcsize);

        /* copy old values into new array */
        uint64_t i;
        for (i = 0; i < id; i++) {
            vcs[i] = g_ud_vc_info[i];
        }

        /* free old array and assign it to new copy */
        spawn_free(&g_ud_vc_info);
        g_ud_vc_info = vcs;
    }

    /* allocate vc structure */
    vc_t* vc = (vc_t*) SPAWN_MALLOC(sizeof(vc_t));

    /* initialize vc */
    vc_init(vc);

    /* record address of vc in array */
    g_ud_vc_info[id] = vc;

    /* set our read id, other end of channel will label its outgoing
     * messages with this id when sending to us (our readid is their
     * writeid) */
    vc->readid = id;

    /* initialize our read count to 0 */
    vc->nread = 0;

    /* return vc to caller */
    return vc;
}

/* release vc back to pool if we can */
static void vc_free(vc_t* vc)
{
    /* TODO: we can release vc only when: both local and remote
     * procs have disconnected and all packets have been acked,
     * but what to do with received data not read by user? */
#if 0
    if (vc->local_closed && vc->remote_closed) {
        /* get id from vc */
        uint64_t id = vc->readid;

        /* clear this vc from our array */
        g_ud_vc_info[id] = NULL;

        /* TODO: free off any memory allocated for vc */

        /* TODO: delete items from message queues */
        vc->send_window;
        vc->ext_window;
        vc->recv_window;
        vc->app_recv_window;

        /* TODO: delete any items from unack'd queue */

        /* destroy address handle */
        if (vc->ah != NULL) {
            int ret = ibv_destroy_ah(vc->ah);
            if (ret != 0) {
                SPAWN_ERR("Error in destroying address handle (ibv_destroy_ah rc=%d %s)", ret, strerror(ret));
            }
            vc->ah = NULL;
        }

        /* free vc object */
        spawn_free(&vc);
    }
#endif

    return;
}

static int vc_set_addr(vc_t* vc, ud_addr *rem_info, int port)
{
    /* don't bother to set anything if the state is already connecting
     * or connected */
    if (vc->state == VC_STATE_CONNECTING ||
        vc->state == VC_STATE_CONNECTED)
    {
        /* duplicate message - return */
        return 0;
    }

    /* clear address handle attribute structure */
    struct ibv_ah_attr ah_attr;
    memset(&ah_attr, 0, sizeof(ah_attr));

    /* initialize attribute values */
    /* TODO: set grh field? */
    ah_attr.dlid          = rem_info->lid;
    ah_attr.sl            = RDMA_DEFAULT_SERVICE_LEVEL;
    ah_attr.src_path_bits = 0; 
    /* TODO: set static_rate field? */
    ah_attr.is_global     = 0; 
    ah_attr.port_num      = port;

    /* create IB address handle and record in vc */
    vc->ah = ibv_create_ah(g_hca_info.pd, &ah_attr);
    if(vc->ah == NULL){    
        /* TODO: man page doesn't say anything about errno */
        SPAWN_ERR("Error in creating address handle (ibv_create_ah errno=%d %s)", errno, strerror(errno));
        return -1;
    }

    /* change vc state to "connecting" */
    vc->state = VC_STATE_CONNECTING;

    /* record remote lid and qpn in vc */
    vc->lid = rem_info->lid;
    vc->qpn = rem_info->qpn;

    return 0;
}

/*******************************************
 * vbuf functions
 ******************************************/

/* Vbufs are allocated in blocks called "regions".
 * Regions are linked together into a list.
 *
 * These data structures record information on all the vbuf
 * regions that have been allocated.  They can be used for
 * error checking and to un-register and deallocate the regions
 * at program termination.  */
typedef struct vbuf_region {
    struct ibv_mr* mem_handle; /* mem hndl for entire region */
    void* malloc_start;        /* used to free region later */
    void* malloc_end;          /* to bracket mem region */
    void* malloc_buf_start;    /* used to free DMA region later */
    void* malloc_buf_end;      /* bracket DMA region */
    int count;                 /* number of vbufs in region */
    struct vbuf* vbuf_head;    /* first vbuf in region */
    struct vbuf_region* next;  /* thread vbuf regions */
    int shmid;                 /* track shared memory id for huge pages */
} vbuf_region;

/* head of list of allocated vbuf regions */
static vbuf_region* vbuf_region_head = NULL;

/* track vbufs stats */
static vbuf* ud_vbuf_free_head     = NULL; /* list of free vbufs */
static int   ud_vbuf_num_allocated = 0;    /* total number allocated */
static long  ud_vbuf_num_free      = 0;    /* number currently free */
static long  ud_vbuf_num_get       = 0;    /* number of times vbufs taken from free list */
static long  ud_vbuf_num_freed     = 0;    /* number of times vbufs added to free list */

/* lock vbuf get/release calls */
static pthread_spinlock_t vbuf_lock;

/* initialize vbuf variables */
static int vbuf_init(void)
{
    int rc = pthread_spin_init(&vbuf_lock, 0);
    if (rc != 0) {
        SPAWN_ERR("Failed to init vbuf_lock (pthread_spin_init rc=%d %s)", rc, strerror(rc));
        spawn_exit(-1);
    }

    return 0;
}

static int vbuf_finalize()
{
  /* free regions */
  vbuf_region* reg = vbuf_region_head;
  while (reg != NULL) {
      /* record pointer to next region element
       * (we'll delete the current one) */
      vbuf_region* next = reg->next;

      /* deregister the memmory with the card */
      if (reg->mem_handle != NULL) {
          int dereg_rc = ibv_dereg_mr(reg->mem_handle);
          if (dereg_rc != 0) {
              SPAWN_ERR("Failed to deregister memory rc=%d %s", dereg_rc, strerror(dereg_rc));
          }
      }

      /* free DMA buffers */
      if (reg->shmid == -1) {
          /* we allocated buffers with posix_memalign */
          free(reg->malloc_buf_start);
      } else {
          /* we allocated buffers in huge pages using shmget/shmat */
          if (shmdt(reg->malloc_buf_start) != 0) {
              SPAWN_ERR("Failed to detach (shmdt errno=%d %s)", errno, strerror(errno));
          }
      }

      /* free vbuf data structures (allocated w/ posix_memalign) */
      free(reg->malloc_start);

      /* delete the region data structure elem (allocated w/ SPAWN_MALLOC) */
      spawn_free(&reg);

      /* go on to next element in list */
      reg = next;
  }

  /* update globals tracking vbufs */
  ud_vbuf_free_head     = NULL;
  ud_vbuf_num_allocated = 0;
  ud_vbuf_num_free      = 0;

  /* destroy pthread spin lock */
  int rc = pthread_spin_destroy(&vbuf_lock);
  if (rc != 0) {
        SPAWN_ERR("Failed to destory vbuf_lock (pthread_spin_destroy rc=%d %s)", rc, strerror(rc));
        spawn_exit(-1);
  }

  return 0;
}

static int alloc_hugepage_region(int *shmid, void **buffer, int *nvbufs, int buf_size)
{
    int ret = 0;
    size_t size = *nvbufs * buf_size;
    MRAILI_ALIGN_LEN(size, HUGEPAGE_ALIGN);

    /* create hugepage shared region */
    *shmid = shmget(IPC_PRIVATE, size, SHM_HUGETLB | IPC_CREAT | SHM_R | SHM_W);
    if (*shmid < 0) {
        if (rdma_enable_hugepage >= 2) {
            SPAWN_ERR("Failed to get shared memory id (shmget errno=%d %s)", errno, strerror(errno));
        }
        goto fn_fail;
    }

    /* attach shared memory */
    *buffer = (void*) shmat(*shmid, SHMAT_ADDR, SHMAT_FLAGS);
    if (*buffer == (void*) -1) {
        SPAWN_ERR("Failed to attach shared memory (shmat errno=%d %s)", errno, strerror(errno));

        /* destroy huge page segment that we created above */
        if (shmctl(*shmid, IPC_RMID, 0) != 0) {
            SPAWN_ERR("Failed to mark shared memory for removal (shmctl errno=%d %s)", errno, strerror(errno));
        }

        goto fn_fail;
    }
    
    /* Mark shmem for removal (will be deleted will last process calls shmdt) */
    if (shmctl(*shmid, IPC_RMID, 0) != 0) {
        SPAWN_ERR("Failed to mark shared memory for removal (shmctl errno=%d %s)", errno, strerror(errno));
    }
    
    /* Find max no.of vbufs can fit in allocated buffer */
    *nvbufs = size / buf_size;
     
fn_exit:
    return ret;
fn_fail:
    ret = -1;
    if (rdma_enable_hugepage >= 2) {
        SPAWN_ERR("Failed to allocate buffer from huge pages. "
                  "Fallback to regular pages. Requested buf size: %llu",
                  (long long unsigned) size
        );
    }
    goto fn_exit;
}    

static int vbuf_region_alloc(struct ibv_pd* pdomain, int nvbufs)
{
    int result;

    /* specify alignment parameters */
    int alignment_vbuf = 64;
    int alignment_dma = getpagesize();

    if (ud_vbuf_free_head != NULL) {
        SPAWN_ERR("Free vbufs available but trying to allocation more");
        spawn_exit(-1);
    }

    /* NOTE: this could change the value of nvbufs depending on
     * how many we can fit in the huge page region */
    /* get memory from huge pages if enabled */
    int shmid = -1;
    void* dmabuf = NULL;
    if (rdma_enable_hugepage) {
        result = alloc_hugepage_region(
            &shmid, &dmabuf, &nvbufs, rdma_default_ud_mtu
        );
    }

    /* compute sizes of buffers we need to allocate */
    size_t vbufs_size  = nvbufs * sizeof(vbuf);
    size_t dmabuf_size = nvbufs * rdma_default_ud_mtu;

    /* do posix_memalign if enable hugepage disabled or failed */
    if (rdma_enable_hugepage == 0 || result != 0)  {
        shmid = -1;
        result = posix_memalign(&dmabuf, alignment_dma, dmabuf_size);
        if (result != 0) {
            SPAWN_ERR("Cannot allocate vbuf region (posix_memalign rc=%d %s)", result, strerror(result));
        }
    }

    /* allocate memory for vbuf data structures */
    void* vbufs;
    result = posix_memalign((void**) &vbufs, alignment_vbuf, vbufs_size);
    if (result != 0) {
        SPAWN_ERR("Cannot allocate vbuf region (posix_memalign rc=%d %s)", result, strerror(result));
        return -1;
    }

    /* check that we got the dma buffer */
    if (result != 0 || NULL == dmabuf) {
        SPAWN_ERR("Failed to allocate vbufs");
        /* TODO: free above memory on failure (and shmid) */
        free(vbufs);
        spawn_exit(-1);
    }
    
    /* clear memory regions */
    memset(vbufs,             0, vbufs_size);
    memset(dmabuf, 0, dmabuf_size);

    /* update global vbuf variables */
    ud_vbuf_free_head      = vbufs;
    ud_vbuf_num_allocated += nvbufs;
    ud_vbuf_num_free      += nvbufs;

    /* fill in fields of vbuf_region structure */
    struct vbuf_region* reg = (struct vbuf_region*) SPAWN_MALLOC(sizeof(struct vbuf_region));
    reg->shmid            = shmid;
    reg->malloc_start     = vbufs;
    reg->malloc_end       = (void *) ((char *) vbufs + vbufs_size);
    reg->malloc_buf_start = dmabuf;
    reg->malloc_buf_end   = (void *) ((char *) dmabuf + dmabuf_size);
    reg->count            = nvbufs;
    reg->vbuf_head        = ud_vbuf_free_head;

    /* register memory region with HCA */
    reg->mem_handle = ibv_reg_mr(pdomain, dmabuf, dmabuf_size, IBV_ACCESS_LOCAL_WRITE);
    if (reg->mem_handle == NULL) {
        SPAWN_ERR("Cannot register vbuf region (ibv_reg_mr errno=%d %s)", errno, strerror(errno)); 
        /* TODO: need to free memory / unregister with some cards? */
        return -1;
    }

    /* init the vbuf structures */
    int i;
    for (i = 0; i < nvbufs; ++i) {
        /* get a pointer to the vbuf */
        vbuf* cur = ud_vbuf_free_head + i;

        /* set next pointer */
        cur->desc.next = ud_vbuf_free_head + i + 1;
        if (i == (nvbufs - 1)) {
            cur->desc.next = NULL;
        }

        /* set pointer to region */
        cur->region = reg;

        /* set pointer to data buffer */
        char* ptr = (char *)dmabuf + i * rdma_default_ud_mtu;
        cur->buffer = (void*) ptr;

        /* set remaining fields */
        cur->content_size = 0;

        /* initialize linked list pointers */
        cur->extwin_msg.prev     = NULL;
        cur->extwin_msg.next     = NULL;
        cur->sendwin_msg.prev    = NULL;
        cur->sendwin_msg.next    = NULL;
        cur->apprecvwin_msg.prev = NULL;
        cur->apprecvwin_msg.next = NULL;
        cur->recvwin_msg.prev    = NULL;
        cur->recvwin_msg.next    = NULL;
        cur->unack_msg.prev      = NULL;
        cur->unack_msg.next      = NULL;
    }

    /* insert region into list */
    reg->next = vbuf_region_head;
    vbuf_region_head = reg;

    return 0;
}

static vbuf* vbuf_get(struct ibv_pd* pd)
{
    vbuf* v = NULL;

    pthread_spin_lock(&vbuf_lock);

    /* if we don't have any free vufs left, try to allocate more */
    if (ud_vbuf_free_head == NULL) {
        if (vbuf_region_alloc(pd, rdma_vbuf_secondary_pool_size) != 0) {
            SPAWN_ERR("UD VBUF reagion allocation failed. Pool size %d", ud_vbuf_num_allocated);
            spawn_exit(-1);
        }
    }

    /* pick item from head of list */
    /* this correctly handles removing from single entry free list */
    v = ud_vbuf_free_head;
    ud_vbuf_free_head = ud_vbuf_free_head->desc.next;
    ud_vbuf_num_free--;
    ud_vbuf_num_get++;

    v->content_size = 0;
    v->retry_count  = 0;
    v->flags        = 0;

    pthread_spin_unlock(&vbuf_lock);

    return(v);
}

static void vbuf_release(vbuf* v)
{
    /* This message might be in progress. Wait for ib send completion 
     * to release this buffer to avoid reusing buffer */

    /* if send is still in progress (has not been ack'd),
     * just mark vbuf as ready to be freed and return */
    if (v->flags & UD_VBUF_SEND_INPROGRESS) {
        /* TODO: when is this really added back to the free buffer? */

        /* mark vbuf that it's ready to be freed */
        v->flags |= UD_VBUF_FREE_PENIDING;
        return;
    }

    /* note this correctly handles appending to empty free list */
    pthread_spin_lock(&vbuf_lock);

    /* add vbuf to front of UD free list */
    assert(v != ud_vbuf_free_head);
    v->desc.next = ud_vbuf_free_head;
    ud_vbuf_free_head = v;
    ud_vbuf_num_free++;
    ud_vbuf_num_freed++;

    /* clear out fields to prepare vbuf for next use */
    v->content_size = 0;
    v->vc           = NULL;

    /* note this correctly handles appending to empty free list */
    pthread_spin_unlock(&vbuf_lock);

    return;
}

static inline void vbuf_prepare_recv(vbuf* v, unsigned long len)
{
    assert(v != NULL);

    /* describe recv buffer */
    v->desc.sg_entry.addr   = (uint64_t) v->buffer;
    v->desc.sg_entry.length = (uint32_t) len;
    v->desc.sg_entry.lkey   = (uint32_t) v->region->mem_handle->lkey;

    /* put together our work request */

    /* record address of vbuf as a tag */
    v->desc.u.rr.wr_id = (uint64_t) v;

    /* we just have this single request */
    v->desc.u.rr.next = NULL;

    /* our request contains a single scatter/gather entry */
    v->desc.u.rr.sg_list = &(v->desc.sg_entry);
    v->desc.u.rr.num_sge = 1;

    return;
}

static inline void vbuf_prepare_send(vbuf* v, unsigned long len)
{
    /* describe data to be sent */
    v->desc.sg_entry.addr   = (uint64_t) v->buffer;
    v->desc.sg_entry.length = (uint32_t) len;
    v->desc.sg_entry.lkey   = (uint32_t) v->region->mem_handle->lkey;

    /* put together our work request */

    /* record address of vbuf as a tag */
    v->desc.u.sr.wr_id = (uint64_t) v;

    /* we just have this single request */
    v->desc.u.sr.next = NULL;

    /* our request contains a single scatter/gather entry */
    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->desc.u.sr.num_sge = 1;

    /* set up the IB flags */
    v->desc.u.sr.opcode     = IBV_WR_SEND;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;

    return;
}

/*******************************************
 * Communication routines
 ******************************************/

/* this queue tracks a list of pending connect messages,
 * the accept function pulls items from this list */
typedef struct connect_list_t {
    vbuf* v;       /* pointer to vbuf for this message */
    uint64_t epid; /* local endpoint id */
    uint32_t lid;  /* requestor lid */
    uint32_t qpn;  /* requestor queue pair number */
    uint64_t id;   /* requestor write id to use when sending */
    char* name;    /* requestor hostname */
    struct connect_list_t* next; /* pointer to next item in list */
} connect_list;

static connect_list* connect_head = NULL;
static connect_list* connect_tail = NULL;

/* tracks list of accepted connection requests, which is used to
 * filter duplicate connection requests and track active channels */
typedef struct connected_list_t {
    unsigned int lid; /* remote LID */
    unsigned int qpn; /* remote Queue Pair Number */
    unsigned int id;  /* write id to use to send to remote side */
    vc_t*  vc;        /* open vc to remote side */
    struct connected_list_t* next; /* pointer to next item in list */
} connected_list;

static connected_list* connected_head = NULL;
static connected_list* connected_tail = NULL;

/* given a packet, virtual channel, and ud context, append packet
 * to extended send queue for UD context or send it out on wire */
static inline void ibv_ud_post_sr(
    vbuf* v,
    vc_t* vc,
    ud_ctx_t* ud_ctx)
{
    /* get a pointer to send request structure */
    struct ibv_send_wr* sr = &v->desc.u.sr;

    /* set the SEND SIGNALED flag and inline data if we can */
    if(v->desc.sg_entry.length <= rdma_max_inline_size) {
        sr->send_flags = (enum ibv_send_flags) (IBV_SEND_SIGNALED | IBV_SEND_INLINE);
    } else {
        sr->send_flags = IBV_SEND_SIGNALED;
    }

    /* specify iB address handle and remote queue pair number */
    sr->wr.ud.ah = vc->ah;
    sr->wr.ud.remote_qpn = vc->qpn;

    /* place packet on extended queue or send it out */
    if (ud_ctx->send_wqes_avail <= 0 ||
        ud_ctx->ext_send_queue.head != NULL)
    {
        /* out of send WQEs or there is a packet on the extended queue,
         * add this packet to the extended queue */
        ext_sendq_add(&ud_ctx->ext_send_queue, v);
    } else {
        /* we have a WQE and the extended send queue is clear,
         * send the packet on the wire */

        /* one less send WQE available now */
        ud_ctx->send_wqes_avail--;

        /* send the packet */
        int ret = ibv_post_send(ud_ctx->qp, sr, &(v->desc.y.bad_sr));
        if (ret != 0) {
            SPAWN_ERR("failed to send (ibv_post_send rc=%d %s)", ret, strerror(ret));
            exit(-1);
        }
    }

    return;
}

/* submit packet to VC to be sent, will submit to UD context if send
 * window is not full and appends packet to VC extended send queue
 * otherwise */
static int ud_post_send(vc_t* vc, vbuf* v, ud_ctx_t* ud_ctx)
{
    /* check that vbuf is for UD and that data fits within UD packet */
    assert(v->desc.sg_entry.length <= MRAIL_MAX_UD_SIZE);

    /* record pointer to VC in vbuf */
    v->vc = (void *)vc;

    /* write send context into packet header */
    packet_header* p = (packet_header*) v->buffer;
    p->srcid = vc->writeid;

    /* if we have too many outstanding sends, or if we have other items
     * on the extended send queue, insert vbuf in extended send queue
     * to be resent later */
    message_queue_t* sendwin = &vc->send_window;
    message_queue_t* extwin  = &vc->ext_window;
    if (sendwin->count > rdma_default_ud_sendwin_size ||
       (extwin->head != NULL && extwin->head != v))
    {
        ext_window_add(extwin, v);

        return 0;
    }

    /* otherwise, we're ok to send packet now, set send sequence number
     * in vbuf and packet header */
    v->seqnum = vc->seqnum_next_tosend;
    p->seqnum = vc->seqnum_next_tosend;
    vc->seqnum_next_tosend++;

    /* piggy-back ack in this message */
    p->acknum = vc->seqnum_next_toack;
    vc->ack_need_tosend = 0;
    vc->ack_pending = 0;

    /* mark vbuf as send-in-progress */
    v->flags |= UD_VBUF_SEND_INPROGRESS;

    /* submit packet to UD context */
    ibv_ud_post_sr(v, vc, ud_ctx);

    /* record packet and time in our send queue */
    rdma_ud_last_check = spawn_clock_time_us();

    /* TODO: what's this mean? */
    /* don't track messages in this case */
    if (v->in_sendwin) {
        return 0;
    }

    /* TODO: should we set this when packet hits wire instead? */
    /* record time at which packet was submitted to UD context */
    v->timestamp = spawn_clock_time_us();

    /* add vbuf to the send window */
    send_window_add(&(vc->send_window), v);

    /* add vbuf to global unack queue */
    unack_queue_add(&proc.unack_queue, v);

    return 0;
}

/* churn through and send as many as packets as we can from the
 * VC extended send queue */
static inline void ud_flush_ext_window(vc_t *vc)
{
    /* get pointer to ud info, send queue, and extended send queue */
    message_queue_t* sendwin = &vc->send_window;
    message_queue_t* extwin  = &vc->ext_window;

    /* get pointer to head of extended send queue */
    vbuf* cur = extwin->head;
    while (cur != NULL &&
           sendwin->count < rdma_default_ud_sendwin_size)
    {
        /* get pointer to next element in list */
        vbuf* next = cur->extwin_msg.next;

        /* send item, associated vbuf will be released when send
         * completion event is processed */
        ud_post_send(vc, cur, proc.ud_ctx);

        /* remove item from head of list, it's important that we
         * do this *after* step above, because ud_post_send will
         * check that this packet is at head of extended queue */
        extwin->head = next;
        extwin->count--;

        /* track number of sends from extended send queue */
        vc->ext_win_send_count++;

        /* go on to next item */
        cur = next;
    }

    /* update queue fields if we emptied the list */
    if (extwin->head == NULL) {
        extwin->tail = NULL;
        assert(extwin->count == 0);
    }

    return;
}

/* given a VC and a seq number, remove all items in send and unack'd
 * queues up to and including this seq number */
static inline void ud_process_ack(vc_t *vc, uint16_t acknum)
{
    /* get pointer to ud info, send queue, and extended send queue */
    message_queue_t* sendwin = &vc->send_window;
    message_queue_t* extwin  = &vc->ext_window;

    /* while we have a vbuf, and while its seq number is before seq
     * number in ack, remove it from send and unack queues */
    vbuf* cur = sendwin->head;
    while (cur != NULL &&
           INCL_BETWEEN(acknum, cur->seqnum, vc->seqnum_next_tosend))
    {
        /* the current vbuf has been ack'd, so remove it from the send
         * window and also the unack'd list */

        /* remove packet from VC send queue (enables VC to submit more
         * packets to UD context */
        send_window_remove(sendwin, cur);

        /* remove packet from UD context unack'd queue */
        unack_queue_remove(&proc.unack_queue, cur);

        /* release vbuf */
        vbuf_release(cur);

        /* get next packet in send window */
        cur = sendwin->head;
    }

    /* see if we can submit move packets from VC extended send
     * queue to send queue (submitted to UD context) */
    if (extwin->head != NULL &&
        sendwin->count < rdma_default_ud_sendwin_size)
    {
        ud_flush_ext_window(vc);
    }

    return;
}

/* add vbuf to tail of apprecv queue */
static inline void apprecv_window_add(message_queue_t *q, vbuf *v)
{
    /* set next and prev pointers ton vbuf */
    v->apprecvwin_msg.next = NULL;
    v->apprecvwin_msg.prev = NULL;

    /* for empty list, update head, otherwise update next pointer
     * of last item in list to point to vbuf */
    if(q->head == NULL) {
        q->head = v;
    } else {
        (q->tail)->apprecvwin_msg.next = v;
    }

    /* point tail to vbuf and increase count */
    q->tail = v;
    q->count++;

    return;
}

/* remove and return vbuf from apprecv queue */
static inline vbuf* apprecv_window_retrieve_and_remove(message_queue_t *q)
{
    /* get pointer to first item in list */
    vbuf* v = q->head;

    /* return right away if it's empty */
    if (v == NULL) {
        return NULL;
    }

    /* update head to point to next item and decrement length of queue */
    q->head = v->apprecvwin_msg.next;
    q->count--;

    /* if we emptied the list, update the tail */
    if (q->head == NULL ) {
        q->tail = NULL;
        assert(q->count == 0);
    }

    /* clear next pointer in vbuf before return it */
    v->apprecvwin_msg.next = NULL;

    return v;
}

/* returns 1 if a message is in the queue, 0 otherwise */
static inline int apprecv_window_test(message_queue_t* q)
{
    /* get pointer to first item in list */
    vbuf* v = q->head;

    /* return 0 if it's empty and 1 otherwise */
    if (v == NULL) {
        return 0;
    }
    return 1;
}

/* places packet either in app recieve queue or out-of-order
 * receive queue (or discards packet if it's outside the sliding
 * window of sequence numbers) */
static inline void mv2_ud_place_recvwin(vbuf *v)
{
    int ret;

    /* get VC vbuf is for */
    vc_t* vc = v->vc;

    /* determine bounds of sliding recv window */
    int recv_win_start = vc->seqnum_next_torecv;
    int recv_win_end = recv_win_start + rdma_default_ud_recvwin_size;
    while (recv_win_end > MAX_SEQ_NUM) {
        recv_win_end -= MAX_SEQ_NUM;
    }

    /* check if the packet seq num is in the window or not */
    if (INCL_BETWEEN(v->seqnum, recv_win_start, recv_win_end)) {
        /* get pointer to out-of-order recv queue */
        message_queue_t* recvwin = &vc->recv_window;

        /* got a packet within range, now check whether its in order or not */
        if (v->seqnum == vc->seqnum_next_torecv) {
            /* packet is the one we expect, add to tail of VC receive queue */
            apprecv_window_add(&vc->app_recv_window, v);

            /* update our ack seq number to attach to outgoing packets */
            vc->seqnum_next_toack = vc->seqnum_next_torecv;

            /* increment the sequence number we expect to get next */
            vc->seqnum_next_torecv++;

            /* mark VC that we need to send an ack message */
            vc->ack_need_tosend = 1;
        } else {
            /* in this case, the packet does not match the expected
             * sequence number, but it is within the window range,
             * add it to our (out-of-order) receive queue */
            ret = recv_window_add(recvwin, v, vc->seqnum_next_torecv);
            if (ret == MSG_IN_RECVWIN) {
                /* release buffer if it is already in queue */
                v->content_size = 0;
                vbuf_release(v);
            }

            /* mark VC that we need to send an ack message,
             * note that we do not update the value of the
             * sequence number that we'll ack here */
            vc->ack_need_tosend = 1;
        }

        /* if we have items at front of (out-of-order) receive queue
         * whose seq num matches expected seq num, extract them from
         * out-of-order recv queue and add them to app recv queue */
        while (recvwin->head != NULL && 
               recvwin->head->seqnum == vc->seqnum_next_torecv)
        {
            /* move item to VC apprecv queue */
            apprecv_window_add(&vc->app_recv_window, recvwin->head);

            /* remove item from head of out-of-order recv queue */
            recv_window_remove(recvwin);

            /* update our ack seq number to attach to outgoing packets */
            vc->seqnum_next_toack = vc->seqnum_next_torecv;

            /* increment the sequence number we expect to get next */
            vc->seqnum_next_torecv++;
        }
    } else {
        /* we got a packet that is not within the receive window,
         * just throw it away */
        v->content_size = 0;
        vbuf_release(v);

        /* the most likely cause for this is that we got a duplicate
         * because the sender hasn't gotten an ack from us yet, so
         * we'll force an ack to clear this up */
        vc->ack_need_tosend = 1;
    }

    return;
}

/*******************************************
 * Functions to manage flow
 ******************************************/

/* send control message with ack update */
static void ud_send_ack(vc_t *vc)
{
    /* get a vbuf to build our packet */
    vbuf* v = vbuf_get(g_hca_info.pd);

    /* record pointer to VC in vbuf */
    v->vc = (void*)vc;

    /* prepare vbuf for sending */
    unsigned long size = sizeof(packet_header);
    vbuf_prepare_send(v, size);

    /* get pointer to packet header */
    packet_header* p = (packet_header*) v->buffer;
    memset((void*)p, 0xfc, sizeof(packet_header));

    /* write type and send context into packet header */
    p->type  = PKT_UD_ACK;
    p->srcid = vc->writeid;

    /* control messages don't have a seq numer */
    v->seqnum = -1;
    p->seqnum = -1;

    /* fill in ACK info */
    p->acknum = vc->seqnum_next_toack;
    vc->ack_need_tosend = 0;
    vc->ack_pending = 0;

    /* get pointer to UD context */
    ud_ctx_t* ud_ctx = proc.ud_ctx;

    /* submit packet to UD context */
    ibv_ud_post_sr(v, vc, ud_ctx);

    /* keep track of total number of control messages sent */
    vc->cntl_acks++;

    return;
}

/* iterate over all active vc's and send ACK messages if necessary */
static inline void ud_check_acks()
{
    /* walk list of connected virtual channels */
    connected_list* elem = connected_head;
    while (elem != NULL) {
        /* get pointer to vc */
        vc_t* vc = elem->vc;

        /* send ack if necessary */
        if (vc->ack_need_tosend) {
            ud_send_ack(vc);
        }

        /* go to next virtual channel */
        elem = elem->next;
    }

    return;
}

/* discard send packet (retries exhausted) */
static void ud_toss_packet(vbuf* v)
{
    /* remove this from VC send queue */
    vc_t* vc = v->vc;
    if (vc != NULL) {
        message_queue_t* sendwin = &vc->send_window;
        send_window_remove(sendwin, v);
    }

    /* drop packet from unack queue */
    unack_queue_remove(&proc.unack_queue, v);

    /* release vbuf */
    vbuf_release(v);

    return;
}

/* resend specified packet and update its retry state */
static void ud_resend(vbuf *v)
{
    /* if vbuf is marked as send-in-progress, don't send again,
     * unless "always retry" flag is set */
    if (v->flags & UD_VBUF_SEND_INPROGRESS && 
        ! (v->flags & UD_VBUF_RETRY_ALWAYS))
    {
        return;
    }

    /* increment our retry count */
    v->retry_count++;

    /* get packet header (since this is a send packet,
     * the header is at the start of the buffer) */
    packet_header* p = (packet_header*) v->buffer;

    /* Disconnecting an unreliable connection is equivalent
     * to the Two Generals problem, which is unsolvable, meaning
     * there is no finite number of acks we can send before
     * we're sure that it's safe to tear down the connection.
     * In a mostly reliable network, the most likely cause to
     * exceed our retries is if we fail to get an ack for our
     * final DISCONNECT packet (because the remote end got it
     * sent an ack and tore down, but the ack failed to reach us.) */

    /* we throw away disconnect packets earlier than normal
     * packets */
    if (p->type == PKT_UD_DISCONNECT && v->retry_count > 5) {
        ud_toss_packet(v);
        return;
    }

    /* if we exceed our limit, just toss the packet,
     * in a true lossy environment, this will cause the app
     * to hang, so print a message to warn user */
    if (v->retry_count > rdma_ud_max_retry_count) {
        SPAWN_ERR("Tossing normal packet, job will hang\n");
        ud_toss_packet(v);
        return;
    }

    /* get VC associated with packet */
    vc_t* vc = v->vc;

    /* piggy-back ack on message and mark VC as ack completed */
    p->acknum = vc->seqnum_next_toack;
    vc->ack_need_tosend = 0;

    /* TODO: why not set ack_pending to 0 here? */

    /* mark vbuf as send-in-progress */
    v->flags |= UD_VBUF_SEND_INPROGRESS;

    /* get pointer to UD context */
    ud_ctx_t* ud_ctx = proc.ud_ctx;

    /* send vbuf (or add to extended UD send queue if we don't have credits) */
    if (ud_ctx->send_wqes_avail > 0) {
//SPAWN_ERR("resending payload of size %d at %p\n", v->content_size, v);
        ud_ctx->send_wqes_avail--;
        int ret = ibv_post_send(ud_ctx->qp, &(v->desc.u.sr), &(v->desc.y.bad_sr));
        if (ret != 0) {
            SPAWN_ERR("reliability resend failed (ibv_post_send rc=%d %s)", ret, strerror(ret));
            exit(-1);
        }
    } else {
        ext_sendq_add(&ud_ctx->ext_send_queue, v);
    }

    /* increment our total resend count */
    vc->resend_count++;

    return;
}

/* iterates over all items on unack queue, checks time since last send,
 * and resends if timer has expired */
static int ud_check_resend()
{
    /* get pointer to unack queue */
    message_queue_t* q = &proc.unack_queue;

    /* if our queue is empty, return 1 */
    if (q->head == NULL) {
        return 1;
    }

    /* get current time */
    double timestamp = spawn_clock_time_us();

    /* walk through unack'd list */
    vbuf* cur = q->head;
    while (cur != NULL) {
        //TODO:: if (cur->left_to_send == 0 || cur->retry_always)

        /* get log of retry count for this packet */
        int r;
        if (cur->retry_count > 1) {
            LOG2(cur->retry_count, r);
        } else {
            r = 1;
        }

        /* compute time this packet has been waiting since we
         * last sent (or resent) it */
        long delay = timestamp - cur->timestamp;
        long waittime = rdma_ud_min_retry_timeout * r;
        if (delay > waittime || delay > rdma_ud_max_retry_timeout) {
            /* we've waited long enough, try again and update
             * its send timestamp */
            ud_resend(cur);
            cur->timestamp = timestamp;

            /* since this may have taken some time, update our current
             * timestamp */
            timestamp = spawn_clock_time_us();
        }

        /* go on to next item in list */
        cur = cur->unack_msg.next;
    }

    /* return 0 to indicate we have at least one item on the queue */
    return 0;
}

static void ud_process_recv(vbuf *v) 
{
    /* TODO: consider sending immedate ack with each receive,
     * trades bandwidth for latency */

    /* get VC of vbuf */
    vc_t* vc = v->vc;

    /* get pointer to packet header */
    packet_header* p = (packet_header*) v->content_buf;

    /* read ack seq number from incoming packet and clear packets
     * up to and including this number from send and unack'd queues */
    ud_process_ack(vc, p->acknum);

    /* check for control message */
    if (p->type & PKT_CONTROL_BIT) {
        /* if we got a disconnect, mark remote side as closed,
         * once all of our outgoing packets are acked and the local
         * side also calls disconnect, we can free the vc */
        if (p->type == PKT_UD_DISCONNECT) {
            /* free the vbuf */
            vbuf_release(v);

            /* record that remote side has disconnected */
            vc->remote_closed = 1;

            /* decrement the number of active connections using the endpoint */
            g_count_conn--;

            /* free the VC if we can */
            vc_free(vc);

            goto fn_exit;
        }

        /* set flag to shutdown if we received a shutdown message
         * (from ourself) */
        if (p->type == PKT_UD_SHUTDOWN) {
            g_shutdown = 1;
        }

        /* no need to send ack or add packet to receive queues,
         * except that we do process ACCEPT messages */
        if (p->type == PKT_UD_ACCEPT) {
            /* don't ACK accept messages, but we do add these
             * messages to the receive queue */

            /* we don't ACK an accept message, because it hasn't been
             * processed yet, so that we have stored the write id
             * to use in the ACK message */

            /* TODO: we could ACK accept messages, but we need to
             * process it here and record the writeid on the vc */
            mv2_ud_place_recvwin(v); 
        } else {
            /* no need to send ack or add packet to receive queues */
            vbuf_release(v);
        }

        /* skip the ACK logic */
        goto fn_exit;
    }

    /* send an explicit ack if we've exceeded our pending ack count */
    vc->ack_pending++;
    if (vc->ack_pending > rdma_ud_max_ack_pending) {
        ud_send_ack(vc);
    }

    /* insert packet in receive queues (or throw it away if seq num
     * is out of current range) */
    mv2_ud_place_recvwin(v); 

fn_exit:
    return;
}

/* given a connection request packet, append entry to our queue
 * of connection requests */
static void ud_process_connreq(vbuf* v, const struct ibv_wc* wc)
{
    /* get pointer to payload in vbuf */
    size_t header_size = sizeof(packet_header);
    char* connect_payload = v->content_buf + header_size;

//printf("req: %s\n", connect_payload);
    /* make a copy of name that we can modify */
    char* name_copy = SPAWN_STRDUP(connect_payload);
    char* ptr = name_copy;

    /* pick out length of remote hostname */
    char* host_len_str = ptr;
    while (*ptr != ':') {
        ptr++;
    }
    *ptr = '\0';
    ptr++;

    /* set remote hostname and skip to IBUD address */
    char* host_str = ptr;
    int host_len = atoi(host_len_str);
    ptr += host_len;
    *ptr = '\0';
    ptr++;

    /* extract lid, queue pair number, and endpoint id from
     * endpoint name */
    unsigned int epid, writeid, lid, qpn;
    int parsed = sscanf(ptr, "%06x:%06x:%04x:%06x", &epid, &writeid, &lid, &qpn);
    if (parsed != 4) {
        SPAWN_ERR("Couldn't parse ep info from %s", connect_payload);
        spawn_free(&name_copy);
        vbuf_release(v);
        return;
    }

    /* TODO: read lid/qpn from vbuf and not payload to avoid
     * spoofing */

    /* check whether we already have duplicate requests in the
     * connection request queue */
    connect_list* req_elem = connect_head;
    while (req_elem != NULL) {
        /* check whether this connect request matches one
         * already in the list */
        if (req_elem->lid == lid &&
            req_elem->qpn == qpn &&
            req_elem->id  == writeid)
        {
            /* we've already got an entry in the connection request
             * queue, this is a duplicate, so just pitch it */
            spawn_free(&name_copy);
            vbuf_release(v);
            return;
        }

        /* no match so far, try the next item */
        req_elem = req_elem->next;
    }

    /* check that we don't already have an existing connection
     * that matches the remote info for this request */
    connected_list* elem = connected_head;
    while (elem != NULL) {
        /* check whether this connect request matches one we're
         * already connected to */
        if (elem->lid == lid &&
            elem->qpn == qpn &&
            elem->id  == writeid)
        {
            /* we're already connected to this process,
             * drop this request */
            spawn_free(&name_copy);
            vbuf_release(v);
            return;
        }

        /* no match so far, try the next item in connected list */
        elem = elem->next;
    }

    /* TODO: we should put a time out on connection request
     * entries once added to the queue so that we can delete
     * them, this matters because we may accept a connection
     * and then get several more requests before the remote
     * end learns that we have accepted. */

    /* allocate and initialize new element for connect queue */
    connect_list* req = (connect_list*) SPAWN_MALLOC(sizeof(connect_list));
    req->v    = v;          /* record pointer to vbuf */
    req->epid = epid;       /* local endpoint id request is trying to connect to */
    req->lid  = wc->slid;   /* requestor lid */
    req->qpn  = wc->src_qp; /* requestor qpn */
    req->id   = writeid;    /* requestor write id to use to send to remote process */
    req->name = SPAWN_STRDUP(host_str); /* record remote hostname */
    req->next = NULL;

    /* append req to connect queue */
    if (connect_head == NULL) {
        connect_head = req;
    }
    if (connect_tail != NULL) {
        connect_tail->next = req;
    }
    connect_tail = req;

    /* free our copy of the packet payload */
    spawn_free(&name_copy);

    /* TODO: better to free the vbuf here? */

    /* if we append to the queue, we don't free the vbuf,
     * we've got a pointer to it, and we'll free it when
     * we process the request */

    /* TODO: send ack back to requestor to prevent a flood
     * of requests coming in if we're slow to accept */

    return;
}

static int ud_post_recv_buffers(int num_bufs, ud_ctx_t *ud_ctx)
{
    /* TODO: post buffers as a linked list to be more efficient? */

//    long start = spawn_clock_time_us();

#if 0
    /* post receives one at a time */

    /* post our vbufs */
    int count = 0;
    while (count < num_bufs) {
        /* get a new vbuf */
        vbuf* v = vbuf_get(g_hca_info.pd);
        if (v == NULL) {
            break;
        }

        /* initialize vubf for UD */
        vbuf_prepare_recv(v, rdma_default_ud_mtu);

        /* post vbuf to receive queue */
        struct ibv_recv_wr* bad_wr;
        if (ud_ctx->qp->srq) {
            int ret = ibv_post_srq_recv(ud_ctx->qp->srq, &v->desc.u.rr, &bad_wr);
            if (ret != 0) {
                vbuf_release(v);
                SPAWN_ERR("Failed to post receive work requests (ibv_post_srq_recv rc=%d %s)", ret, strerror(ret));
                _exit(EXIT_FAILURE);
            }
        } else {
            int ret = ibv_post_recv(ud_ctx->qp, &v->desc.u.rr, &bad_wr);
            if (ret != 0) {
                vbuf_release(v);
                SPAWN_ERR("Failed to post receive work requests (ibv_post_recv rc=%d %s)", ret, strerror(ret));
                _exit(EXIT_FAILURE);
            }
        }

        /* prepare next recv */
        count++;
    }

#else
    /* post receives in batch as linked list */

    /* we submit the work requests as a linked list */
    struct ibv_recv_wr* head = NULL;
    struct ibv_recv_wr* tail = NULL;

    /* post our vbufs */
    int count = 0;
    while (count < num_bufs) {
        /* get a new vbuf */
        vbuf* v = vbuf_get(g_hca_info.pd);
        if (v == NULL) {
            break;
        }

        /* initialize vubf for UD */
        vbuf_prepare_recv(v, rdma_default_ud_mtu);

        /* get pointer to receive work request */
        struct ibv_recv_wr* cur =  &v->desc.u.rr;
        cur->next = NULL;

        /* link request into chain */
        if (head == NULL) {
            head = cur;
        }
        if (tail != NULL) {
            tail->next = cur;
        }
        tail = cur;

        /* prepare next recv */
        count++;
    }

    /* post vbuf to receive queue */
    if (head != NULL) {
        struct ibv_recv_wr* bad_wr;
        if (ud_ctx->qp->srq) {
            int ret = ibv_post_srq_recv(ud_ctx->qp->srq, head, &bad_wr);
            if (ret != 0) {
                SPAWN_ERR("Failed to post receive work requests (ibv_post_srq_recv rc=%d %s)", ret, strerror(ret));
                _exit(EXIT_FAILURE);
            }
        } else {
            int ret = ibv_post_recv(ud_ctx->qp, head, &bad_wr);
            if (ret != 0) {
                SPAWN_ERR("Failed to post receive work requests (ibv_post_recv rc=%d %s)", ret, strerror(ret));
                _exit(EXIT_FAILURE);
            }
        }
    }
#endif

//    long end = spawn_clock_time_us();
//    printf("Posted %d bufs in %lu usecs\n", num_bufs, (end - start));

    return count;
}

/* when a send work element completes, issue another */
static void ud_update_send_credits(int num)
{
    /* increment number of available send work queue elements */
    ud_ctx_t* ud_ctx = proc.ud_ctx;
    ud_ctx->send_wqes_avail += num;

    /* get pointer to UD context extended send queue */
    message_queue_t* q = &ud_ctx->ext_send_queue;

    /* while we have slots available in the send queue and items on
     * the UD context extended send queue, send them */
    vbuf* cur = q->head;
    while (cur != NULL && ud_ctx->send_wqes_avail > 0) {
        /* get pointer to next item */
        vbuf* next = cur->desc.next;

        /* remove item from extended send queue */
        q->head = next;
        if (q->head == NULL) {
            q->tail = NULL;
        }
        q->count--;

        /* sever item from list */
        cur->desc.next = NULL;

        /* TODO: can we reset ack to latest? */
        /* send item */
        ud_ctx->send_wqes_avail--;
        int ret = ibv_post_send(ud_ctx->qp, &(cur->desc.u.sr), &(cur->desc.y.bad_sr));
        if (ret != 0) {
            SPAWN_ERR("extend sendq send failed (ibv_post_send rc=%d %s)", ret, strerror(ret));
            exit(-1);
        }

        /* track number of sends from extended queue */
        ud_ctx->ext_sendq_count++;

        /* go on to next item in queue */
        cur = next;
    }

    return;
}

/* TODO: dedicate a thread to receive packets and append to queues,
 * signal main thread that a new packet is ready */

/* polls completion queue and handles any new items,
 * either receiving messages and appending them to receive queue
 * or adding connection requests to connect queue */
static int cq_poll(int* got_recv)
{
    /* get pointer to completion queue */
    struct ibv_cq* cq = g_hca_info.cq_hndl;

    /* poll cq */
    struct ibv_wc wcs[64];
    int ne = ibv_poll_cq(cq, 64, wcs);

    /* check that we didn't get an error polling */
    if (ne < 0) {
        SPAWN_ERR("poll cq error (ibv_poll_cq rc=%d)", ne);
        exit(-1);
    }

    /* count number of completed sends */
    int sendcnt = 0;

    /* count number of new messages */
    int recvcnt = 0;

    /* process entries if we got any */
    int i;
    for (i = 0; i < ne; i++) {
        /* get pointer to next entry */
        struct ibv_wc* wc = &wcs[i];

        /* first, check that entry was successful */
        if (IBV_WC_SUCCESS != wc->status) {
            SPAWN_ERR("IBV_WC_SUCCESS != wc.status (%d)", wc->status);
            exit(-1);
        }

        /* get vbuf associated with this work request */
        vbuf* v = (vbuf *) ((uintptr_t) wc->wr_id);

        packet_header* p;
        switch (wc->opcode) {
            case IBV_WC_SEND:
                /* remember that a send completed to issue more sends later */
                sendcnt++;

                /* if SEND_INPROGRESS and FREE_PENDING flags are set,
                 * release the vbuf */
                if (v->flags & UD_VBUF_SEND_INPROGRESS) {
                    v->flags &= ~(UD_VBUF_SEND_INPROGRESS);

                    if (v->flags & UD_VBUF_FREE_PENIDING) {
                        v->flags &= ~(UD_VBUF_FREE_PENIDING);

                        vbuf_release(v);
                    }
                }
    
                v = NULL;
                break;
            case IBV_WC_RECV:
                /* IBUD reserves the first 40 bytes of the user buffer
                 * for the Global Routing Header (GRH) if one exists */
                v->content_buf  = v->buffer + MV2_UD_GRH_LEN;
                v->content_size = wc->byte_len - MV2_UD_GRH_LEN;

                /* get pointer to packet header in vbuf */
                p = (packet_header*) v->content_buf;

                if (p->type != PKT_UD_CONNECT) {
                    /* this is not a connect msg, so source id is valid,
                     * we use the src id to lookup vc */
                    uint64_t index = p->srcid;

                    /* check that the vc index is within range */
                    if (index >= g_ud_vc_info_id) {
                        SPAWN_ERR("Packet conext invalid %d", index);
                        vbuf_release(v);
                        v = NULL;
                        break;
                    }

                    /* get pointer to vc */
                    vc_t* vc = g_ud_vc_info[index];

                    /* for UD packets, check that source lid and source
                     * qpn match expected vc to avoid spoofing */
                    if (vc->lid != wc->slid ||
                        vc->qpn != wc->src_qp)
                    {
                        SPAWN_ERR("Packet source lid/qpn do not match expected values");
                        vbuf_release(v);
                        v = NULL;
                        break;
                    }

                    /* Note that we don't increase the receive count
                     * for invalid packets that we throw away.  We
                     * are triggering receive events for control
                     * packets.  An optimization would be to ignore
                     * control packets, as well. */

                    /* increment our receive count */
                    recvcnt++;

                    v->vc     = vc;
                    v->seqnum = p->seqnum;

                    ud_process_recv(v);
                } else {
                    /* a connect message does not have a valid src id field,
                     * so we can't associate msg with a vc yet, we stick this
                     * on the queue that accept looks to later */
                    ud_process_connreq(v, wc);

                    /* For now, we also count a connect message as a
                     * receive.  An optimization would be count
                     * connect messages separately. */

                    /* increment our receive count */
                    recvcnt++;
                }

                /* decrement the count of number of posted receives,
                 * and if we fall below the low-water limit, post more */ 
                proc.ud_ctx->num_recvs_posted--;
                if (proc.ud_ctx->num_recvs_posted < proc.ud_ctx->credit_preserve) {
                    int remaining = rdma_default_max_ud_recv_wqe - proc.ud_ctx->num_recvs_posted;
                    int posted = ud_post_recv_buffers(remaining, proc.ud_ctx);
                    proc.ud_ctx->num_recvs_posted += posted;
                }
                break;
            default:
                SPAWN_ERR("Invalid opcode from ibv_poll_cq: %d", wc->opcode);
                break;
        }
    }

    /* if sends completed, issue pending sends if we have any */
    if (sendcnt > 0) {
        ud_update_send_credits(sendcnt);
    }

    /* indicate to caller whether a new packet was received,
     * we use this to signal the main thread if it's waiting */
    *got_recv = 0;
    if (recvcnt > 0) {
        *got_recv = 1;
    }

    return ne;
}

/* empty all events from completion queue queue */
static inline void cq_drain()
{
    int new_recv = 0;
    int got_recv;
    int rc = cq_poll(&got_recv);
    while (rc > 0) {
        new_recv |= got_recv;
        rc = cq_poll(&got_recv);
    }

    /* if we're using a receive thread, and if we got a new message,
     * then signal the main thread if it's waiting */
    if (! g_recv_busy_spin) {
        if (new_recv && g_recv_flag) {
            g_recv_flag = 0;
            pthread_cond_signal(&g_recv_cond);
        }
    }

    return;
}

/*******************************************
 * UD Progress thread
 ******************************************/

/* this is the function executed by the communication progress thread */
static void* timeout_thread_fn(void *arg)
{
    /* define sleep time between waking and checking for events */
    cm_timeout.tv_sec = rdma_ud_progress_timeout / 1000000;
    cm_timeout.tv_nsec = (rdma_ud_progress_timeout - cm_timeout.tv_sec * 1000000) * 1000;

    /* we'll start a timer when we see the force_shutdown flag set,
     * and we'll bail out when all packets have been acked or the
     * shutdown timer expires, which ever is first */
    int shutdown_timer = 0;
    double shutdown_timeout = (double) (rdma_ud_progress_timeout * 10);
    double end;

    while(1) {
        /* sleep for some time before we look, release lock while
         * sleeping */
        //comm_unlock();
        nanosleep(&cm_timeout, &cm_remain);
        //comm_lock();

        /* resend messages and send acks if we're due */
//        long time = spawn_clock_time_us();
//        long delay = time - rdma_ud_last_check;
//        if (delay > rdma_ud_progress_timeout) {
            /* time is up, grab lock and process acks */
            comm_lock();

            /* send explicit acks out on all vc's if we need to,
             * this ensures acks flow out even if main thread is
             * busy doing other work */
            ud_check_acks();

            /* process any messages that may have come in, we may
             * clear messages we'd otherwise try to resend below */
            cq_drain();

            /* resend any unack'd packets whose timeout has expired */
            int unack_empty = ud_check_resend();

            /* done sending messages, release lock */
            comm_unlock();

            /* if spawn_net_close has been called:
             *   if unack queue is empty, break
             *   otherwise if timer is not set, set timer
             *   otherwise if timer has expired, break */
            if (force_shutdown) {
                if (unack_empty) {
                    /* all sent packets have been acked,
                     * and since we're in close no more
                     * will be sent, we can stop now */
                    break;
                } else if (! shutdown_timer) {
                    /* start the timer */
                    shutdown_timer = 1;
                    end = spawn_clock_time_us();
                    end += shutdown_timeout;
                } else {
                    /* break if the timer has expired */
                    double now = spawn_clock_time_us();
                    if (now > end) {
                        break;
                    }
                }
            }

            /* record the last time we checked acks */
//            rdma_ud_last_check = spawn_clock_time_us();
//        }
    }

    comm_lock();

    /* process any last acks that came in */
    cq_drain();

    /* send acks several times, in a last attempt */
    ud_check_acks();

    /* TODO: throw out all packets on unack queue */

    comm_unlock();

    return NULL;
}

/* this is the function executed by the communication progress thread */
static void* recv_thread_fn(void *arg)
{
    /* get pointer to our HCA info structure */
    mv2_hca_info_t* hca_info = &g_hca_info;

    /* grab the lock so we own it when we specify that we should
     * be notified for any CQ event */
    comm_lock();

    /* loop forever responding to CQ events */
    while(1) {
        /* TODO: do we want to set the solicited only bit? */

        /* indicate to completion queue that we want to be notified
         * of new events */
        int solicited_only = 0;
        int rc = ibv_req_notify_cq(hca_info->cq_hndl, solicited_only);
        if (rc != 0) {
            SPAWN_ERR("failed to request notification for cq (ibv_req_notify_cq rc=%d %s)", rc, strerror(rc));
            exit(-1);
        }

        /* release the lock before we starting waiting */
        comm_unlock();

        /* wait for an event */
        struct ibv_cq* cq;
        void* context;
        rc = ibv_get_cq_event(hca_info->comp_channel, &cq, &context);
        if (rc != 0) {
            SPAWN_ERR("failed to wait for event (ibv_get_cq_event errno=%d %s)", errno, strerror(errno));
            exit(-1);
        }

        /* grab the lock again */
        comm_lock();

        /* TODO: we need to be sure to ack all events before
         * the CQ can be destroyed */

        /* TODO: batch up multiple events and ack at same time */

        /* acknowledge the event */
        ibv_ack_cq_events(cq, 1);

        /* Note that even though we got an event that something is
         * ready, the CQ may already be empty because the timeout
         * thread may have already drained it before we can get
         * the lock */

        /* grab the lock, drain the cq, and release the lock */
        cq_drain();

        /* if the shutdown signal is set, bail out */
        if (g_shutdown) {
            break;
        }
    }

    /* release the lock before we exit */
    comm_unlock();

    return NULL;
}

/*******************************************
 * Functions to send / recv packets
 ******************************************/

/* given a virtual channel, a packet type, and payload, construct and
 * send UD packet */
static inline int packet_send(
    vc_t* vc,
    uint8_t type,
    const void* payload,
    size_t payload_size)
{
    /* grab a packet */
    vbuf* v = vbuf_get(g_hca_info.pd);
    if (v == NULL) {
        SPAWN_ERR("Failed to get vbuf");
        return SPAWN_FAILURE;
    }

    /* compute size of packet header */
    size_t header_size = sizeof(packet_header);

    /* check that we have space for payload */
    assert((MRAIL_MAX_UD_SIZE - header_size) >= payload_size);

    /* set packet header fields */
    packet_header* p = (packet_header*) v->buffer;
    memset((void*)p, 0xfc, sizeof(packet_header));
    p->type = type;

    /* copy in payload */
    if (payload_size > 0) {
        char* ptr = v->buffer + header_size;
        memcpy(ptr, payload, payload_size);
    }

    /* set packet size */
    v->content_size = header_size + payload_size;

    /* prepare packet for send */
    vbuf_prepare_send(v, v->content_size);

    /* and send it */
    ud_post_send(vc, v, proc.ud_ctx);

    return SPAWN_SUCCESS;
}

/* blocks until packet comes in on specified VC,
 * returns a pointer to the packet */
static vbuf* packet_wait(vc_t* vc)
{
    /* if we don't have a receive thread,
     * eagerly pull all events from completion queue */
    if (g_recv_busy_spin) {
        cq_drain();
    }

    /* get pointer to recv queue */
    message_queue_t* q = &vc->app_recv_window;

    /* wait until we have an entry */
    while (q->head == NULL) {
        /* determine whether we should poll or wait for a new message */
        if (g_recv_busy_spin) {
            /* polling, release the lock for some time to let
             * other threads make progress */
            comm_unlock();
//            nanosleep(&cm_timeout, &cm_remain);
            comm_lock();

            /* eagerly pull all events from completion queue */
            cq_drain();
        } else {
            /* nothing ready on our receive queue,
             * wait to be signaled for a new message */
            g_recv_flag = 1;
            pthread_cond_wait(&g_recv_cond, &comm_lock_object);
        }
    }

    /* we've got a vbuf on the recv queue once we get here */
    vbuf* v = q->head;

    return v;
}

static connect_list* scan_connect_message(uint64_t epid)
{
    /* walk connect list looking for matching request */
    connect_list* elem = connect_head;
    while (elem != NULL) {
        /* stop walking the list if we find a matching element */
        if (elem->epid == epid) {
            break;
        }

        /* otherwise, look at the next element */
        elem = elem->next;
    }

    return elem;
}

/* blocks until element arrives on connect queue,
 * extracts element and returns its vbuf */
static connect_list* recv_connect_message(uint64_t epid)
{
    /* if we don't have a receive thread,
     * eagerly pull all events from completion queue */
    if (g_recv_busy_spin) {
        cq_drain();
    }

    /* wait until we find a matching item in the connect queue */
    connect_list* prev = NULL;
    connect_list* elem;
    while (1) {
        /* walk connect list looking for matching request */
        elem = connect_head;
        while (elem != NULL) {
            /* stop walking the list if we find a matching element */
            if (elem->epid == epid) {
                break;
            }

            /* otherwise, look at the next element */
            prev = elem;
            elem = elem->next;
        }

        /* if we found something, break the outer loop */
        if (elem != NULL) {
            break;
        }

        /* determine whether we should poll or wait for a new message */
        if (g_recv_busy_spin) {
            /* polling, release the lock for some time to let
             * other threads make progress */
            comm_unlock();
//            nanosleep(&cm_timeout, &cm_remain);
            comm_lock();

            /* eagerly pull all events from completion queue */
            cq_drain();
        } else {
            /* nothing ready on our connect queue,
             * wait to be signaled for a new message */
            g_recv_flag = 1;
            pthread_cond_wait(&g_recv_cond, &comm_lock_object);
        }
    }

    /* extract element from queue */
    if (prev != NULL) {
      /* update previous item to point to item past this one */
      prev->next = elem->next;
    } else {
      /* there is no item in front of this one,
       * update the head to point to the next item */
      connect_head = elem->next;
    }

    /* if item is last on the list, update tail to point
     * to previous item, which is either real or NULL */
    if (elem->next == NULL) {
        connect_tail = prev;
    }

    /* return item */
    return elem;
}

/* after sending a connect packet, we'll wait to receive an incoming
 * accept packet, which is sent when the remote end calls accept,
 * this packet includes the writeid we should use when sending packets
 * on this VC */
static int recv_accept_message(vc_t* vc)
{
    /* first incoming packet should be accept */
    packet_wait(vc);

    /* extract packet from queue */
    vbuf* v = apprecv_window_retrieve_and_remove(&vc->app_recv_window);

    /* message payload is write id we should use when sending */
    size_t header_size = sizeof(packet_header);
    char* payload = v->content_buf + header_size;

    /* extract write id from payload */
    int id;
    int parsed = sscanf(payload, "%06x", &id);
    if (parsed != 1) {
        SPAWN_ERR("Couldn't parse write id from accept message");
        vbuf_release(v);
        return SPAWN_FAILURE;
    }

    /* TODO: avoid casting up from int here */
    /* set our write id */
    uint64_t writeid = (uint64_t) id;
    vc->writeid = writeid;

    /* put vbuf back on free list */
    vbuf_release(v);

    return SPAWN_SUCCESS;
}

/*******************************************
 * Functions to setup / tear down UD QP
 ******************************************/

/* Get HCA parameters */
static int hca_open(int devnum, mv2_hca_info_t* hca_info)
{
    int i;

    /* we need IB routines to still work after launcher forks children */
    int fork_rc = ibv_fork_init();
    if (fork_rc != 0) {
        SPAWN_ERR("Failed to prepare IB for fork (ibv_fork_init errno=%d %s)",
                    fork_rc, strerror(fork_rc));
        return -1;
    }

    /* get list of HCA devices */
    int num_devices;
    struct ibv_device** dev_list = ibv_get_device_list(&num_devices);
    if (dev_list == NULL) {
        SPAWN_ERR("Failed to get device list (ibv_get_device_list errno=%d %s)",
                    errno, strerror(errno));
        return -1;
    }

    /* check that caller's requested device is within range */
    if (devnum >= num_devices) {
        SPAWN_ERR("Requested device number %d higher than max devices %d",
                    devnum, num_devices);
        ibv_free_device_list(dev_list);
        return -1;
    }

    /* pick out device specified by caller */
    struct ibv_device* dev = dev_list[devnum];

    /* Open the HCA for communication */
    struct ibv_context* context = ibv_open_device(dev);
    if (context == NULL) {
        /* TODO: man page doesn't say anything about errno */
        SPAWN_ERR("Cannot create context for HCA");
        ibv_free_device_list(dev_list);
        return -1;
    }

    /* Create a protection domain for communication */
    struct ibv_pd* pd = ibv_alloc_pd(context);
    if (pd == NULL) {
        /* TODO: man page doesn't say anything about errno */
        SPAWN_ERR("Cannot create PD for HCA");
        ibv_close_device(context);
        ibv_free_device_list(dev_list);
        return -1;
    }
    
    /* Get the attributes of the HCA */
    struct ibv_device_attr attr;
    int retval = ibv_query_device(context, &attr);
    if (retval) {
        SPAWN_ERR("Cannot query HCA (ibv_query_device errno=%d %s)", retval,
                    strerror(retval));
        ibv_dealloc_pd(pd);
        ibv_close_device(context);
        ibv_free_device_list(dev_list);
        return -1;
    }

    /* determine number of ports to query */
    int num_ports = attr.phys_port_cnt;
    if (num_ports > MAX_NUM_PORTS) {
        num_ports = MAX_NUM_PORTS;
    }

    /* allocate space to query each port */
    struct ibv_port_attr* ports = (struct ibv_port_attr*) SPAWN_MALLOC(num_ports * sizeof(struct ibv_port_attr));

    /* Get the attributes of the port */
    for (i = 0; i < num_ports; ++i) {
        retval = ibv_query_port(context, i+1, &ports[i]);
        if (retval != 0) {
            SPAWN_ERR("Failed to query port (ibv_query_port errno=%d %s)", retval, strerror(retval));
        }
    }
    
    /* Create completion channel */
    struct ibv_comp_channel* channel = ibv_create_comp_channel(context);
    if (channel == NULL) {
        /* TODO: man page doesn't say anything about errno */
        SPAWN_ERR("Cannot create completion channel");
        spawn_free(&ports);
        ibv_dealloc_pd(pd);
        ibv_close_device(context);
        ibv_free_device_list(dev_list);
        return -1;
    }

    /* Create completion queue */
    struct ibv_cq* cq = ibv_create_cq(
        context, RDMA_DEFAULT_MAX_CQ_SIZE, NULL, channel, 0
    );
    if (cq == NULL) {
        /* TODO: man page doesn't say anything about errno */
        SPAWN_ERR("Cannot create completion queue");
        ibv_destroy_comp_channel(channel);
        spawn_free(&ports);
        ibv_dealloc_pd(pd);
        ibv_close_device(context);
        ibv_free_device_list(dev_list);
        return -1;
    }

    /* copy values into output struct */
    hca_info->pd           = pd;
    hca_info->device       = dev;
    hca_info->context      = context;
    hca_info->cq_hndl      = cq;
    hca_info->comp_channel = channel;
    for (i = 0; i < num_ports; ++i) {
        memcpy(&(hca_info->port_attr[i]), &ports[i], sizeof(struct ibv_port_attr));
    }
    memcpy(&hca_info->device_attr, &attr, sizeof(struct ibv_device_attr));

    /* free temporary objects */
    spawn_free(&ports);
    ibv_free_device_list(dev_list);

    return 0;
}

/* Transition UD QP */
static int qp_transition(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;

    /* Init QP */
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state   = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num   = RDMA_DEFAULT_PORT;
    attr.qkey       = 0;
    int rc = ibv_modify_qp(qp, &attr,
        IBV_QP_STATE |
        IBV_QP_PKEY_INDEX |
        IBV_QP_PORT | IBV_QP_QKEY
    );
    if (rc != 0) {
        SPAWN_ERR("Failed to modify QP to INIT (ibv_modify_qp errno=%d %s)", rc, strerror(rc));
        return 1;
    }    
        
    /* set QP to RTR */
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTR;
    rc = ibv_modify_qp(qp, &attr, IBV_QP_STATE);
    if (rc != 0) {
        SPAWN_ERR("Failed to modify QP to RTR (ibv_modify_qp errno=%d %s)", rc, strerror(rc));
        return 1;
    }   

    /* set QP to RTS */
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn   = RDMA_DEFAULT_PSN;
    rc = ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN);
    if (rc != 0) {
        SPAWN_ERR("Failed to modify QP to RTS (ibv_modify_qp errno=%d %s)", rc, strerror(rc));
        return 1;
    }

    return 0;
}

/* Create UD QP */
static struct ibv_qp* qp_create(ud_qp_info_t *qp_info)
{
    /* zero out all fields of queue pair attribute structure */
    struct ibv_qp_init_attr init_attr;
    memset(&init_attr, 0, sizeof(struct ibv_qp_init_attr));

    /* set attributes */
    init_attr.send_cq = qp_info->send_cq;
    init_attr.recv_cq = qp_info->recv_cq;
    init_attr.cap.max_send_wr = qp_info->cap.max_send_wr;
    
    if (qp_info->srq) {
        init_attr.srq = qp_info->srq;
        init_attr.cap.max_recv_wr = 0;
    } else {    
        init_attr.cap.max_recv_wr = qp_info->cap.max_recv_wr;
    }

    init_attr.cap.max_send_sge = qp_info->cap.max_send_sge;
    init_attr.cap.max_recv_sge = qp_info->cap.max_recv_sge;
    init_attr.cap.max_inline_data = qp_info->cap.max_inline_data;
    init_attr.qp_type = IBV_QPT_UD;

    /* create queue pair */
    struct ibv_qp* qp = ibv_create_qp(qp_info->pd, &init_attr);
    if(qp == NULL) {
        /* TODO: man page doesn't say anything about errno values */
        SPAWN_ERR("error in creating UD qp");
        return NULL;
    }
    
    /* set queue pair to UD */
    if (qp_transition(qp)) {
        ibv_destroy_qp(qp);
        return NULL;
    }

    return qp;
}

/* Initialize UD Context */
static ud_ctx_t* ud_ctx_create()
{
    /* init vbuf routines */
    vbuf_init();

    /* initialize lock for communication */
    int ret = pthread_mutex_init(&comm_lock_object, 0);
    if (ret != 0) {
        SPAWN_ERR("Failed to init comm_lock_object (pthread_mutex_init ret=%d %s)",
            ret, strerror(ret));
        spawn_exit(-1);
    }   

    rdma_ud_max_ack_pending = rdma_default_ud_sendwin_size / 4;
//    rdma_ud_max_ack_pending = 0;

    /* increase memory locked limit */
    struct rlimit limit;
    ret = getrlimit(RLIMIT_MEMLOCK, &limit);
    if (ret != 0) {
        SPAWN_ERR("Failed to read MEMLOCK limit (getrlimit errno=%d %s)", errno, strerror(errno));
        return NULL;
    }
    limit.rlim_cur = limit.rlim_max;
    ret = setrlimit(RLIMIT_MEMLOCK, &limit);
    if (ret != 0) {
        SPAWN_ERR("Failed to increase MEMLOCK limit (setrlimit errno=%d %s)", errno, strerror(errno));
        return NULL;
    }

    /* allocate UD context structure */
    ud_ctx_t* ud_ctx = (ud_ctx_t*) SPAWN_MALLOC(sizeof(ud_ctx_t));

    /* initialize context fields */
    ud_ctx->qp               = NULL;
    ud_ctx->hca_num          = 0;
    ud_ctx->send_wqes_avail  = rdma_default_max_ud_send_wqe - 50;
    ud_ctx->num_recvs_posted = 0;
    ud_ctx->credit_preserve  = (rdma_default_max_ud_recv_wqe / 4);
    MESSAGE_QUEUE_INIT(&ud_ctx->ext_send_queue);
    ud_ctx->ext_sendq_count  = 0;

    /* set parameters for UD queue pair */
    ud_qp_info_t qp_info;
    qp_info.pd                  = g_hca_info.pd;
    qp_info.srq                 = NULL;
    qp_info.sq_psn              = RDMA_DEFAULT_PSN;
    qp_info.send_cq             = g_hca_info.cq_hndl;
    qp_info.recv_cq             = g_hca_info.cq_hndl;
    qp_info.cap.max_send_wr     = rdma_default_max_ud_send_wqe;
    qp_info.cap.max_recv_wr     = rdma_default_max_ud_recv_wqe;
    qp_info.cap.max_send_sge    = RDMA_DEFAULT_MAX_SG_LIST;
    qp_info.cap.max_recv_sge    = RDMA_DEFAULT_MAX_SG_LIST;
    qp_info.cap.max_inline_data = RDMA_DEFAULT_MAX_INLINE_SIZE;

    /* create UD queue pair and attach to context */
    ud_ctx->qp = qp_create(&qp_info);
    if (ud_ctx->qp == NULL) {
        SPAWN_ERR("Error in creating UD QP");
        return NULL;
    }

    /* post initial UD recv requests */
    int remaining = rdma_default_max_ud_recv_wqe - ud_ctx->num_recvs_posted;
    int posted = ud_post_recv_buffers(remaining, ud_ctx);
    ud_ctx->num_recvs_posted = posted;

    /* save context in global proc structure */
    proc.ud_ctx = ud_ctx;

    /* initialize global unack'd queue */
    MESSAGE_QUEUE_INIT(&proc.unack_queue);

    /* record our lid and qpn */
    local_ep_info.lid = g_hca_info.port_attr[0].lid;
    local_ep_info.qpn = proc.ud_ctx->qp->qp_num;

    /* initialize attributes to create comm thread */
    pthread_attr_t attr;
    if (pthread_attr_init(&attr)) {
        SPAWN_ERR("Unable to init thread attr");
        return NULL;
    }

    /* set stack size for comm thred */
    ret = pthread_attr_setstacksize(&attr, DEFAULT_CM_THREAD_STACKSIZE);
    if (ret && ret != EINVAL) {
        SPAWN_ERR("Unable to set stack size");
        return NULL;
    }

    /* disable SIGCHLD while we start comm thread */
    sigset_t sigmask;
    sigemptyset(&sigmask);
    sigaddset(&sigmask, SIGCHLD);
    ret = pthread_sigmask(SIG_BLOCK, &sigmask, NULL);
    if (ret != 0) {
        SPAWN_ERR("Failed to block SIGCHLD (pthread_sigmask rc=%d %s)", ret, strerror(ret));
    }

    /* if we don't busy spin waiting for a message, start up a
     * receive thread to signal us when a message is ready */
    if (! g_recv_busy_spin) {
        /* initialize the condition variable and flag, do this before
         * creating threads that access these variables */
        g_recv_flag = 0;
        pthread_cond_init(&g_recv_cond, NULL);

        /* start the receive thread */
        pthread_create(&recv_thread, &attr, recv_thread_fn, NULL);

        /* when using recv thread, create a VC to ourself in order
         * to send SHUTDOWN message to thread later, need to do this
         * to exit from ibv_get_cq_event / ibv_destroy_cq cleanly */
        g_vc_self = vc_alloc();
        g_vc_self->writeid = g_vc_self->readid;
        vc_set_addr(g_vc_self, &local_ep_info, RDMA_DEFAULT_PORT);
        g_vc_self->state = VC_STATE_CONNECTED;
    }

    /* start resend timeout threads */
    pthread_create(&timeout_thread, &attr, timeout_thread_fn, NULL);

    /* reenable SIGCHLD in main thread */
    ret = pthread_sigmask(SIG_UNBLOCK, &sigmask, NULL);
    if (ret != 0) {
        SPAWN_ERR("Failed to unblock SIGCHLD (pthread_sigmask rc=%d %s)", ret, strerror(ret));
    }

    return ud_ctx;
}

/* Destroy UD Context */
static void ud_ctx_destroy(spawn_net_endpoint** pep)
{
    int rc;

    /* get pointer to end point */
    spawn_net_endpoint* ep = *pep;

    /* extract context from endpoint */
    ud_ctx_t* ud_ctx = proc.ud_ctx;

    /* signal our threads that we need to shut down */
    force_shutdown = 1;

    /* wait for resend thread to return */
    rc = pthread_join(timeout_thread, NULL);
    if (rc != 0) {
        SPAWN_ERR("Failed to join resend timeout event thread (pthread_join rc=%d %s)", rc, strerror(rc));
    }

    /* shut down receive thread if we have one */
    if (! g_recv_busy_spin) {
        /* TODO: send a shutdown message to force the recv_thread
         * to get an event and thus wake up, then call pthread_join
         * here instead */
        /* send a message to ourself to shutdown recv thread */
        packet_send(g_vc_self, PKT_UD_SHUTDOWN, NULL, 0);

#if 0
        /* shut down the receive thread */
        rc = pthread_cancel(recv_thread);
        if (rc != 0) {
            SPAWN_ERR("Failed to cancel completion event thread (pthread_cancel rc=%d %s)", rc, strerror(rc));
        }
#endif

        /* shut down the receive thread */
        rc = pthread_join(recv_thread, NULL);
        if (rc != 0) {
            SPAWN_ERR("Failed to join completion event thread (pthread_join rc=%d %s)", rc, strerror(rc));
        }

        /* after recv thread finishes, free the VC to ourself */
        g_vc_self->local_closed  = 1;
        g_vc_self->remote_closed = 1;
        vc_free(g_vc_self);
        g_vc_self = NULL;

        /* delete the condition variable */
        rc = pthread_cond_destroy(&g_recv_cond);
        if (rc == 0) {
            //g_recv_cond = PTHREAD_COND_INITIALIZER;
        } else {
            SPAWN_ERR("Failed to destroy receive condition variable (pthread_cond_destroy rc=%d %s)", rc, strerror(rc));
        }
    }

    /* destroy the lock */
    rc = pthread_mutex_destroy(&comm_lock_object);
    if (rc == 0) {
        //comm_lock_object = PTHREAD_MUTEX_INITIALIZER;
    } else {
        SPAWN_ERR("Failed to destroy pthread mutex (pthread_mutex_destroy rc=%d %s)", rc, strerror(rc));
    }

    /* deregister and free memory for vbufs */
    vbuf_finalize();

    /* destroy UD QP if we have one */
    if (ud_ctx->qp) {
        rc = ibv_destroy_qp(ud_ctx->qp);
        if (rc == 0) {
            ud_ctx->qp = NULL;
        } else {
            SPAWN_ERR("Failed to destroy UDQP (ibv_destroy_qp rc=%d %s)", rc, strerror(rc));
        }
    }

    /* free off HCA resources */
    mv2_hca_info_t* hca_info = &g_hca_info;

    /* destroy our completion queue */
    if (hca_info->cq_hndl != NULL) {
        rc = ibv_destroy_cq(hca_info->cq_hndl);
        if (rc == 0) {
            hca_info->cq_hndl = NULL;
        } else {
            SPAWN_ERR("Failed to destroy completion queue (ibv_destroy_cq rc=%d %s)", rc, strerror(rc));
        }
    }

    /* destroy the completion channel */
    if (hca_info->comp_channel != NULL) {
        rc = ibv_destroy_comp_channel(hca_info->comp_channel);
        if (rc == 0) {
            hca_info->comp_channel = NULL;
        } else {
            SPAWN_ERR("Failed to destroy completion channel (ibv_destroy_comp_channel rc=%d %s)", rc, strerror(rc));
        }
    }

    /* free our protection domain */
    if (hca_info->pd != NULL) {
        rc = ibv_dealloc_pd(hca_info->pd);
        if (rc == 0) {
            hca_info->pd = NULL;
        } else {
            /* TODO: silence this error message for now,
             * something is still holding a ref to this pd,
             * perhaps ibv_destroy_ah for each vc? */
            //SPAWN_ERR("Failed to deallocate protection domain (ibv_dealloc_pd rc=%d %s)", rc, strerror(rc));
        }
    }

    /* close our device context */
    if (hca_info->context != NULL) {
        rc = ibv_close_device(hca_info->context);
        if (rc == 0) {
            /* when we close the context, we also lose the associated device */
            hca_info->device  = NULL;
            hca_info->context = NULL;
        } else {
            SPAWN_ERR("Failed to close HCA device (ibv_close_device rc=%d %s)", errno, strerror(errno));
        }
    }

    /* now free context data structure */
    spawn_free(&ud_ctx);

    spawn_free(&g_ud_vc_info);

    return;
}

/*******************************************
 * spawn_net API for IBUD
 ******************************************/

spawn_net_endpoint* spawn_net_open_ib()
{
    /* open HCA context and UD QP if we haven't already */
    if (g_count_open == 0) {
        /* open HCA for communication */
        memset(&g_hca_info, 0, sizeof(mv2_hca_info_t));
        if (hca_open(0, &g_hca_info) != 0) {
            SPAWN_ERR("Failed to initialize HCA");
            return SPAWN_NET_ENDPOINT_NULL;
        }

        /* create our UD QP and data structures to manage it */
        ud_ctx_t* ctx = ud_ctx_create();
        if (ctx == NULL) {
            SPAWN_ERR("Failed to create UDQP context");
            return SPAWN_NET_ENDPOINT_NULL;
        }
    }

    /* increase runing count of open endpoints */
    g_count_open++;

    /* Since we share one UDQP amongst all connections on this process,
     * we assign a unique id to each one so that each open produces an
     * enpoint with a unique name.  This way connect/accept can
     * refer to a particular context. */
    uint64_t epid = g_ud_ep_id;
    g_ud_ep_id++;

    /* get our hostname, we encode this in our endpoint name
     * for better human readability */
    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        SPAWN_ERR("Failed gethostname()");
        return SPAWN_NET_CHANNEL_NULL;
    }
    int hostname_len = (int) strlen(hostname);

    /* allocate new endpoint and fill in its fields */
    spawn_net_endpoint* ep = SPAWN_MALLOC(sizeof(spawn_net_endpoint));
    ep->type = SPAWN_NET_TYPE_IBUD;
    ep->name = SPAWN_STRDUPF("IBUD:%d:%s:%04x:%06x:%06x",
        hostname_len, hostname, local_ep_info.lid, local_ep_info.qpn, epid
    );
    ep->data = (void*) epid; /* cache epid with endpoint (needed in accept) */

    return ep;
}

int spawn_net_close_ib(spawn_net_endpoint** pep)
{
    /* decrement our count of open endpoints */
    g_count_open--;

    /* free the UD QP data structures and release the hca
     * context if we've hit 0 */
    if (g_count_open == 0) {
        /* TODO: while g_count_conn > 0 spin */
        /* TODO: need to ensure comm thread is done */

        /* close down UD endpoint */
        ud_ctx_destroy(pep);
    }

    /* get pointer to endpoint */
    spawn_net_endpoint* ep = *pep;

    /* free the endpoint name */
    spawn_free(&ep->name);

    /* free the endpoint data structure */
    spawn_free(&ep);

    /* set caller's endpoint to NULL */
    *pep = SPAWN_NET_CHANNEL_NULL;

    return SPAWN_SUCCESS;
}

spawn_net_channel* spawn_net_connect_ib(const char* name)
{
    /* verify that the address string starts with correct prefix */
    if (strncmp(name, "IBUD:", 5) != 0) {
        SPAWN_ERR("Endpoint name is not IBUD format %s", name);
        return SPAWN_NET_CHANNEL_NULL;
    }

    /* get our hostname, we encode this in our channel name,
     * because it's useful when debugging */
    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        SPAWN_ERR("Failed gethostname()");
        return SPAWN_NET_CHANNEL_NULL;
    }
    int hostname_len = (int) strlen(hostname);

    /* make a copy of name that we can modify */
    char* name_copy = SPAWN_STRDUP(name);

    /* advance past IBUD: */
    char* ptr = name_copy;
    ptr += 5;

    /* pick out length of remote hostname */
    char* host_len_str = ptr;
    while (*ptr != ':') {
        ptr++;
    }
    *ptr = '\0';
    ptr++;

    /* set remote hostname and skip to IBUD address */
    char* host_str = ptr;
    int host_len = atoi(host_len_str);
    ptr += host_len;
    *ptr = '\0';
    ptr++;

    /* extract lid, queue pair number, and endpoint id from
     * endpoint name */
    unsigned int lid, qpn, epid;
    int parsed = sscanf(ptr, "%04x:%06x:%06x", &lid, &qpn, &epid);
    if (parsed != 3) {
        SPAWN_ERR("Couldn't parse ep info from %s", name);
        spawn_free(&name_copy);
        return SPAWN_NET_CHANNEL_NULL;
    }

    /* if we move this higher, be sure to unlock before returning */
    comm_lock();

    /* allocate and initialize a new virtual channel */
    vc_t* vc = vc_alloc();

    /* store remote lid and queue pair */
    ud_addr ep_info;
    ep_info.lid = lid;
    ep_info.qpn = qpn;

    /* point channel to remote endpoint */
    vc_set_addr(vc, &ep_info, RDMA_DEFAULT_PORT);

    /* build payload for connect message, first record epid
     * of remote endpoint that we're connecting to, remote
     * end uses this to associate our request with a
     * particular endpoint context, then specify our hostname,
     * followed by the id we want remote side to use when
     * sending to us and our lid/qp */
    char* payload = SPAWN_STRDUPF("%d:%s:%06x:%06x:%04x:%06x",
        hostname_len, hostname, epid, vc->readid, local_ep_info.lid, local_ep_info.qpn
    );
    size_t payload_size = strlen(payload) + 1;

    /* send connect packet */
    int rc = packet_send(vc, PKT_UD_CONNECT, payload, payload_size);

    /* free payload memory */
    spawn_free(&payload);

    /* wait for accept message and set vc->writeid */
    recv_accept_message(vc);

    /* Change state to connected */
    vc->state = VC_STATE_CONNECTED;

    /* allocate spawn net channel data structure */
    spawn_net_channel* ch = SPAWN_MALLOC(sizeof(spawn_net_channel));
    ch->type = SPAWN_NET_TYPE_IBUD;

    /* TODO: include local and remote hostnames, with readid/writeid */
    /* Fill in channel name */
    ch->name = SPAWN_STRDUPF("IBUD:%s:%04x:%06x", host_str, ep_info.lid, ep_info.qpn);

    /* record address of vc in channel data field */
    ch->data = (void*) vc;

    /* increase our connection count */
    g_count_conn++;

    /* free temporary copy of name */
    spawn_free(&name_copy);

    comm_unlock();

    return ch;
}

spawn_net_channel* spawn_net_accept_ib(const spawn_net_endpoint* ep)
{
    comm_lock();

    /* NOTE: If we're slow to accept, the process that sent us the
     * connect packet may have timed out and sent a duplicate request.
     * Both packets may be in our connection request queue, and we
     * need to ignore any duplicates.  To do this, we keep track of
     * current connections by recording remote lid/qpn/writeid and
     * then silently drop extras. */

    /* get our endpoint context id, we need to filter connection
     * requests by this id */
    uint64_t local_epid = (uint64_t) ep->data;

    /* wait for connect message */
    connect_list* req = recv_connect_message(local_epid);
    vbuf* v           = req->v;
    unsigned int epid = req->epid;
    unsigned int lid  = req->lid;
    unsigned int qpn  = req->qpn;
    unsigned int id   = req->id;

    /* allocate new vc */
    vc_t* vc = vc_alloc();

    /* allocate and initialize new item for connected list */
    connected_list* elem = (connected_list*) SPAWN_MALLOC(sizeof(connected_list));
    elem->lid  = lid;
    elem->qpn  = qpn;
    elem->id   = id;
    elem->vc   = vc;
    elem->next = NULL;

    /* append item to connected list */
    if (connected_head == NULL) {
        connected_head = elem;
    }
    if (connected_tail != NULL) {
        connected_tail->next = elem;
    }
    connected_tail = elem;

    /* store lid and queue pair */
    ud_addr ep_info;
    ep_info.lid = lid;
    ep_info.qpn = qpn;

    /* record lid/qp in VC */
    vc_set_addr(vc, &ep_info, RDMA_DEFAULT_PORT);

    /* record remote id as write id */
    uint64_t writeid = (uint64_t) id;
    vc->writeid = writeid;

    /* record pointer to VC in vbuf (needed by Process_recv) */
    v->vc = vc;

    /* NOTE: it's tempting to call ud_process_recv here, but that may
     * send and ACK, and we don't want to do that until we first send
     * the ACCEPT packet so that process */

    /* put vbuf back on free list */
    vbuf_release(v);

    /* Increment the next expected seq num */
    vc->seqnum_next_torecv++;

    /* build accept message, specify id we want remote side to use when
     * sending to us followed by our lid/qp */
    char* payload = SPAWN_STRDUPF("%06x", vc->readid);
    size_t payload_size = strlen(payload) + 1;

    /* send the accept packet */
    int rc = packet_send(vc, PKT_UD_ACCEPT, payload, payload_size);

    /* free payload memory */
    spawn_free(&payload);

    /* mark vc as connected */
    vc->state = VC_STATE_CONNECTED;

    /* allocate new channel data structure */
    spawn_net_channel* ch = SPAWN_MALLOC(sizeof(spawn_net_channel));
    ch->type = SPAWN_NET_TYPE_IBUD;

    /* TODO: include local/remote hostnames and readid/writeid */
    /* record name */
    ch->name = SPAWN_STRDUPF("IBUD:%s:%04x:%06x", req->name, ep_info.lid, ep_info.qpn);

    /* record address of vc in channel data field */
    ch->data = (void*) vc;

    /* free connection request item */
    spawn_free(&req->name);
    spawn_free(&req);

    comm_unlock();

    /* increase our connection count */
    g_count_conn++;

    return ch;
}

int spawn_net_disconnect_ib(spawn_net_channel** pch)
{
    /* get pointer to channel */
    spawn_net_channel* ch = *pch;

    /* get pointer to vc from channel data field */
    vc_t* vc = (vc_t*) ch->data;
    if (vc == NULL) {
        return SPAWN_FAILURE;
    }

    comm_lock();

    /* send disconnect packet */
    int ret = packet_send(vc, PKT_UD_DISCONNECT, NULL, 0);

    /* mark vc as closed from local side */
    vc->local_closed = 1;

    /* decrement number of active connections using endpoint */
    g_count_conn--;

    /* release vc if we can */
    vc_free(vc);

    comm_unlock();

    /* free channel name */
    spawn_free(&ch->name);

    /* free memory associated with channel */
    spawn_free(&ch);

    /* set user's pointer to a NULL channel */
    *pch = SPAWN_NET_CHANNEL_NULL;

    return ret;
}

int spawn_net_read_ib(const spawn_net_channel* ch, void* buf, size_t size)
{
    /* get pointer to vc from channel data field */
    vc_t* vc = (vc_t*) ch->data;
    if (vc == NULL) {
        return SPAWN_FAILURE;
    }

    comm_lock();

    /* compute header and payload sizes */
    size_t header_size = sizeof(packet_header);
    assert(MRAIL_MAX_UD_SIZE >= header_size);

    /* read data one packet at a time */
    int ret = SPAWN_SUCCESS;
    size_t nread = 0;
    while (nread < size) {
        /* wait for a packet from this vc */
        vbuf* v = packet_wait(vc);

        /* determine number of bytes in packet payload */
        size_t payload_size = v->content_size - header_size;

        /* determine number of bytes left to read from packet */
        size_t payload_remaining = payload_size - (size_t) vc->nread;

        /* determine number of bytes left in user buffer */
        size_t remaining = size - nread;

        /* compute pointers into user and packet buffers */
        char* ptr  = (char*)buf + nread;
        char* data = v->content_buf + header_size + vc->nread;

        /* copy data to user's buffer and process packet */
        if (remaining < payload_remaining) {
            /* the remaining payload in the packet is strictly more
             * than what we need to fill the user buffer, so just
             * read what we need and advance the vc pointer */
            memcpy(ptr, data, remaining);
            nread     += remaining;
            vc->nread += (int) remaining;
        } else {
            /* in this case, we'll read every byte in the packet */
            memcpy(ptr, data, payload_remaining);
            nread += payload_remaining;

            /* extract the packet from the receive queue,
             * and place it back on free list */
            v = apprecv_window_retrieve_and_remove(&vc->app_recv_window);
            vbuf_release(v);

            /* we'll be pointing to a brand new packet,
             * so reset the vc pointer */
            vc->nread = 0;
        }
    }

    comm_unlock();

    return ret;
}

int spawn_net_write_ib(const spawn_net_channel* ch, const void* buf, size_t size)
{
    /* get pointer to vc from channel data field */
    vc_t* vc = (vc_t*) ch->data;
    if (vc == NULL) {
        return SPAWN_FAILURE;
    }

    comm_lock();

    /* compute header and payload sizes */
    size_t header_size = sizeof(packet_header);
    size_t payload_size = MRAIL_MAX_UD_SIZE - header_size;
    assert(MRAIL_MAX_UD_SIZE >= header_size);

    /* break message up into packets and send each one */
    int ret = SPAWN_SUCCESS;
    size_t nwritten = 0;
    while (nwritten < size) {
        /* determine amount to write in this step */
        size_t bytes = (size - nwritten);
        if (bytes > payload_size) {
            bytes = payload_size;
        }

        /* get pointer to data */
        char* data = (char*)buf + nwritten;

        /* send packet */
        int tmp_rc = packet_send(vc, PKT_UD_DATA, data, bytes);
        if (tmp_rc != SPAWN_SUCCESS) {
            ret = tmp_rc;
            break;
        }

        /* go to next part of message */
        nwritten += bytes;
    }

    comm_unlock();

    return ret;
}

/* this waits until one of the specified channels has a message
 * pending, and then it sets index to the index of that channel,
 * index is set to -1 if none of the channels are valid */
int spawn_net_wait_ib(
  int neps,
  const spawn_net_endpoint** eps,
  int nchs,
  const spawn_net_channel** chs,
  int* index)
{
    /* bail out if channel array is empty */
    if (eps == NULL && chs == NULL) {
        return SPAWN_FAILURE;
    }

    comm_lock();

    /* loop until a message is available on a channel
     * or a connection request comes in on an endpoint */
    while (1) {
        /* set flag to indicate that all channels are NULL */
        int valid = 0;

        /* cycle over every endpoint and look for connreq */
        int i;
        for (i = 0; i < neps; i++) {
            /* get pointer to endpoint */
            const spawn_net_endpoint* ep = eps[i];

            /* skip NULL endpoints */
            if (ep == SPAWN_NET_ENDPOINT_NULL) {
                continue;
            }

            /* get our endpoint context id, we need to filter connection
             * requests by this id */
            uint64_t epid = (uint64_t) ep->data;

            /* we found at least one non-NULL channel */
            valid = 1;

            /* walk connect list looking for matching request */
            connect_list* elem = connect_head;
            while (elem != NULL) {
                /* stop walking the list if we find a matching element */
                if (elem->epid == epid) {
                    /* we found an endpoint with a pending connreq,
                     * set output parameter and exit */
                    *index = i;
                    comm_unlock();
                    return SPAWN_SUCCESS;
                }

                /* otherwise, look at the next element */
                elem = elem->next;
            }
        }

        /* cycle over every channel and check whether a message is ready */
        for (i = 0; i < nchs; i++) {
            /* get pointer to channel */
            const spawn_net_channel* ch = chs[i];

            /* skip NULL channels */
            if (ch == SPAWN_NET_CHANNEL_NULL) {
                continue;
            }

            /* get pointer to vc from channel data field */
            vc_t* vc = (vc_t*) ch->data;
            if (vc == NULL) {
                continue;
            }

            /* we found at least one non-NULL channel */
            valid = 1;

            /* look for entry in apprecv queue */
            int pending = apprecv_window_test(&vc->app_recv_window);
            if (pending) {
                /* we found a channel with a pending message,
                 * set output parameter and exit */
                *index = i + neps;
                comm_unlock();
                return SPAWN_SUCCESS;
            }
        }

        /* if all endpoints and channels are NULL,
         * we can't wait on any of them */
        if (! valid) {
            *index = -1;
            comm_unlock();
            return SPAWN_SUCCESS;
        }

        /* we have at least one valid channel, but nothing is ready
         * on any channels, wait to be signaled for a new message */
        g_recv_flag = 1;
        pthread_cond_wait(&g_recv_cond, &comm_lock_object);
    }
}
