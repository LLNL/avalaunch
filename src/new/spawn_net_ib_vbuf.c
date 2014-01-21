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

#include "spawn_net_ib_vbuf.h"
#include "spawn_net_ib_internal.h"

/*
 * Vbufs are allocated in blocks and threaded on a single free list.
 *
 * These data structures record information on all the vbuf
 * regions that have been allocated.  They can be used for
 * error checking and to un-register and deallocate the regions
 * at program termination.
 *
 */
typedef struct vbuf_region
{
    struct ibv_mr* mem_handle[MAX_NUM_HCAS]; /* mem hndl for entire region */
    void* malloc_start;         /* used to free region later  */
    void* malloc_end;           /* to bracket mem region      */
    void* malloc_buf_start;     /* used to free DMA region later */
    void* malloc_buf_end;       /* bracket DMA region */
    int count;                  /* number of vbufs in region  */
    struct vbuf* vbuf_head;     /* first vbuf in region       */
    struct vbuf_region* next;   /* thread vbuf regions        */
    int shmid;
} vbuf_region;

/* head of list of allocated vbuf regions */
static vbuf_region* vbuf_region_head = NULL;

/* free_vbuf_head is the head of the free list */
static vbuf* free_vbuf_head    = NULL;
static vbuf* ud_free_vbuf_head = NULL;

/* track vbufs for RC connections */
static int vbuf_n_allocated = 0; /* total number allocated */
static long num_free_vbuf   = 0; /* number currently free */
static long num_vbuf_get    = 0; /* number of times vbuf was taken from free list */
static long num_vbuf_freed  = 0; /* number of times vbuf was added to free list */

static int ud_vbuf_n_allocated = 0;
static long ud_num_free_vbuf   = 0;
static long ud_num_vbuf_get    = 0;
static long ud_num_vbuf_freed  = 0;

static pthread_spinlock_t vbuf_lock;

int vbuf_init(void)
{
    int rc = pthread_spin_init(&vbuf_lock, 0);
    if (rc != 0) {
        SPAWN_ERR("Failed to lock vbuf_lock (pthread_spin_init rc=%d %s)", rc, strerror(rc));
        ibv_error_abort(-1, "Failed to init vbuf_lock\n");
    }

    return 0;
}

static int alloc_hugepage_region(int *shmid, void **buffer, int *nvbufs, int buf_size)
{
    int ret = 0;
    size_t size = *nvbufs * buf_size;
    MRAILI_ALIGN_LEN(size, HUGEPAGE_ALIGN);

    /* create hugepage shared region */
    *shmid = shmget(IPC_PRIVATE, size, 
                        SHM_HUGETLB | IPC_CREAT | SHM_R | SHM_W);
    if (*shmid < 0) {
        if (rdma_enable_hugepage >= 2) {
            SPAWN_ERR("Failed to get shared memory id (shmget errno=%d %s)", errno, strerror(errno));
        }
        goto fn_fail;
    }

    /* attach shared memory */
    *buffer = (void *) shmat(*shmid, SHMAT_ADDR, SHMAT_FLAGS);
    if (*buffer == (void *) -1) {
        SPAWN_ERR("Failed to attach shared memory (shmat errno=%d %s)", errno, strerror(errno));
        goto fn_fail;
    }
    
    /* Mark shmem for removal */
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

static int allocate_ud_vbuf_region(struct ibv_pd* pdomain, int nvbufs)
{
    int i;
    int result;

    /* specify alignment parameters */
    int alignment_vbuf = 64;
    int alignment_dma = getpagesize();

    PRINT_DEBUG(DEBUG_UD_verbose>0,"Allocating a UD buf region.\n");

    if (ud_free_vbuf_head != NULL) {
        ibv_error_abort(GEN_ASSERT_ERR, "free_vbuf_head = NULL");
    }

    struct vbuf_region* reg = (struct vbuf_region*) SPAWN_MALLOC(sizeof(struct vbuf_region));
    
    void* vbuf_dma_buffer = NULL;

    /* get memory from huge pages if enabled */
    if (rdma_enable_hugepage) {
        result = alloc_hugepage_region(
            &reg->shmid, &vbuf_dma_buffer, &nvbufs, rdma_default_ud_mtu
        );
    }

    /* do posix_memalign if enable hugepage disabled or failed */
    if (rdma_enable_hugepage == 0 || result != 0)  {
        reg->shmid = -1;
        result = posix_memalign(
            &vbuf_dma_buffer, alignment_dma, nvbufs * rdma_default_ud_mtu
        );
        if (result != 0) {
            SPAWN_ERR("Cannot allocate vbuf region (posix_memalign rc=%d %s)", result, strerror(result));
        }
    }

    /* check that we got the dma buffer */
    if ((result != 0) || (NULL == vbuf_dma_buffer)) {
        ibv_error_abort(GEN_EXIT_ERR, "unable to malloc vbufs DMA buffer");
    }
    
    /* allocate memory for vbuf data structures */
    void* mem;
    result = posix_memalign(
        (void**) &mem, alignment_vbuf, nvbufs * sizeof(vbuf)
    );
    if (result != 0) {
        SPAWN_ERR("Cannot allocate vbuf region (posix_memalign rc=%d %s)", result, strerror(result));
        /* TODO: free vbuf_dma_buffer */
        spawn_free(&reg);
        return -1;
    }

    /* clear memory regions */
    memset(mem,             0, nvbufs * sizeof(vbuf));
    memset(vbuf_dma_buffer, 0, nvbufs * rdma_default_ud_mtu);

    /* update global vbuf variables */
    ud_vbuf_n_allocated += nvbufs;
    ud_num_free_vbuf    += nvbufs;
    ud_free_vbuf_head    = mem;

    /* fill in fields of vbuf_region structure */
    reg->malloc_start     = mem;
    reg->malloc_buf_start = vbuf_dma_buffer;
    reg->malloc_end       = (void *) ((char *) mem + nvbufs * sizeof(vbuf));
    reg->malloc_buf_end   = (void *) ((char *) vbuf_dma_buffer + nvbufs * rdma_default_ud_mtu);
    reg->count            = nvbufs;
    reg->vbuf_head        = ud_free_vbuf_head;

    PRINT_DEBUG(DEBUG_UD_verbose>0,
            "VBUF REGION ALLOCATION SZ %d TOT %d FREE %ld NF %ld NG %ld\n",
            rdma_default_ud_mtu,
            ud_vbuf_n_allocated,
            ud_num_free_vbuf,
            ud_num_vbuf_freed,
            ud_num_vbuf_get);

    /* register region with each HCA */
    for (i = 0; i < rdma_num_hcas; ++i) {
        /* register memory region */
        reg->mem_handle[i] = ibv_reg_mr(
                pdomain,
                vbuf_dma_buffer,
                nvbufs * rdma_default_ud_mtu,
                IBV_ACCESS_LOCAL_WRITE
        );

        if (reg->mem_handle[i] == NULL) {
            SPAWN_ERR("Cannot register vbuf region (ibv_reg_mr errno=%d %s)", errno, strerror(errno)); 
            /* TODO: need to free memory / unregister with some cards? */
            return -1;
        }
    }

    /* init the vbuf structures */
    for (i = 0; i < nvbufs; ++i) {
        /* get a pointer to the vbuf */
        vbuf* cur = ud_free_vbuf_head + i;

        /* set next pointer */
        cur->desc.next = ud_free_vbuf_head + i + 1;
        if (i == (nvbufs - 1)) {
            cur->desc.next = NULL;
        }

        /* set pointer to region */
        cur->region = reg;

        /* set pointer to data buffer */
        char* ptr = (char *)vbuf_dma_buffer + i * rdma_default_ud_mtu;
        cur->buffer = (void*) ptr;

        /* set pointer to head flag, comes as last bytes of buffer */
        cur->head_flag = (VBUF_FLAG_TYPE *) (ptr + rdma_default_ud_mtu - sizeof(cur->head_flag));

        /* set remaining fields */
        cur->content_size = 0;
    }

    /* insert region into list */
    reg->next = vbuf_region_head;
    vbuf_region_head = reg;

    return 0;
}

static int allocate_ud_vbufs(struct ibv_pd* pd, int nvbufs)
{
    return allocate_ud_vbuf_region(pd, nvbufs);
}

vbuf* vbuf_get(struct ibv_pd* pd)
{
    vbuf* v = NULL;

    pthread_spin_lock(&vbuf_lock);

    /* if we don't have any free vufs left, try to allocate more */
    if (ud_free_vbuf_head == NULL) {
        if (allocate_ud_vbuf_region(pd, rdma_vbuf_secondary_pool_size) != 0) {
            ibv_va_error_abort(GEN_EXIT_ERR,
                    "UD VBUF reagion allocation failed. Pool size %d\n", vbuf_n_allocated);
        }
    }

    /* pick item from head of list */
    /* this correctly handles removing from single entry free list */
    v = ud_free_vbuf_head;
    ud_free_vbuf_head = ud_free_vbuf_head->desc.next;
    --ud_num_free_vbuf;
    ++ud_num_vbuf_get;

    /* need to change this to RPUT_VBUF_FLAG later
     * if we are doing rput */
    v->padding     = NORMAL_VBUF_FLAG;
    v->pheader     = (void *)v->buffer;
    v->transport   = IB_TRANSPORT_UD;
    v->retry_count = 0;
    v->flags       = 0;
    v->pending_send_polls = 0;

    /* this is probably not the right place to initialize shandle to NULL.
     * Do it here for now because it will make sure it is always initialized.
     * Otherwise we would need to very carefully add the initialization in
     * a dozen other places, and probably miss one. */
    v->content_size = 0;
    /* TODO: Decide which transport need to assign here */

    pthread_spin_unlock(&vbuf_lock);

    return(v);
}

void vbuf_release(vbuf* v)
{
    /* This message might be in progress. Wait for ib send completion 
     * to release this buffer to avoid reusing buffer */
    if (v->flags & UD_VBUF_MCAST_MSG) {
        v->pending_send_polls--;
        if(v->transport == IB_TRANSPORT_UD  &&
           (v->flags & UD_VBUF_SEND_INPROGRESS || v->pending_send_polls > 0))
        {
            /* TODO: when is this really added back to the free buffer? */

            /* if number of pending sends has dropped to 0,
             * mark vbuf that it's ready to be freed */
            if (v->pending_send_polls == 0) {
                v->flags |= UD_VBUF_FREE_PENIDING;
            }
            return;
        }
    } else {
        /* if send is still in progress (has not been ack'd),
         * just mark vbuf as ready to be freed and return */
        if(v->transport == IB_TRANSPORT_UD &&
           (v->flags & UD_VBUF_SEND_INPROGRESS))
        {
            /* TODO: when is this really added back to the free buffer? */

            /* mark vbuf that it's ready to be freed */
            v->flags |= UD_VBUF_FREE_PENIDING;
            return;
        }
    }

    /* note this correctly handles appending to empty free list */
    pthread_spin_lock(&vbuf_lock);

    //DEBUG_PRINT("release_vbuf: releasing %p previous head = %p, padding %d\n",
    //    v, free_vbuf_head, v->padding);

    /* add vbuf to front of appropriate free list */
    if(v->transport == IB_TRANSPORT_UD) {
        /* add vbuf to front of UD free list */
        assert(v != ud_free_vbuf_head);
        v->desc.next = ud_free_vbuf_head;
        ud_free_vbuf_head = v;
        ++ud_num_free_vbuf;
        ++ud_num_vbuf_freed;
    } else {
        /* add vbuf to front of RC free list */
        assert(v != free_vbuf_head);
        v->desc.next = free_vbuf_head;
        free_vbuf_head = v;
        ++num_free_vbuf;
        ++num_vbuf_freed;
    }

    if (v->padding != NORMAL_VBUF_FLAG) {
        ibv_error_abort(GEN_EXIT_ERR, "vbuf not correct.\n");
    }

    /* clear out fields to prepare vbuf for next use */
    *v->head_flag   = 0;
    v->pheader      = NULL;
    v->content_size = 0;
    v->vc           = NULL;

    /* note this correctly handles appending to empty free list */
    pthread_spin_unlock(&vbuf_lock);
}

void vbuf_prepare_recv(vbuf* v, unsigned long len, int hca_num)
{
    assert(v != NULL);

    v->desc.u.rr.next = NULL;
    v->desc.u.rr.wr_id = (uintptr_t) v;
    v->desc.u.rr.num_sge = 1;
    v->desc.u.rr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = len;
    v->desc.sg_entry.lkey = v->region->mem_handle[hca_num]->lkey;
    v->desc.sg_entry.addr = (uintptr_t)(v->buffer);
    v->padding = NORMAL_VBUF_FLAG;
    v->rail = hca_num;
}

void vbuf_prepare_send(vbuf* v, unsigned long len, int rail)
{
    int hca_num = rail / (rdma_num_rails / rdma_num_hcas);

    v->desc.u.sr.next = NULL;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.opcode = IBV_WR_SEND;
    v->desc.u.sr.wr_id = (uintptr_t) v;
    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = len;
    v->desc.sg_entry.lkey = v->region->mem_handle[hca_num]->lkey;
    v->desc.sg_entry.addr = (uintptr_t)(v->buffer);
    v->padding = NORMAL_VBUF_FLAG;
    v->rail = rail;

    return;
}

/* vi:set sw=4 tw=80: */
