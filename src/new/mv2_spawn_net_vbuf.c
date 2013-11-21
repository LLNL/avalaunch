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

#include <mv2_spawn_net_vbuf.h>
#include <mv2_spawn_net_ud.h>
#include <ib_internal.h>

/* vbuf pool info */
vbuf_pool_t *rdma_vbuf_pools;
int rdma_num_vbuf_pools;

/* head of list of allocated vbuf regions */
static vbuf_region *vbuf_region_head = NULL;
/*
 * free_vbuf_head is the head of the free list
 */
static vbuf *free_vbuf_head = NULL;

/*
 * cache the nic handle, and ptag the first time a region is
 * allocated (at init time) for later additional vbuf allocations
 */
static struct ibv_pd *ptag_save[MAX_NUM_HCAS];

/* track vbufs for RC connections */
int vbuf_n_allocated = 0; /* total number allocated */
long num_free_vbuf   = 0; /* number currently free */
long num_vbuf_get    = 0; /* number of times vbuf was taken from free list */
long num_vbuf_freed  = 0; /* number of times vbuf was added to free list */

static vbuf *ud_free_vbuf_head = NULL;
int ud_vbuf_n_allocated = 0;
long ud_num_free_vbuf = 0;
long ud_num_vbuf_get = 0;
long ud_num_vbuf_freed = 0;

static pthread_spinlock_t vbuf_lock;

#if defined(DEBUG)
void dump_vbuf(char* msg, vbuf* v)
{
    int i = 0;
    int len = 100;
    MPIDI_CH3I_MRAILI_Pkt_comm_header* header = v->pheader;
    printf("%s: dump of vbuf %p, type = %d\n", msg, v, header->type);
    len = 100;

    for (; i < len; ++i) {
        if (0 == i % 16) {
            printf("\n  ");
        }

        printf("%2x  ", (unsigned int) v->buffer[i]);
    }

    printf("\n");
    printf("  END OF VBUF DUMP\n");
}
#endif /* defined(DEBUG) */

void mv2_print_vbuf_usage_usage()
{
    size_t tot_mem = (vbuf_n_allocated * (rdma_vbuf_total_size + sizeof(struct vbuf)));
    tot_mem += (ud_vbuf_n_allocated * (rdma_default_ud_mtu + sizeof(struct vbuf))); 
    PRINT_INFO(DEBUG_MEM_verbose, "RC VBUFs:%ld  UD VBUFs:%ld TOT MEM:%ld kB\n",
                    vbuf_n_allocated, ud_vbuf_n_allocated, (tot_mem / 1024));
}

int init_vbuf_lock(void)
{
    int rc = pthread_spin_init(&vbuf_lock, 0);
    if (rc != 0) {
        SPAWN_ERR("Failed to lock vbuf_lock (pthread_spin_init rc=%d %s)", rc, strerror(rc));
        ibv_error_abort(-1, "Failed to init vbuf_lock\n");
    }

    return 0;
}

void deallocate_vbufs(int hca_num)
{
    pthread_spin_lock(&vbuf_lock);

    /* iterate over and deregister each vbuf region */
    vbuf_region* r = vbuf_region_head;
    while (r) {
        /* deregister region if we have a handle to it */
        if (r->mem_handle[hca_num] != NULL &&
            ibv_dereg_mr(r->mem_handle[hca_num]))
        {
            /* TODO: is errno set in this case? */
            SPAWN_ERR("Failed to deregister vbuf region (ibv_dereg_mr rc=%d %s)", errno, strerror(errno));
            ibv_error_abort(IBV_RETURN_ERR, "could not deregister MR");
        }

        /* get next region */
        r = r->next;
    }

    pthread_spin_unlock(&vbuf_lock);
}

void deallocate_vbuf_region(void)
{
    vbuf_region* curr = vbuf_region_head;
    while (curr) {
        free(curr->malloc_start);

        if (rdma_enable_hugepage && curr->shmid >= 0) {
            shmdt(curr->malloc_buf_start);
        } else {
            free(curr->malloc_buf_start);
        }

        /* get pointer to next element and free this one */
        vbuf_region* next = curr->next;
        spawn_free(&curr);
        curr = next;
    }
}

int alloc_hugepage_region (int *shmid, void **buffer, int *nvbufs, int buf_size)
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

static int allocate_vbuf_region(int nvbufs)
{
    struct vbuf_region *reg = NULL;
    void *mem = NULL;
    int i = 0;
    vbuf *cur = NULL;
    void *vbuf_dma_buffer = NULL;
    int alignment_vbuf = 64;
    int alignment_dma = getpagesize();
    int result = 0;

    if (nvbufs <= 0) {
        return 0;
    }

    //DEBUG_PRINT("Allocating a new vbuf region.\n");

    if (free_vbuf_head != NULL)
    {
        ibv_error_abort(GEN_ASSERT_ERR, "free_vbuf_head = NULL");
    }

    /* are we limiting vbuf allocation?  If so, make sure
     * we dont alloc more than allowed
     */
    if (rdma_vbuf_max > 0)
    {
        nvbufs = MIN(nvbufs, rdma_vbuf_max - vbuf_n_allocated);

        if (nvbufs <= 0)
        {
            ibv_error_abort(GEN_EXIT_ERR, "VBUF alloc failure, limit exceeded");
        }
    }

    reg = (struct vbuf_region *) SPAWN_MALLOC (sizeof(struct vbuf_region));

    if (rdma_enable_hugepage) {
        result = alloc_hugepage_region (&reg->shmid, &vbuf_dma_buffer, &nvbufs, rdma_vbuf_total_size);
    }

    /* do posix_memalign if enable hugepage disabled or failed */
    if (rdma_enable_hugepage == 0 || result != 0 )  
    {
        reg->shmid = -1;
        result = posix_memalign(&vbuf_dma_buffer, alignment_dma, nvbufs * rdma_vbuf_total_size);
    }

    if ((result!=0) || (NULL == vbuf_dma_buffer))
    {
       ibv_error_abort(GEN_EXIT_ERR, "unable to malloc vbufs DMA buffer");
    }
    
    if (posix_memalign(
        (void**) &mem,
        alignment_vbuf,
        nvbufs * sizeof(vbuf)))
    {
        fprintf(stderr, "[%s %d] Cannot allocate vbuf region\n", __FILE__, __LINE__);
        return -1;
    }
  
    /* region should be registered for all of the hca */
    {
        reg->mem_handle[0] = ibv_reg_mr(
            g_hca_info.pd,
            vbuf_dma_buffer,
            nvbufs * rdma_vbuf_total_size,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC);

        if (!reg->mem_handle[i])
        {
            /* de-register already registered with other hcas*/
            for (i = i-1; i >=0 ; --i)
            {
                if (reg->mem_handle[i] != NULL
                        && ibv_dereg_mr(reg->mem_handle[i]))
                {
                    fprintf(stderr, "[%s %d] Cannot de-register vbuf region\n", __FILE__, __LINE__);
                }
            }
            /* free allocated buffers */
            free(vbuf_dma_buffer);
            free(mem);
            spawn_free(reg);
            fprintf(stderr, "[%s %d] Cannot register vbuf region\n", __FILE__, __LINE__);
            return -1;
        }
    }

    memset(mem, 0, nvbufs * sizeof(vbuf));
    memset(vbuf_dma_buffer, 0, nvbufs * rdma_vbuf_total_size);

    vbuf_n_allocated += nvbufs;
    num_free_vbuf += nvbufs;
    reg->malloc_start = mem;
    reg->malloc_buf_start = vbuf_dma_buffer;
    reg->malloc_end = (void *) ((char *) mem + nvbufs * sizeof(vbuf));
    reg->malloc_buf_end = (void *) ((char *) vbuf_dma_buffer + nvbufs * rdma_vbuf_total_size);

    reg->count = nvbufs;
    free_vbuf_head = mem;
    reg->vbuf_head = free_vbuf_head;

#if 0
    DEBUG_PRINT(
        "VBUF REGION ALLOCATION SZ %d TOT %d FREE %ld NF %ld NG %ld\n",
        nvbufs,
        vbuf_n_allocated,
        num_free_vbuf,
        num_vbuf_freed,
        num_vbuf_get);
#endif

    /* init the free list */
    for (i = 0; i < nvbufs - 1; ++i)
    {
        cur = free_vbuf_head + i;
        cur->desc.next = free_vbuf_head + i + 1;
        cur->region = reg;
	cur->head_flag = (VBUF_FLAG_TYPE *) ((char *)vbuf_dma_buffer
            + (i + 1) * rdma_vbuf_total_size
            - sizeof * cur->head_flag);
        cur->buffer = (unsigned char *) ((char *)vbuf_dma_buffer
            + i * rdma_vbuf_total_size);

        cur->eager = 0;
        cur->content_size = 0;
        cur->coalesce = 0;
    }

    /* last one needs to be set to NULL */
    cur = free_vbuf_head + nvbufs - 1;
    cur->desc.next = NULL;
    cur->region = reg;
    cur->head_flag = (VBUF_FLAG_TYPE *) ((char *)vbuf_dma_buffer
        + nvbufs * rdma_vbuf_total_size
        - sizeof * cur->head_flag);
    cur->buffer = (unsigned char *) ((char *)vbuf_dma_buffer
        + (nvbufs - 1) * rdma_vbuf_total_size);
    cur->eager = 0;
    cur->content_size = 0;
    cur->coalesce = 0;

    /* thread region list */
    reg->next = vbuf_region_head;
    vbuf_region_head = reg;

    return 0;
}

/* this function is only called by the init routines.
 * Cache the nic handle and ptag for later vbuf_region allocations. */
int allocate_vbufs(struct ibv_pd* ptag[], int nvbufs)
{
    int i;
    for (i = 0; i < rdma_num_hcas; ++i) {
        ptag_save[i] = ptag[i];
    }

    if (allocate_vbuf_region(nvbufs) != 0) {
        ibv_va_error_abort(GEN_EXIT_ERR,
            "VBUF reagion allocation failed. Pool size %d\n", 
                vbuf_n_allocated);
    }
    
    return 0;
}

vbuf* get_vbuf(void)
{
    vbuf* v = NULL;

    {
    	pthread_spin_lock(&vbuf_lock);
    }

    /*
     * It will often be possible for higher layers to recover
     * when no vbuf is available, but waiting for more descriptors
     * to complete. For now, just abort.
     */
    if (NULL == free_vbuf_head)
    {
        if(allocate_vbuf_region(rdma_vbuf_secondary_pool_size) != 0) {
            ibv_va_error_abort(GEN_EXIT_ERR,
                "VBUF reagion allocation failed. Pool size %d\n", vbuf_n_allocated);
        }
    }

    v = free_vbuf_head;
    --num_free_vbuf;
    ++num_vbuf_get;

    /* this correctly handles removing from single entry free list */
    free_vbuf_head = free_vbuf_head->desc.next;

    /* need to change this to RPUT_VBUF_FLAG later
     * if we are doing rput */
    v->padding = NORMAL_VBUF_FLAG;
    v->pheader = (void *)v->buffer;

    /* this is probably not the right place to initialize shandle to NULL.
     * Do it here for now because it will make sure it is always initialized.
     * Otherwise we would need to very carefully add the initialization in
     * a dozen other places, and probably miss one.
     */
    v->sreq = NULL;
    v->coalesce = 0;
    v->content_size = 0;
    v->eager = 0;
    /* Decide which transport need to assign here */
    v->transport = IB_TRANSPORT_RC;

    {
        pthread_spin_unlock(&vbuf_lock);
    }

    return(v);
}

void MRAILI_Release_vbuf(vbuf* v)
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

    if (v->padding != NORMAL_VBUF_FLAG &&
        v->padding != RPUT_VBUF_FLAG &&
        v->padding != RGET_VBUF_FLAG &&
        v->padding != COLL_VBUF_FLAG &&
        v->padding != RDMA_ONE_SIDED)
    {
        ibv_error_abort(GEN_EXIT_ERR, "vbuf not correct.\n");
    }

    /* clear out fields to prepare vbuf for next use */
    *v->head_flag   = 0;
    v->pheader      = NULL;
    v->content_size = 0;
    v->sreq         = NULL;
    v->vc           = NULL;

    /* note this correctly handles appending to empty free list */
    pthread_spin_unlock(&vbuf_lock);
}

static int allocate_ud_vbuf_region(int nvbufs)
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

    struct vbuf_region* reg = (struct vbuf_region*) SPAWN_MALLOC (sizeof(struct vbuf_region));
    
    void *vbuf_dma_buffer = NULL;

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
                g_hca_info.pd,
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
        cur->eager        = 0;
        cur->content_size = 0;
        cur->coalesce     = 0;
    }

    /* insert region into list */
    reg->next = vbuf_region_head;
    vbuf_region_head = reg;

    return 0;
}

int allocate_ud_vbufs(int nvbufs)
{
    return allocate_ud_vbuf_region(nvbufs);
}

vbuf* get_ud_vbuf(void)
{
    vbuf* v = NULL;

    pthread_spin_lock(&vbuf_lock);

    /* if we don't have any free vufs left, try to allocate more */
    if (ud_free_vbuf_head == NULL) {
        if (allocate_ud_vbuf_region(rdma_vbuf_secondary_pool_size) != 0) {
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
    v->sreq         = NULL;
    v->coalesce     = 0;
    v->content_size = 0;
    v->eager        = 0;
    /* TODO: Decide which transport need to assign here */

    pthread_spin_unlock(&vbuf_lock);

    return(v);
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_ud_recv
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_ud_recv(vbuf* v, unsigned long len, int hca_num)
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

#undef FUNCNAME
#define FUNCNAME vbuf_init_rdma_write
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_rdma_write(vbuf* v)
{
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.opcode = IBV_WR_RDMA_WRITE;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.wr_id = (uintptr_t) v;

    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->padding = FREE_FLAG;
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_send
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_send(vbuf* v, unsigned long len, int rail)
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

#undef FUNCNAME
#define FUNCNAME vbuf_init_recv
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_recv(vbuf* v, unsigned long len, int rail)
{
    int hca_num = rail / (rdma_num_rails / rdma_num_hcas);

    assert(v != NULL);

    v->desc.u.rr.next = NULL;
    v->desc.u.rr.wr_id = (uintptr_t) v;
    v->desc.u.rr.num_sge = 1;
    v->desc.u.rr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = len;
    v->desc.sg_entry.lkey = v->region->mem_handle[hca_num]->lkey;
    v->desc.sg_entry.addr = (uintptr_t)(v->buffer);
    v->padding = NORMAL_VBUF_FLAG;
    v->rail = rail;

    return;
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_rget
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_rget(
    vbuf* v,
    void* local_address,
    uint32_t lkey, 
    void* remote_address,
    uint32_t rkey,
    int len,
    int rail)
{
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.opcode = IBV_WR_RDMA_READ;
    v->desc.u.sr.wr_id = (uintptr_t) v;

    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.wr.rdma.remote_addr = (uintptr_t)(remote_address);
    v->desc.u.sr.wr.rdma.rkey = rkey;

    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = len;
    v->desc.sg_entry.lkey = lkey;
    v->desc.sg_entry.addr = (uintptr_t)(local_address);
    v->padding = RGET_VBUF_FLAG;
    v->rail = rail;	
    //DEBUG_PRINT("RDMA Read\n");
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_rput
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_rput(
    vbuf* v,
    void* local_address,
    uint32_t lkey,
    void* remote_address,
    uint32_t rkey,
    int len,
    int rail)
{
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.opcode = IBV_WR_RDMA_WRITE;
    v->desc.u.sr.wr_id = (uintptr_t) v;

    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.wr.rdma.remote_addr = (uintptr_t)(remote_address);
    v->desc.u.sr.wr.rdma.rkey = rkey;

    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = len;
    v->desc.sg_entry.lkey = lkey;
    v->desc.sg_entry.addr = (uintptr_t)(local_address);
    v->padding = RPUT_VBUF_FLAG;
    v->rail = rail;	
    //DEBUG_PRINT("RDMA write\n");
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_rma_get
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_rma_get(vbuf *v, void *l_addr, uint32_t lkey,
                       void *r_addr, uint32_t rkey, int len, int rail)
{
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.opcode = IBV_WR_RDMA_READ;
    v->desc.u.sr.wr_id = (uintptr_t) v;

    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.wr.rdma.remote_addr = (uintptr_t)(r_addr);
    v->desc.u.sr.wr.rdma.rkey = rkey;

    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = len;
    v->desc.sg_entry.lkey = lkey;
    v->desc.sg_entry.addr = (uintptr_t)(l_addr);
    v->padding = RDMA_ONE_SIDED;
    v->rail = rail;
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_rma_put
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_rma_put(vbuf *v, void *l_addr, uint32_t lkey,
                       void *r_addr, uint32_t rkey, int len, int rail)
{
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.opcode = IBV_WR_RDMA_WRITE;
    v->desc.u.sr.wr_id = (uintptr_t) v;

    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.wr.rdma.remote_addr = (uintptr_t)(r_addr);
    v->desc.u.sr.wr.rdma.rkey = rkey;

    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = len;
    v->desc.sg_entry.lkey = lkey;
    v->desc.sg_entry.addr = (uintptr_t)(l_addr);
    v->padding = RDMA_ONE_SIDED;
    v->rail = rail;
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_rma_fetch_and_add 
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_rma_fetch_and_add(vbuf *v, void *l_addr, uint32_t lkey,
        void *r_addr, uint32_t rkey, uint64_t add,
        int rail)
{   
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.wr_id = (uintptr_t) v;

    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.wr.atomic.remote_addr = (uintptr_t)(r_addr);
    v->desc.u.sr.wr.atomic.rkey = rkey;
    v->desc.u.sr.wr.atomic.compare_add = add;

    v->desc.u.sr.sg_list = &(v->desc.sg_entry);
    v->desc.sg_entry.length = sizeof(uint64_t);
    v->desc.sg_entry.lkey = lkey;
    v->desc.sg_entry.addr = (uintptr_t)(l_addr);
    v->padding = RDMA_ONE_SIDED;
    v->rail = rail;
}

#undef FUNCNAME
#define FUNCNAME vbuf_init_rma_compare_and_swap
#undef FCNAME
#define FCNAME MPIDI_QUOTE(FUNCNAME)
void vbuf_init_rma_compare_and_swap(vbuf *v, void *l_addr, uint32_t lkey,
                        void *r_addr, uint32_t rkey, uint64_t compare, 
                        uint64_t swap, int rail)
{
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.wr_id  = (uintptr_t) v;
    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.wr.atomic.remote_addr = (uintptr_t)(r_addr);
    v->desc.u.sr.wr.atomic.rkey = rkey;
    v->desc.u.sr.wr.atomic.compare_add = compare;
    v->desc.u.sr.wr.atomic.swap = swap;

    v->desc.u.sr.sg_list = &(v->desc.sg_entry);

    v->desc.sg_entry.length = sizeof(uint64_t);
    v->desc.sg_entry.lkey = lkey;
    v->desc.sg_entry.addr = (uintptr_t)(v->buffer);

    v->padding = RDMA_ONE_SIDED;
    v->rail = rail;
}

/* vi:set sw=4 tw=80: */
