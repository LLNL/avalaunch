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
#include <ib_internal.h>

mv2_hca_info_t g_hca_info;
mv2_ud_ctx_t *ud_ctx;

/* head of list of allocated vbuf regions */
static vbuf_region *vbuf_region_head = NULL;

static vbuf *ud_free_vbuf_head = NULL;
int ud_vbuf_n_allocated = 0;
long ud_num_free_vbuf = 0;
long ud_num_vbuf_get = 0;
long ud_num_vbuf_freed = 0;
static pthread_spinlock_t vbuf_lock;

/* ===== Beign: VBUF Related functions ===== */
static int alloc_hugepage_region (int *shmid, void **buffer, int *nvbufs,
                                  int buf_size)
{
    int ret = 0;
    size_t size = *nvbufs * buf_size;
    MRAILI_ALIGN_LEN(size, HUGEPAGE_ALIGN);

    /* create hugepage shared region */
    *shmid = shmget(IPC_PRIVATE, size, 
                        SHM_HUGETLB | IPC_CREAT | SHM_R | SHM_W);
    if (*shmid < 0) {
        goto fn_fail;
    }

    /* attach shared memory */
    *buffer = (void *) shmat(*shmid, SHMAT_ADDR, SHMAT_FLAGS);
    if (*buffer == (void *) -1) {
        goto fn_fail;
    }
    
    /* Mark shmem for removal */
    if (shmctl(*shmid, IPC_RMID, 0) != 0) {
        fprintf(stderr, "Failed to mark shm for removal\n");
    }
    
    /* Find max no.of vbufs can fit in allocated buffer */
    *nvbufs = size / buf_size;
     
fn_exit:
    return ret;
fn_fail:
    ret = -1;
    fprintf(stderr,"Failed to allocate buffer from huge pages. "
                   "fallback to regular pages. requested buf size:%lu\n", size);
    goto fn_exit;
}    

static int allocate_ud_vbuf_region(int nvbufs)
{
    struct vbuf_region *reg = NULL;
    void *mem = NULL;
    int i = 0;
    vbuf *cur = NULL;
    void *vbuf_dma_buffer = NULL;
    int alignment_vbuf = 64;
    int alignment_dma = getpagesize();
    int result = 0;

    //PRINT_DEBUG(DEBUG_UD_verbose>0,"Allocating a UD buf region.\n");

    if (ud_free_vbuf_head != NULL)
    {
        fprintf(stderr, "free_vbuf_head = NULL");
        return -1;
    }

    reg = (struct vbuf_region *) SPAWN_MALLOC (sizeof(struct vbuf_region));

    if (NULL == reg)
    {
        fprintf(stderr, "Unable to malloc a new struct vbuf_region");
        return -1;
    }
    
    {
        result = alloc_hugepage_region(&reg->shmid, &vbuf_dma_buffer, &nvbufs,
                                        RDMA_DEFAULT_UD_MTU);
    }

    /* do posix_memalign if enable hugepage disabled or failed */
    if (result != 0 )  
    {
        reg->shmid = -1;
        result = posix_memalign(&vbuf_dma_buffer, 
            alignment_dma, nvbufs * RDMA_DEFAULT_UD_MTU);
    }

    if ((result!=0) || (NULL == vbuf_dma_buffer))
    {
        fprintf(stderr, "unable to malloc vbufs DMA buffer");
        return -1;
    }
    
    if (posix_memalign(
                (void**) &mem,
                alignment_vbuf,
                nvbufs * sizeof(vbuf)))
    {
        fprintf(stderr, "[%s %d] Cannot allocate vbuf region\n", 
                __FILE__, __LINE__);
        return -1;
    }

    memset(mem, 0, nvbufs * sizeof(vbuf));
    memset(vbuf_dma_buffer, 0, nvbufs * RDMA_DEFAULT_UD_MTU);

    ud_free_vbuf_head       = mem;

    ud_num_free_vbuf        += nvbufs;
    ud_vbuf_n_allocated     += nvbufs;

    reg->malloc_start       = mem;
    reg->malloc_buf_start   = vbuf_dma_buffer;
    reg->malloc_end         = (void *) ((char *) mem + nvbufs * sizeof(vbuf));
    reg->malloc_buf_end     = (void *) ((char *) vbuf_dma_buffer + 
                                nvbufs * RDMA_DEFAULT_UD_MTU);
    reg->count              = nvbufs;
    reg->vbuf_head          = ud_free_vbuf_head;

#if 0
    PRINT_DEBUG(DEBUG_UD_verbose>0,
            "VBUF REGION ALLOCATION SZ %d TOT %d FREE %ld NF %ld NG %ld\n",
            RDMA_DEFAULT_UD_MTU,
            ud_vbuf_n_allocated,
            ud_num_free_vbuf,
            ud_num_vbuf_freed,
            ud_num_vbuf_get);
#endif

    /* region should be registered with all hcas */
    {
        reg->mem_handle[i] = ibv_reg_mr(
                g_hca_info.pd,
                vbuf_dma_buffer,
                nvbufs * RDMA_DEFAULT_UD_MTU,
                IBV_ACCESS_LOCAL_WRITE);

        if (!reg->mem_handle[i])
        {
            fprintf(stderr, "[%s %d] Cannot register vbuf region\n", 
                    __FILE__, __LINE__);
            return -1;
        }
    }

    /* init the free list */
    for (i = 0; i < nvbufs; ++i) {
        cur                 = ud_free_vbuf_head + i;
        cur->desc.next      = ud_free_vbuf_head + i + 1;
        if (i == (nvbufs -1)) {
            cur->desc.next  = NULL;
        }
        cur->region         = reg;
        cur->head_flag      = (VBUF_FLAG_TYPE *) ((char *)vbuf_dma_buffer +
                    (i + 1) * RDMA_DEFAULT_UD_MTU - sizeof * cur->head_flag);
        cur->buffer         = (unsigned char *) ((char *)vbuf_dma_buffer +
                                i * RDMA_DEFAULT_UD_MTU);
        cur->eager          = 0;
        cur->content_size   = 0;
        cur->coalesce       = 0;
    }

    /* thread region list */
    reg->next               = vbuf_region_head;
    vbuf_region_head        = reg;

    return 0;
}

static int allocate_ud_vbufs(int nvbufs)
{
    return allocate_ud_vbuf_region(nvbufs);
}

vbuf* get_ud_vbuf(void)
{
    vbuf* v = NULL;

    {
        pthread_spin_lock(&vbuf_lock);
    }

    if (NULL == ud_free_vbuf_head)
    {
        if(allocate_ud_vbuf_region(RDMA_VBUF_SECONDARY_POOL_SIZE) != 0) {
            fprintf(stderr,
                    "UD VBUF reagion allocation failed. Pool size %d\n", ud_vbuf_n_allocated);
            return NULL;
        }
    }

    v = ud_free_vbuf_head;
    --ud_num_free_vbuf;
    ++ud_num_vbuf_get;

    /* this correctly handles removing from single entry free list */
    ud_free_vbuf_head = ud_free_vbuf_head->desc.next;

    /* need to change this to RPUT_VBUF_FLAG later
     * if we are doing rput */
    v->padding = NORMAL_VBUF_FLAG;
    v->pheader = (void *)v->buffer;
    v->transport = IB_TRANSPORT_UD;
    v->retry_count = 0;
    v->flags = 0;
    v->pending_send_polls = 0;

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

    {
        pthread_spin_unlock(&vbuf_lock);
    }

    return(v);
}

static void vbuf_init_ud_recv(vbuf* v, unsigned long len, int hca_num)
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

    return;
}

static int init_vbuf_lock(void)
{
    if (pthread_spin_init(&vbuf_lock, 0)) {
        fprintf(stderr, "Cannot init VBUF lock\n");
        return -1;
    }

    return 0;
}

static void MRAILI_Release_vbuf(vbuf* v)
{
    /* This message might be in progress. Wait for ib send completion 
     * to release this buffer to avoid to reusing buffer
     */
    {
        if(v->transport== IB_TRANSPORT_UD 
                && (v->flags & UD_VBUF_SEND_INPROGRESS)) {
            v->flags |= UD_VBUF_FREE_PENIDING;
            return;
        }
    }

    {
        pthread_spin_lock(&vbuf_lock);
    }

    //DEBUG_PRINT("release_vbuf: releasing %p previous head = %p, padding %d\n", v, free_vbuf_head, v->padding);

    {
        assert(v != ud_free_vbuf_head);
        v->desc.next = ud_free_vbuf_head;
        ud_free_vbuf_head = v;
        ++ud_num_free_vbuf;
        ++ud_num_vbuf_freed;
    } 

    if (v->padding != NORMAL_VBUF_FLAG
        && v->padding != RPUT_VBUF_FLAG
        && v->padding != RGET_VBUF_FLAG
        && v->padding != COLL_VBUF_FLAG
        && v->padding != RDMA_ONE_SIDED)
    {
        fprintf(stderr, "vbuf not correct.\n");
        exit(-1);
    }

    *v->head_flag = 0;
    v->pheader = NULL;
    v->content_size = 0;
    v->sreq = NULL;
    v->vc = NULL;

    {
        pthread_spin_unlock(&vbuf_lock);
    }
}

static int mv2_post_ud_recv_buffers(int num_bufs, mv2_ud_ctx_t *ud_ctx)
{
    int i = 0,ret = 0;
    vbuf* v = NULL;
    struct ibv_recv_wr* bad_wr = NULL;

    if (num_bufs > RDMA_DEFAULT_MAX_UD_RECV_WQE)
    {
        fprintf(stderr,
                "Try to post %d to UD recv buffers, max %d\n",
                num_bufs, RDMA_DEFAULT_MAX_UD_RECV_WQE);
        return -1;
    }

    for (; i < num_bufs; ++i)
    {
        if ((v = get_ud_vbuf()) == NULL)
        {
            break;
        }

        vbuf_init_ud_recv(v, RDMA_DEFAULT_UD_MTU, 0);
        v->transport = IB_TRANSPORT_UD;
        if (ud_ctx->qp->srq) {
            ret = ibv_post_srq_recv(ud_ctx->qp->srq, &v->desc.u.rr, &bad_wr);
        } else {
            ret = ibv_post_recv(ud_ctx->qp, &v->desc.u.rr, &bad_wr);
        }
        if (ret)
        {
            MRAILI_Release_vbuf(v);
            break;
        }
    }

#if 0
    PRINT_DEBUG(DEBUG_UD_verbose>0 ,"Posted %d buffers of size:%d to UD QP\n",
                num_bufs, RDMA_DEFAULT_UD_MTU);
#endif

    return i;
}
/* ===== End: VBUF Related functions ===== */

/* ===== Begin: Initialization functions ===== */
/* Get HCA parameters */
static int mv2_get_hca_info(int devnum, mv2_hca_info_t *hca_info)
{
    int i = 0, j = 0, retval;
    int num_devices = 0;
    struct ibv_device **dev_list = NULL;

    dev_list = ibv_get_device_list(&num_devices);

    for (i = 0; i < num_devices; i++) {
        if (j == devnum) {
            hca_info->device = dev_list[i];
            break;
        }
        j++;
    }

    /* Open the HCA for communication */
    hca_info->context = ibv_open_device(hca_info->device);
    if (!hca_info->context) {
        fprintf(stderr, "Cannot create context for HCA\n");
        return -1;
    }

    /* Create a protection domain for communication */
    hca_info->pd = ibv_alloc_pd(hca_info->context);
    if (!hca_info->pd) {
        fprintf(stderr, "Cannot create PD for HCA\n");
        return -1;
    }
    
    /* Get the attributes of the HCA */
    retval = ibv_query_device(g_hca_info.context, &g_hca_info.device_attr);
    if (retval) {
        fprintf(stderr, "Cannot query HCA\n");
        return -1;
    }

    /* Get the attributes of the port */
    for (i = 0; i < MAX_NUM_PORTS; ++i) {
        retval = ibv_query_port(g_hca_info.context, i, &g_hca_info.port_attr[i]);
    }
    
    /* Create completion channel */
    hca_info->comp_channel = ibv_create_comp_channel(hca_info->context);
    if (!hca_info->comp_channel) {
        fprintf(stderr, "Cannot create completion channel\n");
        return -1;
    }

    /* Create completion queue */
    hca_info->cq_hndl = ibv_create_cq(hca_info->context,
                                      RDMA_DEFAULT_MAX_CQ_SIZE, NULL,
                                      hca_info->comp_channel, 0);
    if (!hca_info->cq_hndl) {
        fprintf(stderr, "Cannot create completion queue\n");
        return -1;
    }

    ibv_free_device_list(dev_list);

    return 0;
}

/* Open HCA for communication */
int mv2_hca_open()
{
    memset(&g_hca_info, 0, sizeof(mv2_hca_info_t));
    if (0!= mv2_get_hca_info(0, &g_hca_info)){
        fprintf(stderr, "Failed to initialize HCA\n");
        return -1;
    }
    return 0;
}

/* Transition UD QP */
static int mv2_ud_qp_transition(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(struct ibv_qp_attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = RDMA_DEFAULT_PORT;
    attr.qkey = 0;

    if (ibv_modify_qp(qp, &attr,
                IBV_QP_STATE |
                IBV_QP_PKEY_INDEX |
                IBV_QP_PORT | IBV_QP_QKEY)) {
            fprintf(stderr,"Failed to modify QP to INIT\n");
            return 1;
    }    
        
    memset(&attr, 0, sizeof(struct ibv_qp_attr));

    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(qp, &attr, IBV_QP_STATE)) {
            fprintf(stderr, "Failed to modify QP to RTR\n");
            return 1;
    }   

    memset(&attr, 0, sizeof(struct ibv_qp_attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = RDMA_DEFAULT_PSN;
    if (ibv_modify_qp(qp, &attr,
                IBV_QP_STATE | IBV_QP_SQ_PSN)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;

}

/* Create UD QP */
static struct ibv_qp * mv2_ud_create_qp(mv2_ud_qp_info_t *qp_info)
{
    struct ibv_qp *qp;
    struct ibv_qp_init_attr init_attr;
 
    memset(&init_attr, 0, sizeof(struct ibv_qp_init_attr));
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

    qp = ibv_create_qp(qp_info->pd, &init_attr);
    if(!qp) {
        fprintf(stderr,"error in creating UD qp\n");
        return NULL;
    }
    
    if (mv2_ud_qp_transition(qp)) {
        return NULL;
    }

    //PRINT_DEBUG(DEBUG_UD_verbose>0," UD QP:%p qpn:%d \n",qp, qp->qp_num);

    return qp;
}

/* Create UD Context */
static mv2_ud_ctx_t* mv2_ud_create_ctx (mv2_ud_qp_info_t *qp_info)
{
    mv2_ud_ctx_t *ctx;

    ctx = SPAWN_MALLOC( sizeof(mv2_ud_ctx_t) );
    if (!ctx){
        fprintf( stderr, "%s:no memory!\n", __func__ );
        return NULL;
    }
    memset( ctx, 0, sizeof(mv2_ud_ctx_t) );

    ctx->qp = mv2_ud_create_qp(qp_info);
    if(!ctx->qp) {
        fprintf(stderr, "Error in creating UD QP\n");
        return NULL;
    }

    return ctx;
}

/* Initialize UD Context */
int mv2_init_ud()
{
    mv2_ud_qp_info_t qp_info;   

    /* Init lock for vbuf */
    init_vbuf_lock();

    /* Allocate vbufs */
    allocate_ud_vbufs(RDMA_DEFAULT_NUM_VBUFS);

    qp_info.pd                  = g_hca_info.pd;
    qp_info.srq                 = NULL;
    qp_info.sq_psn              = RDMA_DEFAULT_PSN;
    qp_info.send_cq             = g_hca_info.cq_hndl;
    qp_info.recv_cq             = g_hca_info.cq_hndl;
    qp_info.cap.max_send_wr     = RDMA_DEFAULT_MAX_UD_SEND_WQE;
    qp_info.cap.max_recv_wr     = RDMA_DEFAULT_MAX_UD_RECV_WQE;
    qp_info.cap.max_send_sge    = RDMA_DEFAULT_MAX_SG_LIST;
    qp_info.cap.max_recv_sge    = RDMA_DEFAULT_MAX_SG_LIST;
    qp_info.cap.max_inline_data = RDMA_DEFAULT_MAX_INLINE_SIZE;

    ud_ctx = mv2_ud_create_ctx(&qp_info);
    if (!ud_ctx) {          
        fprintf(stderr, "Error in create UD qp\n");
        return -1;
    }
    
    ud_ctx->send_wqes_avail     = RDMA_DEFAULT_MAX_UD_SEND_WQE - 50;
    ud_ctx->ext_sendq_count     = 0;
    MESSAGE_QUEUE_INIT(&ud_ctx->ext_send_queue);

    ud_ctx->hca_num             = 0;
    ud_ctx->num_recvs_posted    = 0;
    ud_ctx->credit_preserve     = (RDMA_DEFAULT_MAX_UD_RECV_WQE / 4);
    ud_ctx->num_recvs_posted    += mv2_post_ud_recv_buffers(
             (RDMA_DEFAULT_MAX_UD_RECV_WQE - ud_ctx->num_recvs_posted), ud_ctx);
    MESSAGE_QUEUE_INIT(&ud_ctx->unack_queue);

    return 0;
}

/* Create UD VC */
int mv2_ud_set_vc_info (mv2_ud_vc_info_t *ud_vc_info, mv2_ud_exch_info_t *rem_info, struct ibv_pd *pd, int port)
{
    struct ibv_ah_attr ah_attr;

    //PRINT_DEBUG(DEBUG_UD_verbose>0,"lid:%d\n", rem_info->lid );
    
    memset(&ah_attr, 0, sizeof(ah_attr));
    ah_attr.is_global = 0; 
    ah_attr.dlid = rem_info->lid;
    ah_attr.sl = RDMA_DEFAULT_SERVICE_LEVEL;
    ah_attr.src_path_bits = 0; 
    ah_attr.port_num = port;

    ud_vc_info->ah = ibv_create_ah(pd, &ah_attr);
    if(!(ud_vc_info->ah)){    
        fprintf(stderr, "Error in creating address handle\n");
        return -1;
    }
    ud_vc_info->lid = rem_info->lid;
    ud_vc_info->qpn = rem_info->qpn;
    return 0;
}

/* Destroy UD Context */
void mv2_ud_destroy_ctx (mv2_ud_ctx_t *ctx)
{
    if (ctx->qp) {
        ibv_destroy_qp(ctx->qp);
    }
    spawn_free(ctx);
}
/* ===== End: Initialization functions ===== */

/* ===== Begin: Send/Recv functions ===== */
int mv2_poll_cq()
{
    int ne = 0;
    struct ibv_wc wc;
    struct ibv_cq *cq = g_hca_info.cq_hndl;

    /* poll cq */
    ne = ibv_poll_cq(cq, 1, &wc);
    if ( 1 == ne ) {
        if ( IBV_WC_SUCCESS != wc.status ) {
            fprintf(stderr, "IBV_WC_SUCCESS != wc.status (%d)\n", wc.status);
            return -1;
        }
        switch (wc.opcode) {
            case IBV_WC_SEND:
            case IBV_WC_RDMA_READ:
            case IBV_WC_RDMA_WRITE:
                break;
            case IBV_WC_RECV:
                break;
            default:
                fprintf(stderr, "Invalid opcode from ibv_poll_cq()\n");
                break;
        }
    } else if ( ne < 0 ){
        fprintf(stderr, "poll cq error\n");
        return -1;
    } else {
        return 1;   /* indicates i got nothing! */
    }
}

int mv2_wait_on_channel()
{
    void *ev_ctx = NULL;
    struct ibv_cq *ev_cq = NULL;

    /* Wait for the completion event */
    if (ibv_get_cq_event(g_hca_info.comp_channel, &ev_cq, &ev_ctx)) {
        fprintf(stderr, "Failed to get cq_event\n");
        return 1;
    }

    /* Ack the event */
    ibv_ack_cq_events(ev_cq, 1);

    /* Request notification upon the next completion event */
    if (ibv_req_notify_cq(ev_cq, 0)) {
        fprintf(stderr, "Couldn't request CQ notification\n");
        return 1;
    }

    mv2_poll_cq();
    return 0;
}
/* ===== End: Send/Recv functions ===== */
