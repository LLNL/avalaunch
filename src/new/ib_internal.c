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
#include <spawn_net.h>
#include <ib_internal.h>
#include <mv2_ud.h>
#include <mv2_ud_inline.h>
#include <debug_utils.h>

int num_rdma_buffer;
int rdma_num_rails = 1;
int rdma_num_hcas = 1;
int rdma_vbuf_max = -1;
int rdma_enable_hugepage = 1;
int rdma_vbuf_total_size;
uint16_t rdma_default_ud_mtu = 2048;
uint8_t rdma_enable_hybrid = 1;
uint8_t rdma_enable_only_ud = 0;
uint8_t rdma_use_ud_zcopy = 1;
int rdma_vbuf_secondary_pool_size = RDMA_VBUF_SECONDARY_POOL_SIZE;
int rdma_max_inline_size = RDMA_DEFAULT_MAX_INLINE_SIZE;
uint32_t rdma_default_max_ud_send_wqe = RDMA_DEFAULT_MAX_UD_SEND_WQE;
uint32_t rdma_default_max_ud_recv_wqe = RDMA_DEFAULT_MAX_UD_RECV_WQE;
uint32_t rdma_ud_num_msg_limit = RDMA_UD_NUM_MSG_LIMIT;
uint32_t rdma_ud_vbuf_pool_size = RDMA_UD_VBUF_POOL_SIZE;
/* Maximum number of outstanding buffers (waiting for ACK)*/
uint32_t rdma_default_ud_sendwin_size = 400;
/* Maximum number of out-of-order messages that will be buffered */
uint32_t rdma_default_ud_recvwin_size = 2501;
/* Time (usec) until ACK status is checked (and ACKs are sent) */
long rdma_ud_progress_timeout = 48000;
/* Time (usec) until a message is resent */
long rdma_ud_retry_timeout = 500000;
long rdma_ud_max_retry_timeout = 20000000;
long rdma_ud_last_check;
long rdma_ud_retransmissions=0;
uint32_t rdma_ud_zcopy_threshold;
uint32_t rdma_ud_zcopy_rq_size = 4096;
uint32_t rdma_hybrid_enable_threshold = 1024;
uint16_t rdma_ud_progress_spin = 1200;
uint16_t rdma_ud_max_retry_count = 1000;
uint16_t rdma_ud_max_ack_pending;
uint16_t rdma_ud_num_rndv_qps = 64;
uint16_t rdma_hybrid_max_rc_conn = 64;
uint16_t rdma_hybrid_pending_rc_conn = 0;

struct timespec remain;
struct timespec cm_timeout;
mv2_proc_info_t proc;
mv2_hca_info_t g_hca_info;
mv2_ud_exch_info_t local_ep_info;
MPIDI_VC_t *ud_vc_info = NULL; 
static pthread_mutex_t comm_lock_object;
static pthread_t comm_thread;

int mv2_wait_on_channel();
int mv2_ud_init_vc (int rank);
void* cm_timeout_handler(void *arg);
int mv2_post_ud_recv_buffers(int num_bufs, mv2_ud_ctx_t *ud_ctx);

/* ===== Begin: Initialization functions ===== */
/* Get HCA parameters */
static int mv2_get_hca_info(int devnum, mv2_hca_info_t *hca_info)
{
    int i;

    /* get list of HCA devices */
    int num_devices;
    struct ibv_device** dev_list = ibv_get_device_list(&num_devices);
    if (dev_list == NULL) {
        SPAWN_ERR("Failed to get device list (ibv_get_device_list errno=%d %s)", errno, strerror(errno));
        return -1;
    }

    /* check that caller's requested device is within range */
    if (devnum >= num_devices) {
        SPAWN_ERR("Requested device number %d higher than max devices %d", devnum, num_devices);
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
        SPAWN_ERR("Cannot query HCA (ibv_query_device errno=%d %s)", retval, strerror(retval));
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
        retval = ibv_query_port(context, i, &ports[i]);
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

/* Open HCA for communication */
int mv2_hca_open()
{
    memset(&g_hca_info, 0, sizeof(mv2_hca_info_t));
    if (mv2_get_hca_info(0, &g_hca_info) != 0){
        SPAWN_ERR("Failed to initialize HCA");
        return -1;
    }
    return 0;
}

/* Transition UD QP */
int mv2_ud_qp_transition(struct ibv_qp *qp)
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
struct ibv_qp * mv2_ud_create_qp(mv2_ud_qp_info_t *qp_info)
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
    if(qp == NULL) {
        /* TODO: man page doesn't say anything about errno values */
        SPAWN_ERR("error in creating UD qp");
        return NULL;
    }
    
    if (mv2_ud_qp_transition(qp)) {
        return NULL;
    }

    //PRINT_DEBUG(DEBUG_UD_verbose>0," UD QP:%p qpn:%d \n",qp, qp->qp_num);

    return qp;
}

/* Create UD Context */
mv2_ud_ctx_t* mv2_ud_create_ctx (mv2_ud_qp_info_t *qp_info)
{
    mv2_ud_ctx_t *ctx;

    ctx = SPAWN_MALLOC( sizeof(mv2_ud_ctx_t) );
    memset( ctx, 0, sizeof(mv2_ud_ctx_t) );

    ctx->qp = mv2_ud_create_qp(qp_info);
    if(!ctx->qp) {
        SPAWN_ERR("Error in creating UD QP");
        return NULL;
    }

    return ctx;
}

/* Initialize UD Context */
spawn_net_endpoint* mv2_init_ud(int nchild)
{
    int i = 0;
    int ret = 0;
    pthread_attr_t attr;
    mv2_ud_qp_info_t qp_info;   
    spawn_net_endpoint *ep = SPAWN_NET_ENDPOINT_NULL;

    /* Init lock for vbuf */
    init_vbuf_lock();

    ret = pthread_mutex_init(&comm_lock_object, 0);
    if (ret != 0) {
        SPAWN_ERR("Failed to init comm_lock_object (pthread_mutex_init ret=%d %s)", ret, strerror(ret));
        ibv_error_abort(-1, "Failed to init comm_lock_object\n");
    }   

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

    proc.ud_ctx = mv2_ud_create_ctx(&qp_info);
    if (!proc.ud_ctx) {          
        SPAWN_ERR("Error in create UD qp");
        return SPAWN_NET_ENDPOINT_NULL;
    }
    
    proc.ud_ctx->send_wqes_avail     = RDMA_DEFAULT_MAX_UD_SEND_WQE - 50;
    proc.ud_ctx->ext_sendq_count     = 0;
    MESSAGE_QUEUE_INIT(&proc.ud_ctx->ext_send_queue);

    proc.ud_ctx->hca_num             = 0;
    proc.ud_ctx->num_recvs_posted    = 0;
    proc.ud_ctx->credit_preserve     = (RDMA_DEFAULT_MAX_UD_RECV_WQE / 4);
    proc.ud_ctx->num_recvs_posted    += mv2_post_ud_recv_buffers(
             (RDMA_DEFAULT_MAX_UD_RECV_WQE - proc.ud_ctx->num_recvs_posted), proc.ud_ctx);
    MESSAGE_QUEUE_INIT(&proc.unack_queue);

    ud_vc_info = SPAWN_MALLOC(sizeof(MPIDI_VC_t) * nchild);

    for (i = 0; i < nchild; ++i) {
        mv2_ud_init_vc(i);
    }

    proc.post_send = post_ud_send;

    /* Create end point */
    local_ep_info.lid = g_hca_info.port_attr[0].lid;
    local_ep_info.qpn = proc.ud_ctx->qp->qp_num;

    ep = SPAWN_MALLOC(sizeof(spawn_net_endpoint));
    ep->name = SPAWN_MALLOC(sizeof(char)*RDMA_CONNECTION_INFO_LEN);
    ep->data = SPAWN_MALLOC(sizeof(mv2_ud_exch_info_t));

    /* Populate spawn_net_endpoint with correct data */
    ep->type = SPAWN_NET_TYPE_IB;

    sprintf(ep->name, "%06x:%04x:%06x", PG_RANK, local_ep_info.lid, local_ep_info.qpn);

    memcpy(ep->data, &local_ep_info, sizeof(mv2_ud_exch_info_t));

    /*Spawn comm thread */
    if (pthread_attr_init(&attr)) {
        SPAWN_ERR("Unable to init thread attr");
        return SPAWN_NET_ENDPOINT_NULL;
    }

    ret = pthread_attr_setstacksize(&attr, DEFAULT_CM_THREAD_STACKSIZE);
    if (ret && ret != EINVAL) {
        SPAWN_ERR("Unable to set stack size");
        return SPAWN_NET_ENDPOINT_NULL;
    }

    pthread_create(&comm_thread, &attr, cm_timeout_handler, NULL);

    return ep;
}

/* Create UD VC */
int mv2_ud_init_vc (int rank)
{
    ud_vc_info[rank].mrail.state = MRAILI_INIT;

    ud_vc_info[rank].mrail.ack_need_tosend = 0;
    ud_vc_info[rank].mrail.seqnum_next_tosend = 0;
    ud_vc_info[rank].mrail.seqnum_next_torecv = 0;
    ud_vc_info[rank].mrail.seqnum_next_toack = UINT16_MAX;

    ud_vc_info[rank].mrail.ud.cntl_acks = 0; 
    ud_vc_info[rank].mrail.ud.ack_pending = 0;
    ud_vc_info[rank].mrail.ud.resend_count = 0;
    ud_vc_info[rank].mrail.ud.total_messages = 0;
    ud_vc_info[rank].mrail.ud.ext_win_send_count = 0;

    MESSAGE_QUEUE_INIT(&ud_vc_info[rank].mrail.ud.send_window);
    MESSAGE_QUEUE_INIT(&ud_vc_info[rank].mrail.ud.ext_window);
    MESSAGE_QUEUE_INIT(&ud_vc_info[rank].mrail.ud.recv_window);
    MESSAGE_QUEUE_INIT(&ud_vc_info[rank].mrail.app_recv_window);

    return 0;
}

int mv2_ud_set_vc_info (int rank, mv2_ud_exch_info_t *rem_info, int port)
{
    struct ibv_ah_attr ah_attr;

    assert(rank < PG_SIZE);

    if (ud_vc_info[rank].mrail.state == MRAILI_UD_CONNECTING ||
        ud_vc_info[rank].mrail.state == MRAILI_UD_CONNECTED) {
        /* Duplicate message - return */
        return 0;
    }

    //PRINT_DEBUG(DEBUG_UD_verbose>0,"lid:%d\n", rem_info->lid );
    
    memset(&ah_attr, 0, sizeof(ah_attr));

    ah_attr.sl              = RDMA_DEFAULT_SERVICE_LEVEL;
    ah_attr.dlid            = rem_info->lid;
    ah_attr.port_num        = port;
    ah_attr.is_global       = 0; 
    ah_attr.src_path_bits   = 0; 

    ud_vc_info[rank].mrail.ud.ah = ibv_create_ah(g_hca_info.pd, &ah_attr);
    if(!(ud_vc_info[rank].mrail.ud.ah)){    
        SPAWN_ERR("Error in creating address handle\n");
        return -1;
    }

    ud_vc_info[rank].mrail.state = MRAILI_UD_CONNECTING;

    ud_vc_info[rank].mrail.ud.lid = rem_info->lid;
    ud_vc_info[rank].mrail.ud.qpn = rem_info->qpn;

    return 0;
}

/* Destroy UD Context */
void mv2_ud_destroy_ctx (mv2_ud_ctx_t *ctx)
{
    if (ctx->qp) {
        ibv_destroy_qp(ctx->qp);
    }
    spawn_free(ctx);
    spawn_free(ud_vc_info);

    pthread_cancel(comm_thread);
}
/* ===== End: Initialization functions ===== */

/* ===== Begin: Send/Recv functions ===== */
/*Interface to lock/unlock connection manager*/
void comm_lock(void)
{           
    pthread_mutex_lock(&comm_lock_object);
}               
            
void comm_unlock(void)
{           
    pthread_mutex_unlock(&comm_lock_object);
}               

void* cm_timeout_handler(void *arg)
{
    int nspin = 0;

    cm_timeout.tv_sec = rdma_ud_progress_timeout / 1000000;
    cm_timeout.tv_nsec = (rdma_ud_progress_timeout - cm_timeout.tv_sec * 1000000) * 1000;

    while(1) {
        comm_unlock();
        nanosleep(&cm_timeout, &remain);
        comm_lock();
        for (nspin = 0; nspin < rdma_ud_progress_spin; nspin++) {
            mv2_poll_cq();
        }
        if (UD_ACK_PROGRESS_TIMEOUT) {
            mv2_check_resend();
            MV2_UD_SEND_ACKS();
            rdma_ud_last_check = mv2_get_time_us();
        }
    }

    return NULL;
}

int MRAILI_Process_send(void *vbuf_addr)
{
    vbuf *v = vbuf_addr;

    if (v->padding == NORMAL_VBUF_FLAG) {
        MRAILI_Release_vbuf(v);
    } else {
        printf("Couldn't release VBUF; v->padding = %d\n", v->padding);
    }

    return 0;
}

void MPIDI_CH3I_MRAIL_Release_vbuf(vbuf * v)
{
    v->eager = 0;
    v->coalesce = 0;
    v->content_size = 0;

    if (v->padding == NORMAL_VBUF_FLAG || v->padding == RPUT_VBUF_FLAG)
        MRAILI_Release_vbuf(v);
#if 0
    else {
        MRAILI_Release_recv_rdma(v);
        MRAILI_Send_noop_if_needed((MPIDI_VC_t *) v->vc, v->rail);
    }
#endif
}

int mv2_post_ud_recv_buffers(int num_bufs, mv2_ud_ctx_t *ud_ctx)
{
    int i = 0,ret = 0;
    vbuf* v = NULL;
    struct ibv_recv_wr* bad_wr = NULL;

    if (num_bufs > rdma_default_max_ud_recv_wqe)
    {
        ibv_va_error_abort(
                GEN_ASSERT_ERR,
                "Try to post %d to UD recv buffers, max %d\n",
                num_bufs, rdma_default_max_ud_recv_wqe);
    }

    for (; i < num_bufs; ++i)
    {
        if ((v = get_ud_vbuf()) == NULL)
        {
            break;
        }

        vbuf_init_ud_recv(v, rdma_default_ud_mtu, 0);
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

    PRINT_DEBUG(DEBUG_UD_verbose>0 ,"Posted %d buffers of size:%d to UD QP\n",num_bufs, rdma_default_ud_mtu);

    return i;
}

int mv2_send_connect_message(MPIDI_VC_t *vc)
{
    int avail = 0;
    MPIDI_CH3I_MRAILI_Pkt_comm_header *connect_pkt = NULL;
    char conn_info[RDMA_CONNECTION_INFO_LEN];
    vbuf *v = get_ud_vbuf();
    void *ptr = (v->buffer + v->content_size);

    sprintf(conn_info, "%06x:%04x:%06x", PG_RANK, local_ep_info.lid, local_ep_info.qpn);

    avail = MRAIL_MAX_UD_SIZE - v->content_size;

    assert (avail >= RDMA_CONNECTION_INFO_LEN);

    memcpy(ptr, conn_info, sizeof(conn_info));
    v->content_size += RDMA_CONNECTION_INFO_LEN;

    connect_pkt = v->pheader;

    MPIDI_Pkt_init(connect_pkt, MPIDI_CH3_PKT_UD_CONNECT);
    connect_pkt->acknum = vc->mrail.seqnum_next_toack;
    connect_pkt->rail = v->rail;

    vbuf_init_send(v, sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header), v->rail);

    proc.post_send(vc, v, 0, NULL);

    return 0;
}

spawn_net_channel* mv2_ep_connect(const char *name)
{
    int parsed = 0;
    int dest_rank = -1;
    MPIDI_VC_t *vc = NULL;
    mv2_ud_exch_info_t ep_info;
    spawn_net_channel* ch = SPAWN_NET_CHANNEL_NULL;

    parsed = sscanf(name, "%06x:%04x:%06x", &dest_rank, &ep_info.lid, &ep_info.qpn);
    if (parsed != 3) {
        SPAWN_ERR("Couldn't parse ep info from %s\n", name);
        return SPAWN_NET_CHANNEL_NULL;
    }

    ch = SPAWN_MALLOC(sizeof(spawn_net_channel));
    ch->name = SPAWN_MALLOC(sizeof(char)*RDMA_CONNECTION_INFO_LEN);
    ch->data = SPAWN_MALLOC(sizeof(mv2_ud_exch_info_t));

    /* Populate spawn_net_channel with information */
    ch->type = SPAWN_NET_TYPE_IB;
    sprintf(ch->name, "%06x:%04x:%06x", dest_rank, local_ep_info.lid, local_ep_info.qpn);
    memcpy(ch->data, &ep_info, sizeof(mv2_ud_exch_info_t));

    /* Set VC info */
    mv2_ud_set_vc_info(dest_rank, &ep_info, RDMA_DEFAULT_PORT);

    MV2_Get_vc(dest_rank, &vc);

    /* Send connect message to destination */
    mv2_send_connect_message(vc);

    /* Change state to connected */
    ud_vc_info[dest_rank].mrail.state = MRAILI_UD_CONNECTED;

    return ch;
}

int mv2_ud_send_internal(MPIDI_VC_t *vc, const void* buf, size_t size)
{
    int avail = 0;
    MPIDI_CH3I_MRAILI_Pkt_comm_header *pkt = NULL;
    vbuf *v = get_ud_vbuf();

    /* Offset for the packet header */
    v->content_size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);

    void *ptr = (v->buffer + v->content_size);

    avail = MRAIL_MAX_UD_SIZE - v->content_size;

    assert (avail >= size);

    memcpy(ptr, buf, size);
    v->content_size += size;

    pkt = v->pheader;

    MPIDI_Pkt_init(pkt, MPIDI_CH3_PKT_UD_DATA);
    pkt->acknum = vc->mrail.seqnum_next_toack;
    pkt->rail = v->rail;

    vbuf_init_send(v, sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header), v->rail);

    if (vc->mrail.state != MRAILI_UD_CONNECTED) {
        mv2_ud_ext_window_add(&vc->mrail.ud.ext_window, v);
    }

    proc.post_send(vc, v, 0, NULL);

    return 0;
}

int mv2_ud_send(const spawn_net_channel* ch, const void* buf, size_t size)
{
    int ret = 0;
    int parsed = 0;
    int dest_rank = -1;
    MPIDI_VC_t *vc = NULL;
    mv2_ud_exch_info_t ep_info;

    parsed = sscanf(ch->name, "%06x:%04x:%06x", &dest_rank, &ep_info.lid, &ep_info.qpn);
    if (parsed != 3) {
        SPAWN_ERR("Couldn't parse ep info from %s\n", ch->name);
        return NULL;
    }

    MV2_Get_vc(dest_rank, &vc);

    ret = mv2_ud_send_internal(vc, buf, size);

    return ret;
}

int mv2_ud_recv(const spawn_net_channel* ch, void* buf, size_t size)
{
    vbuf *v  = NULL;
    void *ptr = NULL;
    int parsed = 0;
    int dest_rank = -1;
    size_t recv_size = 0;
    MPIDI_VC_t *vc = NULL;
    mv2_ud_exch_info_t ep_info;

    parsed = sscanf(ch->name, "%06x:%04x:%06x", &dest_rank, &ep_info.lid, &ep_info.qpn);
    if (parsed != 3) {
        SPAWN_ERR("Couldn't parse ep info from %s\n", ch->name);
        return NULL;
    }

    MV2_Get_vc(dest_rank, &vc);

    v = mv2_ud_apprecv_window_retrieve_and_remove(&vc->mrail.app_recv_window);
    while (v == NULL) {
        comm_unlock();
        nanosleep(&cm_timeout, &remain);
        comm_lock();
        v = mv2_ud_apprecv_window_retrieve_and_remove(&vc->mrail.app_recv_window);
    }

    ptr = v->buffer + sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);

    recv_size = v->content_size - sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);

    assert(size >= recv_size);

    memcpy(buf, ptr, recv_size);

    MRAILI_Release_vbuf(v);

    return recv_size;
}

spawn_net_channel* mv2_ep_accept()
{
    int i = 0;
    spawn_net_channel* ch = SPAWN_NET_CHANNEL_NULL;
    mv2_ud_exch_info_t ep_info;

    for (i = 0; i < PG_SIZE; ++i) {
        if (ud_vc_info[i].mrail.state == MRAILI_UD_CONNECTING) {
            ch = SPAWN_MALLOC(sizeof(spawn_net_channel));
            ch->name = SPAWN_MALLOC(sizeof(char)*RDMA_CONNECTION_INFO_LEN);
            ch->data = SPAWN_MALLOC(sizeof(mv2_ud_exch_info_t));
        
            /* Populate spawn_net_channel with information */
            ch->type = SPAWN_NET_TYPE_IB;
            sprintf(ch->name, "%06x:%04x:%06x", i, ud_vc_info[i].mrail.ud.lid,
                    ud_vc_info[i].mrail.ud.qpn);

            ep_info.lid = ud_vc_info[i].mrail.ud.lid;
            ep_info.qpn = ud_vc_info[i].mrail.ud.qpn;
            memcpy(ch->data, &ep_info, sizeof(mv2_ud_exch_info_t));

            ud_vc_info[i].mrail.state = MRAILI_UD_CONNECTED;

            break;
        }
    }

    return ch;
}

int mv2_poll_cq()
{
    int ne = 0;
    vbuf *v = NULL;
    MPIDI_VC_t *vc = NULL;
    struct ibv_wc wc;
    struct ibv_cq *cq = g_hca_info.cq_hndl;
    MPIDI_CH3I_MRAILI_Pkt_comm_header *p = NULL;

    /* poll cq */
    
    ne = ibv_poll_cq(cq, 1, &wc);
    if ( 1 == ne ) {
        if ( IBV_WC_SUCCESS != wc.status ) {
            SPAWN_ERR("IBV_WC_SUCCESS != wc.status (%d)\n", wc.status);
            exit(-1);
        }
        /* Get VBUF */
        v = (vbuf *) ((uintptr_t) wc.wr_id);
        /* Get VC from VBUF */
        vc = (MPIDI_VC_t*) (v->vc);
    
        switch (wc.opcode) {
            case IBV_WC_SEND:
            case IBV_WC_RDMA_READ:
            case IBV_WC_RDMA_WRITE:
                if (v->pheader && IS_CNTL_MSG(p)) {
                }
                mv2_ud_update_send_credits(v);
                if (v->flags & UD_VBUF_SEND_INPROGRESS) {
                    v->flags &= ~(UD_VBUF_SEND_INPROGRESS);
                    if (v->flags & UD_VBUF_FREE_PENIDING) {
                        v->flags &= ~(UD_VBUF_FREE_PENIDING);
                        MRAILI_Release_vbuf(v);
                    }
                }
    
                v = NULL;
                break;
            case IBV_WC_RECV:
                SET_PKT_LEN_HEADER(v, wc);
                SET_PKT_HEADER_OFFSET(v);
    
                p = v->pheader;
                /* Retrieve the VC */
                MV2_Get_vc(p->src.rank, &vc);
    
                v->vc   = vc;
                v->rail = p->rail;
    
                if (p->type == MPIDI_CH3_PKT_UD_CONNECT) {
                    mv2_ud_set_vc_info(p->src.rank, (mv2_ud_exch_info_t*) v->buffer, RDMA_DEFAULT_PORT);
                }

                MRAILI_Process_recv(v);

                proc.ud_ctx->num_recvs_posted--;
                if(proc.ud_ctx->num_recvs_posted < proc.ud_ctx->credit_preserve) {
                    proc.ud_ctx->num_recvs_posted += mv2_post_ud_recv_buffers(
                            (RDMA_DEFAULT_MAX_UD_RECV_WQE - proc.ud_ctx->num_recvs_posted),
                            proc.ud_ctx);
                }
                break;
            default:
                SPAWN_ERR("Invalid opcode from ibv_poll_cq()\n");
                break;
        }
    } else if ( ne < 0 ){
        SPAWN_ERR("poll cq error\n");
        exit(-1);
    }

    return ne;
}

int mv2_wait_on_channel()
{
    void *ev_ctx = NULL;
    struct ibv_cq *ev_cq = NULL;

    if (UD_ACK_PROGRESS_TIMEOUT) {
        mv2_check_resend();
        MV2_UD_SEND_ACKS();
        rdma_ud_last_check = mv2_get_time_us();
    }

    /* Unlock before going to sleep */
    comm_unlock();
    /* Wait for the completion event */
    if (ibv_get_cq_event(g_hca_info.comp_channel, &ev_cq, &ev_ctx)) {
        ibv_error_abort(-1, "Failed to get cq_event\n");
    }
    /* Get lock before processing */
    comm_lock();

    /* Ack the event */
    ibv_ack_cq_events(ev_cq, 1);

    /* Request notification upon the next completion event */
    if (ibv_req_notify_cq(ev_cq, 0)) {
        ibv_error_abort(-1, "Couldn't request CQ notification\n");
    }

    mv2_poll_cq();

    return 0;
}
/* ===== End: Send/Recv functions ===== */
