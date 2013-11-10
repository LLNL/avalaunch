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

mv2_proc_info_t proc;
mv2_hca_info_t g_hca_info;
mv2_ud_exch_info_t local_ep_info;
MPIDI_VC_t *ud_vc_info = NULL; 

int mv2_post_ud_recv_buffers(int num_bufs, mv2_ud_ctx_t *ud_ctx);

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
int mv2_ud_qp_transition(struct ibv_qp *qp)
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
mv2_ud_ctx_t* mv2_ud_create_ctx (mv2_ud_qp_info_t *qp_info)
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
spawn_net_endpoint* mv2_init_ud(int nchild)
{
    mv2_ud_qp_info_t qp_info;   
    spawn_net_endpoint *ep = NULL;

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

    proc.ud_ctx = mv2_ud_create_ctx(&qp_info);
    if (!proc.ud_ctx) {          
        fprintf(stderr, "Error in create UD qp\n");
        return NULL;
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
    if (NULL == ud_vc_info) {
        fprintf(stderr, "Unable to malloc ud_vc_info");
        return NULL;
    }

    proc.post_send = post_ud_send;

    /* Create end point */
    local_ep_info.lid = g_hca_info.port_attr[0].lid;
    local_ep_info.qpn = proc.ud_ctx->qp->qp_num;

    ep = SPAWN_MALLOC(sizeof(spawn_net_endpoint));
    ep->name = SPAWN_MALLOC(sizeof(char)*RDMA_CONNECTION_INFO_LEN);
    ep->data = SPAWN_MALLOC(sizeof(mv2_ud_exch_info_t));
    if (NULL == ep || NULL == ep->name || NULL == ep->data) {
        fprintf(stderr, "Unable to malloc ep");
        return NULL;
    }

    /* Populate spawn_net_endpoint with correct data */
    ep->type = SPAWN_NET_TYPE_IB;

    sprintf(ep->name, "%06x:%04x:%06x", PG_RANK, local_ep_info.lid, local_ep_info.qpn);

    memcpy(ep->data, &local_ep_info, sizeof(mv2_ud_exch_info_t));

    return ep;
}

/* Create UD VC */
int mv2_ud_set_vc_info (int rank, mv2_ud_exch_info_t *rem_info, int port)
{
    struct ibv_ah_attr ah_attr;

    assert(rank < PG_SIZE);

    //PRINT_DEBUG(DEBUG_UD_verbose>0,"lid:%d\n", rem_info->lid );
    
    memset(&ah_attr, 0, sizeof(ah_attr));

    ah_attr.sl              = RDMA_DEFAULT_SERVICE_LEVEL;
    ah_attr.dlid            = rem_info->lid;
    ah_attr.port_num        = port;
    ah_attr.is_global       = 0; 
    ah_attr.src_path_bits   = 0; 

    ud_vc_info[rank].mrail.ud.ah = ibv_create_ah(g_hca_info.pd, &ah_attr);
    if(!(ud_vc_info[rank].mrail.ud.ah)){    
        fprintf(stderr, "Error in creating address handle\n");
        return -1;
    }

    ud_vc_info[rank].mrail.ud.lid = rem_info->lid;
    ud_vc_info[rank].mrail.ud.qpn = rem_info->qpn;

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
}
/* ===== End: Initialization functions ===== */

/* ===== Begin: Send/Recv functions ===== */
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
    spawn_net_channel* ch = NULL;

    parsed = sscanf(name, "%06x:%04x:%06x", &dest_rank, &ep_info.lid, &ep_info.qpn);
    if (parsed != 3) {
        fprintf(stderr, "Couldn't parse ep info from %s\n", name);
        return NULL;
    }

    ch = SPAWN_MALLOC(sizeof(spawn_net_channel));
    ch->name = SPAWN_MALLOC(sizeof(char)*16);
    ch->data = SPAWN_MALLOC(sizeof(mv2_ud_exch_info_t));
    if (NULL == ch || NULL == ch->name || NULL == ch->data) {
        fprintf(stderr, "Unable to malloc spawn_net_channel");
        return NULL;
    }

    /* Populate spawn_net_channel with information */
    ch->type = SPAWN_NET_TYPE_IB;
    sprintf(ch->name, "%06x:%04x:%06x", dest_rank, local_ep_info.lid, local_ep_info.qpn);
    memcpy(ch->data, &ep_info, sizeof(mv2_ud_exch_info_t));

    /* Set VC info */
    mv2_ud_set_vc_info(dest_rank, &ep_info, RDMA_DEFAULT_PORT);

    MV2_Get_vc(dest_rank, &vc);

    /* Send connect message to destination */
    mv2_send_connect_message(vc);

    return ch;
}

int mv2_ud_send_internal(MPIDI_VC_t *vc, const void* buf, size_t size)
{
    int avail = 0;
    MPIDI_CH3I_MRAILI_Pkt_comm_header *pkt = NULL;
    vbuf *v = get_ud_vbuf();
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
        fprintf(stderr, "Couldn't parse ep info from %s\n", ch->name);
        return NULL;
    }

    MV2_Get_vc(dest_rank, &vc);

    ret = mv2_ud_send_internal(vc, buf, size);

    return ret;
}

vbuf* mv2_poll_cq()
{
    int ne = 0;
    vbuf *v = NULL;
    mv2_ud_vc_info_t *vc = NULL;
    struct ibv_wc wc;
    struct ibv_cq *cq = g_hca_info.cq_hndl;
    MPIDI_CH3I_MRAILI_Pkt_comm_header *p = NULL;

    /* poll cq */
    ne = ibv_poll_cq(cq, 1, &wc);
    if ( 1 == ne ) {
        if ( IBV_WC_SUCCESS != wc.status ) {
            fprintf(stderr, "IBV_WC_SUCCESS != wc.status (%d)\n", wc.status);
            exit(-1);
        }
        /* Get VBUF */
        v = (vbuf *) ((uintptr_t) wc.wr_id);
        /* Get VC from VBUF */
        vc = (mv2_ud_vc_info_t*) (v->vc);

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

                break;
            case IBV_WC_RECV:
                SET_PKT_LEN_HEADER(v, wc);
                SET_PKT_HEADER_OFFSET(v);

                p = v->pheader;
                /* Retrieve the VC */
                MV2_Get_vc(p->src.rank, &vc);

                v->vc   = vc;
                v->rail = p->rail;

                proc.ud_ctx->num_recvs_posted--;
                if(proc.ud_ctx->num_recvs_posted < proc.ud_ctx->credit_preserve) {
                    proc.ud_ctx->num_recvs_posted += mv2_post_ud_recv_buffers(
                            (RDMA_DEFAULT_MAX_UD_RECV_WQE - proc.ud_ctx->num_recvs_posted),
                            proc.ud_ctx);
                }
                break;
            default:
                fprintf(stderr, "Invalid opcode from ibv_poll_cq()\n");
                break;
        }
    } else if ( ne < 0 ){
        fprintf(stderr, "poll cq error\n");
        exit(-1);
    }

    return NULL;
}

vbuf* mv2_wait_on_channel()
{
    vbuf *v = NULL;
    void *ev_ctx = NULL;
    struct ibv_cq *ev_cq = NULL;

    if (UD_ACK_PROGRESS_TIMEOUT) {
        mv2_check_resend();
        MV2_UD_SEND_ACKS();
        rdma_ud_last_check = mv2_get_time_us();
    }

    /* Wait for the completion event */
    if (ibv_get_cq_event(g_hca_info.comp_channel, &ev_cq, &ev_ctx)) {
        ibv_error_abort(-1, "Failed to get cq_event\n");
    }

    /* Ack the event */
    ibv_ack_cq_events(ev_cq, 1);

    /* Request notification upon the next completion event */
    if (ibv_req_notify_cq(ev_cq, 0)) {
        ibv_error_abort(-1, "Couldn't request CQ notification\n");
    }

    do {
        v = mv2_poll_cq();
    } while (NULL == v);

    return v;
}
/* ===== End: Send/Recv functions ===== */
