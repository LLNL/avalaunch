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
#include <spawn_internal.h>
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

/* Tracks an array of virtual channels.  With each new channel created,
 * the id is incremented.  Grows channel array as needed. */
static uint64_t ud_vc_info_id  = 0;    /* next id to be assigned */
static uint64_t ud_vc_infos    = 0;    /* capacity of VC array */
MPIDI_VC_t** ud_vc_info = NULL; /* VC array */

static pthread_mutex_t comm_lock_object;
static pthread_t comm_thread;

void* cm_timeout_handler(void *arg);

#define MV2_UD_SEND_ACKS() {            \
    int i;                              \
    MPIDI_VC_t *vc;                     \
    int size = ud_vc_info_id;           \
    for (i=0; i<size; i++) {            \
        MV2_Get_vc(i, &vc);             \
        if (vc->mrail.ack_need_tosend) {\
            mv2_send_explicit_ack(vc);  \
        }                               \
    }                                   \
}

/*******************************************
 * Manage VC objects
 ******************************************/

/* initialize UD VC */
static void vc_init(MPIDI_VC_t* vc)
{
    vc->mrail.state = MRAILI_INIT;

    vc->mrail.ack_need_tosend = 0;
    vc->mrail.seqnum_next_tosend = 0;
    vc->mrail.seqnum_next_torecv = 0;
    vc->mrail.seqnum_next_toack = UINT16_MAX;

    vc->mrail.ud.cntl_acks = 0; 
    vc->mrail.ud.ack_pending = 0;
    vc->mrail.ud.resend_count = 0;
    vc->mrail.ud.total_messages = 0;
    vc->mrail.ud.ext_win_send_count = 0;

    MESSAGE_QUEUE_INIT(&(vc->mrail.ud.send_window));
    MESSAGE_QUEUE_INIT(&(vc->mrail.ud.ext_window));
    MESSAGE_QUEUE_INIT(&(vc->mrail.ud.recv_window));
    MESSAGE_QUEUE_INIT(&(vc->mrail.app_recv_window));

    return;
}

/* allocate and initialize a new VC */
static MPIDI_VC_t* vc_alloc()
{
    /* get a new id */
    uint64_t id = ud_vc_info_id;

    /* increment our counter for next time */
    ud_vc_info_id++;

    /* check whether we need to allocate more VC's */
    if (id >= ud_vc_infos) {
        /* increase capacity of array */
        if (ud_vc_infos > 0) {
            ud_vc_infos *= 2;
        } else {
            ud_vc_infos = 1;
        }

        /* allocate space to hold VC pointers */
        size_t vcsize = ud_vc_infos * sizeof(MPIDI_VC_t*);
        MPIDI_VC_t** vcs = (MPIDI_VC_t**) SPAWN_MALLOC(vcsize);

        /* copy old values into new array */
        uint64_t i;
        for (i = 0; i < id; i++) {
            vcs[i] = ud_vc_info[i];
        }

        /* free old array and assign it to new copy */
        spawn_free(&ud_vc_info);
        ud_vc_info = vcs;
    }

    /* allocate and initialize a new VC */
    MPIDI_VC_t* vc = (MPIDI_VC_t*) SPAWN_MALLOC(sizeof(MPIDI_VC_t));
    vc_init(vc);

    /* record address of vc in array */
    ud_vc_info[id] = vc;

    /* set our read id, other end of channel will label its outgoing
     * messages with this id when sending to us (our readid is their
     * writeid) */
    vc->mrail.readid = id;

    /* return vc to caller */
    return vc;
}

static int vc_set_addr(MPIDI_VC_t* vc, mv2_ud_exch_info_t *rem_info, int port)
{
    if (vc->mrail.state == MRAILI_UD_CONNECTING ||
        vc->mrail.state == MRAILI_UD_CONNECTED)
    {
        /* Duplicate message - return */
        return 0;
    }

    //PRINT_DEBUG(DEBUG_UD_verbose>0,"lid:%d\n", rem_info->lid );
    
    struct ibv_ah_attr ah_attr;
    memset(&ah_attr, 0, sizeof(ah_attr));

    ah_attr.sl              = RDMA_DEFAULT_SERVICE_LEVEL;
    ah_attr.dlid            = rem_info->lid;
    ah_attr.port_num        = port;
    ah_attr.is_global       = 0; 
    ah_attr.src_path_bits   = 0; 

    vc->mrail.ud.ah = ibv_create_ah(g_hca_info.pd, &ah_attr);
    if(vc->mrail.ud.ah == NULL){    
        /* TODO: man page doesn't say anything about errno */
        SPAWN_ERR("Error in creating address handle (ibv_create_ah errno=%d %s)", errno, strerror(errno));
        return -1;
    }

    vc->mrail.state = MRAILI_UD_CONNECTING;

    vc->mrail.ud.lid = rem_info->lid;
    vc->mrail.ud.qpn = rem_info->qpn;

    return 0;
}

static void vc_free(MPIDI_VC_t** pvc)
{
    if (pvc == NULL) {
        return;
    }

    //MPIDI_VC_t* vc = *pvc;

    /* TODO: free off any memory allocated for vc */

    spawn_free(pvc);

    return;
}

/*******************************************
 * Communication routines
 ******************************************/

/* this queue tracks a list of pending connect messages,
 * the accept function pulls items from this list */
typedef struct vbuf_list_t {
    vbuf* v;
    struct vbuf_list_t* next;
} vbuf_list;

static vbuf_list* connect_head = NULL;
static vbuf_list* connect_tail = NULL;

static int mv2_post_ud_recv_buffers(int num_bufs, mv2_ud_ctx_t *ud_ctx)
{
    /* check that we don't exceed the max number of work queue elements */
    if (num_bufs > rdma_default_max_ud_recv_wqe) {
        ibv_va_error_abort(
                GEN_ASSERT_ERR,
                "Try to post %d to UD recv buffers, max %d\n",
                num_bufs, rdma_default_max_ud_recv_wqe);
    }

    /* post our vbufs */
    int i;
    for (i = 0; i < num_bufs; ++i) {
        /* get a new vbuf */
        vbuf* v = get_ud_vbuf();
        if (v == NULL) {
            break;
        }

        /* initialize vubf for UD */
        vbuf_init_ud_recv(v, rdma_default_ud_mtu, 0);
        v->transport = IB_TRANSPORT_UD;

        /* post vbuf to receive queue */
        int ret;
        struct ibv_recv_wr* bad_wr = NULL;
        if (ud_ctx->qp->srq) {
            ret = ibv_post_srq_recv(ud_ctx->qp->srq, &v->desc.u.rr, &bad_wr);
        } else {
            ret = ibv_post_recv(ud_ctx->qp, &v->desc.u.rr, &bad_wr);
        }

        /* check that our post was successful */
        if (ret) {
            MRAILI_Release_vbuf(v);
            break;
        }
    }

    PRINT_DEBUG(DEBUG_UD_verbose>0 ,"Posted %d buffers of size:%d to UD QP\n",num_bufs, rdma_default_ud_mtu);

    return i;
}

static int mv2_poll_cq()
{
    /* poll cq */
    struct ibv_wc wc;
    struct ibv_cq* cq = g_hca_info.cq_hndl;
    int ne = ibv_poll_cq(cq, 1, &wc);
    if (ne == 1) {
        if (IBV_WC_SUCCESS != wc.status) {
            SPAWN_ERR("IBV_WC_SUCCESS != wc.status (%d)", wc.status);
            exit(-1);
        }

        /* Get VBUF */
        vbuf* v = (vbuf *) ((uintptr_t) wc.wr_id);

        /* get packet header */
        SET_PKT_LEN_HEADER(v, wc);
        SET_PKT_HEADER_OFFSET(v);
        MPIDI_CH3I_MRAILI_Pkt_comm_header* p = v->pheader;

        switch (wc.opcode) {
            case IBV_WC_SEND:
            case IBV_WC_RDMA_READ:
            case IBV_WC_RDMA_WRITE:
                if (p != NULL && IS_CNTL_MSG(p)) {
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
                /* we don't have a source id for connect messages */
                if (p->type != MPIDI_CH3_PKT_UD_CONNECT) {
                    /* src field is valid (unless we have a connect message),
                     * use src id to get vc */
                    MPIDI_VC_t* vc;
                    MV2_Get_vc(p->src.rank, &vc);

                    v->vc   = vc;
                    v->rail = p->rail;

                    MRAILI_Process_recv(v);
                } else {
                    /* a connect message does not have a valid src id field,
                     * so we can't associate msg with a vc yet, we stick this
                     * on the queue that accept looks to later */

                    /* allocate and initialize new element for connect queue */
                    vbuf_list* elem = (vbuf_list*) SPAWN_MALLOC(sizeof(vbuf_list));
                    elem->v = v;
                    elem->next = NULL;

                    /* append elem to connect queue */
                    if (connect_head == NULL) {
                        connect_head = elem;
                    }
                    if (connect_tail != NULL) {
                        connect_tail->next = elem;
                    }
                    connect_tail = elem;
                }

                proc.ud_ctx->num_recvs_posted--;
                if(proc.ud_ctx->num_recvs_posted < proc.ud_ctx->credit_preserve) {
                    proc.ud_ctx->num_recvs_posted += mv2_post_ud_recv_buffers(
                            (RDMA_DEFAULT_MAX_UD_RECV_WQE - proc.ud_ctx->num_recvs_posted),
                            proc.ud_ctx);
                }
                break;
            default:
                SPAWN_ERR("Invalid opcode from ibv_poll_cq()");
                break;
        }
    } else if (ne < 0) {
        SPAWN_ERR("poll cq error");
        exit(-1);
    }

    return ne;
}

static int mv2_wait_on_channel()
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
static int mv2_ud_qp_transition(struct ibv_qp *qp)
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
static struct ibv_qp* mv2_ud_create_qp(mv2_ud_qp_info_t *qp_info)
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
    if (mv2_ud_qp_transition(qp)) {
        ibv_destroy_qp(qp);
        return NULL;
    }

    return qp;
}

/* Create UD Context */
static mv2_ud_ctx_t* mv2_ud_create_ctx(mv2_ud_qp_info_t *qp_info)
{
    mv2_ud_ctx_t* ctx = (mv2_ud_ctx_t*) SPAWN_MALLOC(sizeof(mv2_ud_ctx_t));
    memset(ctx, 0, sizeof(mv2_ud_ctx_t));

    ctx->qp = mv2_ud_create_qp(qp_info);
    if(! ctx->qp) {
        SPAWN_ERR("Error in creating UD QP");
        return NULL;
    }

    return ctx;
}

/* Destroy UD Context */
static void mv2_ud_destroy_ctx(mv2_ud_ctx_t *ctx)
{
    if (ctx->qp) {
        ibv_destroy_qp(ctx->qp);
    }
    spawn_free(&ctx);
    spawn_free(&ud_vc_info);

    pthread_cancel(comm_thread);
}

/* Initialize UD Context */
spawn_net_endpoint* mv2_init_ud()
{
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

    proc.post_send = post_ud_send;

    /* Create end point */
    local_ep_info.lid = g_hca_info.port_attr[0].lid;
    local_ep_info.qpn = proc.ud_ctx->qp->qp_num;

    /* allocate new endpoint and fill in its fields */
    ep = SPAWN_MALLOC(sizeof(spawn_net_endpoint));
    ep->type = SPAWN_NET_TYPE_IBUD;
    ep->name = SPAWN_STRDUPF("IBUD:%04x:%06x", local_ep_info.lid, local_ep_info.qpn);
    ep->data = NULL;

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

/* ===== End: Initialization functions ===== */

/* ===== Begin: Send/Recv functions ===== */

/*Interface to lock/unlock connection manager*/
void comm_lock(void)
{           
    int rc = pthread_mutex_lock(&comm_lock_object);
    if (rc != 0) {
        SPAWN_ERR("Failed to lock comm mutex (pthread_mutex_lock rc=%d %s)", rc, strerror(rc));
    }
    return;
}
            
void comm_unlock(void)
{           
    int rc = pthread_mutex_unlock(&comm_lock_object);
    if (rc != 0) {
        SPAWN_ERR("Failed to unlock comm mutex (pthread_mutex_unlock rc=%d %s)", rc, strerror(rc));
    }
    return;
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

/* blocks until packet comes in on specified VC */
static vbuf* packet_read(MPIDI_VC_t* vc)
{
    vbuf* v = mv2_ud_apprecv_window_retrieve_and_remove(&vc->mrail.app_recv_window);
    while (v == NULL) {
        comm_unlock();
        nanosleep(&cm_timeout, &remain);
        comm_lock();
        v = mv2_ud_apprecv_window_retrieve_and_remove(&vc->mrail.app_recv_window);
    }
    return v;
}

static int mv2_send_connect_message(MPIDI_VC_t *vc)
{
    /* message payload, specify id we want remote side to use when
     * sending to us followed by our lid/qp */
    char* payload = SPAWN_STRDUPF("%06x:%04x:%06x",
        vc->mrail.readid, local_ep_info.lid, local_ep_info.qpn
    );
    size_t payload_size = strlen(payload) + 1;

    /* grab a packet */
    vbuf* v = get_ud_vbuf();
    if (v == NULL) {
        SPAWN_ERR("Failed to get vbuf for connect msg");
        return SPAWN_FAILURE;
    }

    /* Offset for the packet header */
    size_t header_size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);
    assert((MRAIL_MAX_UD_SIZE - header_size) >= payload_size);

    /* set header fields */
    MPIDI_CH3I_MRAILI_Pkt_comm_header* connect_pkt = v->pheader;
    MPIDI_Pkt_init(connect_pkt, MPIDI_CH3_PKT_UD_CONNECT);
    connect_pkt->acknum = vc->mrail.seqnum_next_toack;
    connect_pkt->rail   = v->rail;

    /* copy in payload */
    char* ptr = (char*)v->buffer + header_size;
    memcpy(ptr, payload, payload_size);

    /* compute packet size */
    v->content_size = header_size + payload_size;

    /* prepare packet for send */
    vbuf_init_send(v, header_size, v->rail);

    /* and send it */
    proc.post_send(vc, v, 0, NULL);

    return SPAWN_SUCCESS;
}

/* blocks until element arrives on connect queue,
 * extracts element and returns its vbuf */
static vbuf* mv2_recv_connect_message()
{
    /* wait until we see at item at head of connect queue */
    while (connect_head == NULL) {
        comm_unlock();
        nanosleep(&cm_timeout, &remain);
        comm_lock();
    }

    /* get pointer to element */
    vbuf_list* elem = connect_head;

    /* extract element from queue */
    connect_head = elem->next;
    if (connect_head == NULL) {
        connect_tail = NULL;
    }

    /* get pointer to vbuf */
    vbuf* v = elem->v;

    /* free element */
    spawn_free(&elem);

    /* return vbuf */
    return v;
}

static int mv2_send_accept_message(MPIDI_VC_t *vc)
{
    /* message payload, specify id we want remote side to use when
     * sending to us followed by our lid/qp */
    char* payload = SPAWN_STRDUPF("%06x", vc->mrail.readid);
    size_t payload_size = strlen(payload) + 1;

    /* grab a packet */
    vbuf* v = get_ud_vbuf();
    if (v == NULL) {
        SPAWN_ERR("Failed to get vbuf for accept msg");
        return SPAWN_FAILURE;
    }

    /* Offset for the packet header */
    size_t header_size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);
    assert((MRAIL_MAX_UD_SIZE - header_size) >= payload_size);

    /* set header fields */
    MPIDI_CH3I_MRAILI_Pkt_comm_header* connect_pkt = v->pheader;
    MPIDI_Pkt_init(connect_pkt, MPIDI_CH3_PKT_UD_ACCEPT);
    connect_pkt->acknum = vc->mrail.seqnum_next_toack;
    connect_pkt->rail   = v->rail;

    /* copy in payload */
    char* ptr = (char*)v->buffer + header_size;
    memcpy(ptr, payload, payload_size);

    /* compute packet size */
    v->content_size = header_size + payload_size;

    /* prepare packet for send */
    vbuf_init_send(v, header_size, v->rail);

    /* and send it */
    proc.post_send(vc, v, 0, NULL);

    return SPAWN_SUCCESS;
}

static int mv2_recv_accept_message(MPIDI_VC_t* vc)
{
    /* first incoming packet should be accept */
    vbuf* v = packet_read(vc);

    /* message payload is write id we should use when sending */
    size_t header_size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);
    char* payload = (char*)v->buffer + header_size;

    /* extract write id from payload */
    int id;
    int parsed = sscanf(payload, "%06x", &id);
    if (parsed != 1) {
        SPAWN_ERR("Couldn't parse write id from accept message");
        return SPAWN_FAILURE;
    }

    /* TODO: avoid casting up from int here */
    uint64_t writeid = (uint64_t) id;

    /* set our write id */
    vc->mrail.writeid = writeid;

    /* put vbuf back on free list */
    MRAILI_Release_vbuf(v);

    return SPAWN_SUCCESS;
}

spawn_net_channel* mv2_ep_connect(const char *name)
{
    /* extract lid and queue pair address from endpoint name */
    unsigned int lid, qpn;
    int parsed = sscanf(name, "IBUD:%04x:%06x", &lid, &qpn);
    if (parsed != 2) {
        SPAWN_ERR("Couldn't parse ep info from %s", name);
        return SPAWN_NET_CHANNEL_NULL;
    }

    /* store lid and queue pair */
    mv2_ud_exch_info_t ep_info;
    ep_info.lid = lid;
    ep_info.qpn = qpn;

    /* allocate and initialize a new virtual channel */
    MPIDI_VC_t* vc = vc_alloc();

    /* point channel to remote endpoint */
    vc_set_addr(vc, &ep_info, RDMA_DEFAULT_PORT);

    /* send connect message to destination */
    mv2_send_connect_message(vc);

    /* wait for accept message and set vc->mrail.writeid */
    mv2_recv_accept_message(vc);

    /* Change state to connected */
    vc->mrail.state = MRAILI_UD_CONNECTED;

    /* allocate spawn net channel data structure */
    spawn_net_channel* ch = SPAWN_MALLOC(sizeof(spawn_net_channel));
    ch->type = SPAWN_NET_TYPE_IBUD;

    /* TODO: include hostname here */
    /* Fill in channel name */
    ch->name = SPAWN_STRDUPF("IBUD:%04x:%06x", ep_info.lid, ep_info.qpn);

    /* record address of vc in channel data field */
    ch->data = (void*) vc;

    return ch;
}

spawn_net_channel* mv2_ep_accept()
{
    /* wait for connect message */
    vbuf* v = mv2_recv_connect_message();

    /* get pointer to payload */
    size_t header_size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);
    char* payload = (char*)v->buffer + header_size;

    /* get id and endpoint name from message payload */
    int id, lid, qpn;
    int parsed = sscanf(payload, "%06x:%04x:%06x", &id, &lid, &qpn);
    if (parsed != 3) {
        SPAWN_ERR("Couldn't parse ep info from %s", payload);
        return SPAWN_NET_CHANNEL_NULL;
    }
    uint64_t writeid = id;

    /* put vbuf back on free list */
    MRAILI_Release_vbuf(v);

    /* allocate new vc */
    MPIDI_VC_t* vc = vc_alloc();

    /* record lid/qp in VC */
    mv2_ud_exch_info_t ep_info;
    ep_info.lid = lid;
    ep_info.qpn = qpn;
    vc_set_addr(vc, (mv2_ud_exch_info_t*) v->buffer, RDMA_DEFAULT_PORT);

    /* record remote id as write id */
    vc->mrail.writeid = writeid;

    /* send accept message */
    mv2_send_accept_message(vc);

    /* mark vc as connected */
    vc->mrail.state = MRAILI_UD_CONNECTED;

    /* allocate new channel data structure */
    spawn_net_channel* ch = SPAWN_MALLOC(sizeof(spawn_net_channel));
    ch->type = SPAWN_NET_TYPE_IBUD;

    /* record name */
    ch->name = SPAWN_STRDUPF("IBUD:%04x:%06x", ep_info.lid, ep_info.qpn);

    /* record address of vc in channel data field */
    ch->data = (void*) vc;

    return ch;
}

int mv2_ud_send(MPIDI_VC_t *vc, const void* buf, size_t size)
{
    /* compute header and payload sizes */
    size_t header_size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);
    size_t payload_size = MRAIL_MAX_UD_SIZE - header_size;
    assert(MRAIL_MAX_UD_SIZE >= header_size);

    /* break message up into packets and send each one */
    int rc = SPAWN_SUCCESS;
    size_t nwritten = 0;
    while (nwritten < size) {
        /* determine amount to write in this step */
        size_t bytes = (size - nwritten);
        if (bytes > payload_size) {
            bytes = payload_size;
        }

        /* get a packet */
        vbuf* v = get_ud_vbuf();
        if (v == NULL) {
            /* TODO: need to worry about this? */
            SPAWN_ERR("Failed to get vbuf for sending");
            return SPAWN_FAILURE;
        }

        /* fill in packet header fields */
        MPIDI_CH3I_MRAILI_Pkt_comm_header* pkt = v->pheader;
        MPIDI_Pkt_init(pkt, MPIDI_CH3_PKT_UD_DATA);
        pkt->acknum = vc->mrail.seqnum_next_toack;
        pkt->rail   = v->rail;

        /* fill in the message payload */
        char* ptr  = (char*)v->buffer + header_size;
        char* data = (char*)buf + nwritten;
        memcpy(ptr, data, bytes);

        /* set packet size */
        v->content_size = header_size + bytes;

        /* prepare packet for send */
        vbuf_init_send(v, header_size, v->rail);

        if (vc->mrail.state != MRAILI_UD_CONNECTED) {
            /* TODO: what's this do? */
            mv2_ud_ext_window_add(&vc->mrail.ud.ext_window, v);
        } else {
            /* send packet */
            proc.post_send(vc, v, 0, NULL);
        }

        /* go to next part of message */
        nwritten += bytes;
    }

    return rc;
}

int mv2_ud_recv(MPIDI_VC_t* vc, void* buf, size_t size)
{
    /* compute header and payload sizes */
    size_t header_size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);
    assert(MRAIL_MAX_UD_SIZE >= header_size);

    /* read data one packet at a time */
    int rc = SPAWN_SUCCESS;
    size_t nread = 0;
    while (nread < size) {
        /* read a packet from this vc */
        vbuf* v = packet_read(vc);

        /* copy data to user's buffer */
        size_t payload_size = v->content_size - header_size;
        if (payload_size > 0) {
            /* get pointer to message payload */
            char* ptr  = (char*)buf + nread;
            char* data = (char*)v->buffer + header_size;
            memcpy(ptr, data, payload_size);
        }

        /* put vbuf back on free list */
        MRAILI_Release_vbuf(v);

        /* go on to next part of message */
        nread += payload_size;
    }

    return rc;
}

/* ===== End: Send/Recv functions ===== */
