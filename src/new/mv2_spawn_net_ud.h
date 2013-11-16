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

#ifndef _MV2_UD_H_
#define _MV2_UD_H_

#include "mv2_spawn_net_vbuf.h"
#include <infiniband/verbs.h>

#define LOG2(_v, _r)                            \
do {                                            \
    (_r) = ((_v) & 0xFF00) ? 8 : 0;             \
    if ( (_v) & ( 0x0F << (_r + 4 ))) (_r)+=4;  \
    if ( (_v) & ( 0x03 << (_r + 2 ))) (_r)+=2;  \
    if ( (_v) & ( 0x01 << (_r + 1 ))) (_r)+=1;  \
} while(0)

/*
** We should check if the ackno had been handled before.
** We process this only if ackno had advanced.
** There are 2 cases to consider:
** 1. ackno_handled < seqnolast (normal case)
** 2. ackno_handled > seqnolast (wraparound case)
*/
#define INCL_BETWEEN(_val, _start, _end)                            \
    (((_start > _end) && (_val >= _start || _val <= _end)) ||       \
     ((_end > _start) && (_val >= _start && _val <= _end)) ||       \
     ((_end == _start) && (_end == _val)))

#define EXCL_BETWEEN(_val, _start, _end)                            \
    (((_start > _end) && (_val > _start || _val < _end)) ||         \
     ((_end > _start) && (_val > _start && _val < _end)))

#define UD_ACK_PROGRESS_TIMEOUT (((mv2_get_time_us() - rdma_ud_last_check) > rdma_ud_progress_timeout))

#define MV2_UD_RESET_CREDITS(_vc, _v)  {    \
    if (_v->transport == IB_TRANSPORT_UD) { \
        _vc->mrail.ud.ack_pending = 0;      \
    }                                       \
}   

#define MV2_UD_ACK_CREDIT_CHECK(_vc, _v)   {                            \
    if (_v->transport == IB_TRANSPORT_UD) {                             \
        if (++(_vc->mrail.ud.ack_pending) > rdma_ud_max_ack_pending) {  \
            mv2_send_explicit_ack(_vc);                                 \
        }                                                               \
    }                                                                   \
}

#define MAX_SEQ_NUM (UINT16_MAX)
#define MESSAGE_QUEUE_INIT(q)   \
{                               \
    (q)->head = NULL;           \
    (q)->tail = NULL;           \
    (q)->count = 0 ;            \
}

#ifdef _ENABLE_UD_
#define VC_SRC_INFO \
    union {                     \
        uint32_t smp_index;     \
        uint64_t rank;          \
    } src;                      
#else
#define VC_SRC_INFO \
    union {                     \
        uint32_t smp_index;     \
        uint64_t vc_addr;       \
    } src;                      
#endif

#ifdef CRC_CHECK
#define VC_CRC_INFO  unsigned long crc;
#else
#define VC_CRC_INFO  
#endif

#define MPIDI_CH3I_MRAILI_IBA_PKT_DECL \
    uint16_t seqnum;            \
    uint16_t acknum;            \
    uint8_t  remote_credit;     \
    uint8_t  rdma_credit;       \
    VC_SRC_INFO                 \
    VC_CRC_INFO                 \
    uint8_t  vbuf_credit;       \
    uint8_t  rail;              

typedef struct MPIDI_CH3I_MRAILI_Pkt_comm_header_t {
    uint8_t type;
#if defined(MPIDI_CH3I_MRAILI_IBA_PKT_DECL)
    MPIDI_CH3I_MRAILI_IBA_PKT_DECL
#endif
} MPIDI_CH3I_MRAILI_Pkt_comm_header;

#define MARK_ACK_COMPLETED(vc) (vc->mrail.ack_need_tosend = 0)
#define MARK_ACK_REQUIRED(vc) (vc->mrail.ack_need_tosend = 1)

#define MRAILI_UD_CONNECTING    (0x0001)
#define MRAILI_UD_CONNECTED     (0x0002)
#define MRAILI_RC_CONNECTING    (0x0004)
#define MRAILI_RC_CONNECTED     (0x0008)
#define MRAILI_RFP_CONNECTING   (0x0010)
#define MRAILI_RFP_CONNECTED    (0x0020)
#define MRAILI_INIT             (0x0040)

typedef struct message_queue_t
{
    struct vbuf *head;
    struct vbuf *tail;
    uint16_t count;
} message_queue_t;

/* ud context */
typedef struct _mv2_ud_ctx_t {
    int hca_num;
    int send_wqes_avail;
    int num_recvs_posted;
    int default_mtu_sz;
    int credit_preserve;
    struct ibv_qp *qp;
    message_queue_t ext_send_queue;
    uint64_t ext_sendq_count;
}mv2_ud_ctx_t;

typedef struct mv2_ud_qp_info {
    struct ibv_cq      *send_cq;
    struct ibv_cq      *recv_cq;
    struct ibv_srq     *srq;
    struct ibv_pd      *pd; 
    struct ibv_qp_cap  cap; 
    uint32_t           sq_psn;
} mv2_ud_qp_info_t;

/* ud vc info */
typedef struct _mv2_ud_vc_info_t {
    struct ibv_ah *ah;
    uint32_t qpn;
    uint16_t lid;
    uint16_t ack_pending;
    message_queue_t send_window;
    message_queue_t ext_window;
    message_queue_t recv_window;
    unsigned long long total_messages;

    /* profiling counters */
    uint64_t cntl_acks;
    uint64_t resend_count;
    uint64_t ext_win_send_count;
} mv2_ud_vc_info_t;

/* ud exhange info */
typedef struct _mv2_ud_exch_info_t
{
    uint16_t lid;
    uint32_t qpn;
}mv2_ud_exch_info_t;

typedef struct _mv2_rndv_qp_t {
    uint32_t seqnum;
    uint16_t index;
    uint16_t hca_num;
    
    struct ibv_qp *ud_qp;
    struct ibv_cq *ud_cq;

    void *next;
    void *prev;
}mv2_rndv_qp_t;

typedef struct _mv2_ud_zcopy_info_t {
    /* Rndv QP pool */
    mv2_rndv_qp_t *rndv_qp_pool;
    mv2_rndv_qp_t *rndv_qp_pool_free_head;
    int no_free_rndv_qp;
    char *grh_buf;
    void *grh_mr;
    
    struct ibv_cq **rndv_ud_cqs;
    mv2_ud_ctx_t **rndv_ud_qps;
}mv2_ud_zcopy_info_t;

typedef struct MPIDI_CH3I_MRAIL_VC_t
{
    /* number of send wqes available */
    uint16_t    seqnum_next_tosend;
    uint16_t    seqnum_next_torecv;
    uint16_t    seqnum_last_recv;
    uint16_t    seqnum_next_toack;
    uint16_t    ack_need_tosend;
    uint16_t    state;
    message_queue_t app_recv_window;

    mv2_ud_vc_info_t ud;

    uint64_t readid;  /* remote proc labels its packets with this id when sending to us */
    uint64_t writeid; /* we label our outgoing packets with this id */
} MPIDI_CH3I_MRAIL_VC;

typedef struct MPIDI_CH3_Pkt_zcopy_finish_t
{
    uint8_t type;
    MPIDI_CH3I_MRAILI_IBA_PKT_DECL
    int hca_index;
} MPIDI_CH3_Pkt_zcopy_finish_t;
        
typedef struct MPIDI_CH3_Pkt_zcopy_ack_t
{       
    uint8_t type;
    MPIDI_CH3I_MRAILI_IBA_PKT_DECL
} MPIDI_CH3_Pkt_zcopy_ack_t;

#define MPIDI_CH3I_VC_RDMA_DECL MPIDI_CH3I_MRAIL_VC mrail;

typedef struct MPIDI_VC
{
    MPIDI_CH3I_VC_RDMA_DECL
} MPIDI_VC_t;

typedef struct _mv2_proc_info_t {
    uint32_t                    rc_connections;
    mv2_ud_ctx_t                *ud_ctx;
    message_queue_t             unack_queue;
    mv2_ud_exch_info_t          *remote_ud_info;
    mv2_ud_zcopy_info_t         zcopy_info;
    int   (*post_send)(MPIDI_VC_t * vc, vbuf * v, int rail, mv2_ud_ctx_t *send_ud_ctx);
} mv2_proc_info_t;

void mv2_ud_zcopy_poll_cq(mv2_ud_zcopy_info_t *zcopy_info, mv2_ud_ctx_t *ud_ctx,
                                vbuf *resend_buf, int hca_index, int *found);

int post_ud_send(MPIDI_VC_t* vc, vbuf* v, int rail, mv2_ud_ctx_t *send_ud_ctx);

/* destroy ud context */
void mv2_ud_resend(vbuf *v);
void mv2_check_resend();
void mv2_ud_update_send_credits(vbuf *v);
int MRAILI_Process_send(void *vbuf_addr);
void mv2_send_explicit_ack(MPIDI_VC_t *vc);
void MPIDI_CH3I_MRAIL_Release_vbuf(vbuf * v);
void MRAILI_Process_recv(vbuf *v);
inline void mv2_ud_apprecv_window_add(message_queue_t *q, vbuf *v);
inline vbuf* mv2_ud_apprecv_window_retrieve_and_remove(message_queue_t *q);

int mv2_ud_send(MPIDI_VC_t* vc, const void* buf, size_t size);
int mv2_ud_recv(MPIDI_VC_t* vc, void* buf, size_t size);

#endif /* #ifndef _MV2_UD_H_ */

