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

/* Big Picture:
 *
 * A "UD context" data structure tracks info on access to the UD QP
 * opened on an HCA.  This includes fields like a pointer to the QP,
 * the MTU size, the number of available send work queue elements,
 * the number of outstanding receives, and a pointer to a message
 * queue of packets to be sent as soon as send WQEs become available.
 *
 * Then, for each remote endpoint that a process "connects" to, we
 * track details in a "virtual connection".  Each packet sent on a
 * virtual connection is assigned a sequence number.  Sequence numbers
 * are 16-bit integers that increment with each packet sent and wrap
 * around.  A sliding window of sequence numbers are valid at any
 * given instant, and processes use ACKS to manage this window.  Each
 * VC manages several queues (called windows): send, extended send,
 * in-order received, out-of-order receieved, and unack'd.
 *
 * - send window - tracks packets handed to the UD context.
 *
 * - extended send window - tracks packets ready to be sent on the VC,
 *   but not yet handed off to the UD context.
 *
 * - in-order receive window - tracks a list of received packets
 *   ordered by sequence number with no missing packets
 *
 * - out-of-order receive window - tracks a list of received packets
 *   that includes one or more missing packets
 *
 * - unack'd window - records a list of packets that must be ACK'd
 *   by the destination
 *
 * When sending a packet, it is added to the send window if there is
 * room.  Otherwise, it is added to the extended send window.  When a
 * packet is added to the send window, it is submitted to the UD
 * context.  In this way, the send window enforces a limit on the
 * number of packets a VC can have outstanding on the UD context.
 *
 * Each control message (except explicit ACKS) is added to the
 * "unack'd window" when actually sent by the UD context.  This records
 * packets yet to be acknowledged from the destination, as well as a
 * timestamp on when the packet was last sent.  A thread peridoically
 * wakes up and scans the unack'd list to resend packets that have
 * exceeded their timeout.
 *
 * When a process sends a message to another process, it also records
 * the sequence number for the latest packet it has received from the
 * destination.  Upon receiving the message, the destination will
 * clear any packets from its send and unack'd windows up to and
 * including that sequence number.  After removing packets from the
 * send window, more packets can be queued by taking them from the
 * VC extended send window.
 *
 * The in-order receive window records a list of packets ready to be
 * received by the application (apprecv_win).
 *
 * There is also an out-of-order receive window which records packets
 * that have been received but cannot be appended to the in-order queue
 * because one or more packets may be missing.  With each received
 * packet, this queue is checked and packets are moved to the in-order
 * receive queue if possible.
 *
 * In case there are no data packets flowing back from receiver to
 * sender to carry implicit acks, explicit ack messages are sent in
 * different circumstances.
 *
 * The thread that periodically wakes to check whether packets need to
 * be resent will also send "explicit acks" if necessary.  An explicit
 * ack is sent whenever an ack needs to be sent but a piggy-backed ack
 * has not been sent for a certain amount of time.
 *
 * An explicit ACK is also sent if the number of received packets exceeds
 * a threshold since the last ACK was sent. */

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

/* check whether val is within [start, end] */
#define INCL_BETWEEN(_val, _start, _end)                            \
    (((_start > _end) && (_val >= _start || _val <= _end)) ||       \
     ((_end > _start) && (_val >= _start && _val <= _end)) ||       \
     ((_end == _start) && (_end == _val)))

/* check whether val is within (start, end) */
#define EXCL_BETWEEN(_val, _start, _end)                            \
    (((_start > _end) && (_val > _start || _val < _end)) ||         \
     ((_end > _start) && (_val > _start && _val < _end)))

#define MAX_SEQ_NUM (UINT16_MAX)

/* srcid  - source context id to identify sender context */
/* seqnum - sequence number tracking packet sent from source */
/* acknum - most recent sequence number that source has received */
/* rail   - rail id to send packet on */
#define MPIDI_CH3I_MRAILI_IBA_PKT_DECL \
    uint64_t srcid;             \
    uint16_t seqnum;            \
    uint16_t acknum;            \
    uint8_t  rail;              

typedef struct MPIDI_CH3I_MRAILI_Pkt_comm_header_t {
    uint8_t type;
    MPIDI_CH3I_MRAILI_IBA_PKT_DECL
} MPIDI_CH3I_MRAILI_Pkt_comm_header;

/* VC state values */
#define MRAILI_UD_CONNECTING    (0x0001)
#define MRAILI_UD_CONNECTED     (0x0002)
#define MRAILI_RC_CONNECTING    (0x0004)
#define MRAILI_RC_CONNECTED     (0x0008)
#define MRAILI_RFP_CONNECTING   (0x0010)
#define MRAILI_RFP_CONNECTED    (0x0020)
#define MRAILI_INIT             (0x0040)

/* tracks a list of vbufs */
typedef struct message_queue_t {
    struct vbuf *head;
    struct vbuf *tail;
    uint16_t count;
} message_queue_t;

/* initialize fields of a message queue */
#define MESSAGE_QUEUE_INIT(q)   \
{                               \
    (q)->head = NULL;           \
    (q)->tail = NULL;           \
    (q)->count = 0 ;            \
}

/* ud context - tracks access to open UD QP on HCA */
typedef struct _mv2_ud_ctx_t {
    int hca_num;              /* id of HCA to use, starts at 0 */
    int send_wqes_avail;      /* number of available send work queue elements for UD QP */
    int num_recvs_posted;     /* number of receive elements currently posted */
    int default_mtu_sz;       /* UD packet size */
    int credit_preserve;      /* low-water mark for number of posted receives */
    struct ibv_qp *qp;        /* UD QP */
    message_queue_t ext_send_queue; /* UD extended send queue */
    uint64_t ext_sendq_count; /* cumulative number of messages sent from UD extended send queue */
} mv2_ud_ctx_t;

typedef struct mv2_ud_qp_info {
    struct ibv_cq      *send_cq;
    struct ibv_cq      *recv_cq;
    struct ibv_srq     *srq;
    struct ibv_pd      *pd;
    struct ibv_qp_cap  cap;
    uint32_t           sq_psn;
} mv2_ud_qp_info_t;

/* IB address info for ud exhange */
typedef struct _mv2_ud_exch_info_t {
    uint16_t lid; /* lid of process */
    uint32_t qpn; /* queue pair of process */
} mv2_ud_exch_info_t;

typedef struct _mv2_rndv_qp_t {
    uint32_t seqnum;
    uint16_t index;
    uint16_t hca_num;
    
    struct ibv_qp *ud_qp;
    struct ibv_cq *ud_cq;

    void *next;
    void *prev;
} mv2_rndv_qp_t;

typedef struct _mv2_ud_zcopy_info_t {
    /* Rndv QP pool */
    mv2_rndv_qp_t *rndv_qp_pool;
    mv2_rndv_qp_t *rndv_qp_pool_free_head;
    int no_free_rndv_qp;
    char *grh_buf;
    void *grh_mr;
    
    struct ibv_cq **rndv_ud_cqs;
    mv2_ud_ctx_t **rndv_ud_qps;
} mv2_ud_zcopy_info_t;

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

/* ud vc info - tracks connection info between process pair */
typedef struct MPIDI_VC
{
    /* remote address info and VC state */
    struct ibv_ah *ah;            /* IB address of remote process */
    uint32_t qpn;                 /* queue pair number of remote process */
    uint16_t lid;                 /* lid of remote process */
    uint16_t state;               /* state of VC */

    /* read/write context ids */
    uint64_t readid;              /* remote proc labels its packets with this id when sending to us */
    uint64_t writeid;             /* we label our outgoing packets with this id */

    /* track sequence numbers and acks */
    uint16_t seqnum_next_tosend;  /* next sequence number to use when sending */
    uint16_t seqnum_next_torecv;  /* next sequence number needed for tail of in-order app receive window */
    uint16_t seqnum_next_toack;   /* sequence number to ACK in next ACK message */
    uint16_t ack_need_tosend;     /* whether we need to send an ACK on this VC */
    uint16_t ack_pending;         /* number of messages we've received w/o sending an ack */

    /* message queues */
    message_queue_t send_window;  /* VC send window */
    message_queue_t ext_window;   /* VC extended send window */
    message_queue_t recv_window;  /* VC out-of-order receive window */
    message_queue_t app_recv_window; /* in-order receive window */

    /* profiling counters */
    uint64_t cntl_acks;          /* number of explicit ACK messages sent */
    uint64_t resend_count;       /* number of resend operations */
    uint64_t ext_win_send_count; /* number of sends from extended send wnidow */
} MPIDI_VC_t;

typedef struct _mv2_proc_info_t {
    mv2_ud_ctx_t                *ud_ctx;
    message_queue_t             unack_queue;
    mv2_ud_zcopy_info_t         zcopy_info;
    int   (*post_send)(MPIDI_VC_t * vc, vbuf * v, int rail, mv2_ud_ctx_t *send_ud_ctx);
} mv2_proc_info_t;

void mv2_ud_zcopy_poll_cq(mv2_ud_zcopy_info_t *zcopy_info, mv2_ud_ctx_t *ud_ctx,
                                vbuf *resend_buf, int hca_index, int *found);

int post_ud_send(MPIDI_VC_t* vc, vbuf* v, int rail, mv2_ud_ctx_t *send_ud_ctx);
void mv2_check_resend();
void mv2_ud_update_send_credits(vbuf *v);
void mv2_send_explicit_ack(MPIDI_VC_t *vc);
int MRAILI_Process_send(void *vbuf_addr);
void MRAILI_Process_recv(vbuf *v);
void MPIDI_CH3I_MRAIL_Release_vbuf(vbuf * v);
inline vbuf* mv2_ud_apprecv_window_retrieve_and_remove(message_queue_t *q);

int mv2_ud_send(MPIDI_VC_t* vc, const void* buf, size_t size);
int mv2_ud_recv(MPIDI_VC_t* vc, void* buf, size_t size);

#endif /* #ifndef _MV2_UD_H_ */

