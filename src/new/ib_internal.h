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
#ifndef _IB_INTERNAL_H
#define _IB_INTERNAL_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <spawn_util.h>
#include <infiniband/verbs.h>
#include <infiniband/umad.h>

#ifdef __ia64__
/* Only ia64 requires this */
#define SHMAT_ADDR (void *)(0x8000000000000000UL)
#define SHMAT_FLAGS (SHM_RND)
#else
#define SHMAT_ADDR (void *)(0x0UL)
#define SHMAT_FLAGS (0)
#endif /* __ia64__*/
#define HUGEPAGE_ALIGN  (2*1024*1024)

#define DEF_NUM_CQS                     (1)
#define MAX_NUM_CQS                     (2)
#define MAX_NUM_PORTS                   (2)
#define MAX_NUM_HCAS                    (1)
#define RDMA_DEFAULT_PSN                (0)
#define RDMA_DEFAULT_PORT               (1)
#define DEFAULT_GID_INDEX               (0)
#define DEF_NUM_BUFFERS                 (128)
#define DEF_BUFFER_SIZE                 (1024)
#define DEF_POLLING_THRESHOLD           (-1)
#define RDMA_DEFAULT_UD_MTU             (2048)
#define RDMA_DEFAULT_NUM_VBUFS          (256)
#define RDMA_VBUF_SECONDARY_POOL_SIZE   (512)
#define RDMA_DEFAULT_SERVICE_LEVEL      (0)
#define RDMA_DEFAULT_MAX_CQ_SIZE        (40000)
#define RDMA_DEFAULT_PSN                (0)
#define RDMA_DEFAULT_MAX_SG_LIST        (1)
#define RDMA_DEFAULT_MAX_UD_SEND_WQE    (2048)
#define RDMA_DEFAULT_MAX_UD_RECV_WQE    (4096)
#define RDMA_DEFAULT_MAX_INLINE_SIZE    (128)


#define DEFAULT_NUM_RPOOLS         (1)

#define LOG2(_v, _r)                            \
do {                                            \
    (_r) = ((_v) & 0xFF00) ? 8 : 0;             \
    if ( (_v) & ( 0x0F << (_r + 4 ))) (_r)+=4;  \
    if ( (_v) & ( 0x03 << (_r + 2 ))) (_r)+=2;  \
    if ( (_v) & ( 0x01 << (_r + 1 ))) (_r)+=1;  \
} while(0)

#define VBUF_FLAG_TYPE uint64_t

#define CREDIT_VBUF_FLAG (111)
#define NORMAL_VBUF_FLAG (222)
#define RPUT_VBUF_FLAG (333)
#define RGET_VBUF_FLAG (444)
#define RDMA_ONE_SIDED (555)
#define COLL_VBUF_FLAG (666)

#define UD_VBUF_FREE_PENIDING       (0x01)
#define UD_VBUF_SEND_INPROGRESS     (0x02)
#define UD_VBUF_RETRY_ALWAYS        (0x04)
#define UD_VBUF_MCAST_MSG           (0x08)

#define MRAILI_ALIGN_LEN(len, align_unit)           \
{                                                   \
    len = ((int)(((len)+align_unit-1) /             \
                align_unit)) * align_unit;          \
}

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

struct ibv_wr_descriptor
{
    union
    {
        struct ibv_recv_wr rr;
        struct ibv_send_wr sr;
    } u;
    union
    {
        struct ibv_send_wr* bad_sr;
        struct ibv_recv_wr* bad_rr;
    } y;
    struct ibv_sge sg_entry;
    void* next;
};

typedef enum {
    IB_TRANSPORT_UD = 1,
    IB_TRANSPORT_RC = 2,
} ib_transport;

typedef struct link
{
    void *next;
    void *prev;
} LINK;

typedef struct vbuf
{
    struct ibv_wr_descriptor desc;
    void* pheader;
    void* sreq;
    struct vbuf_region* region;
    void* vc;
    int rail;
    int padding;
    VBUF_FLAG_TYPE* head_flag;
    unsigned char* buffer;

    int content_size;
    int content_consumed;

    /* used to keep track of eager sends */
    uint8_t eager;
    uint8_t coalesce;

    /* used to keep one sided put get list */
    void * list;

    /* NULL shandle means not send or not complete. Non-null
     * means pointer to send handle that is now complete. Used
     * by MRAILI_Process_send */
    ib_transport transport;
    uint16_t seqnum;
    uint16_t retry_count;
    uint16_t pending_send_polls;
    uint8_t flags;
    double timestamp;
    uint8_t in_sendwin;
    LINK sendwin_msg;
    LINK recvwin_msg;
    LINK extwin_msg;
    LINK unack_msg;
} vbuf;

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

/* The data structure to hold vbuf pool info */
typedef struct vbuf_pool
{
    uint8_t index;
    uint16_t initial_count;
    uint16_t incr_count;
    uint32_t buf_size;
    uint32_t num_allocated;
    uint32_t num_free;
    uint32_t max_num_buf;
    long num_get;
    long num_freed;
    vbuf *free_head;
    vbuf_region *region_head;
}vbuf_pool_t;

/* hca_info */
typedef struct _mv2_hca_info_t {
    struct ibv_pd *pd;
    struct ibv_device *device;
    struct ibv_context *context;
    struct ibv_cq  *cq_hndl;
    struct ibv_comp_channel     *comp_channel;
    union  ibv_gid gid[MAX_NUM_PORTS];
    struct ibv_port_attr port_attr[MAX_NUM_PORTS];
    struct ibv_device_attr device_attr;
} mv2_hca_info_t;

typedef struct message_queue_t
{
    struct vbuf *head;
    struct vbuf *tail;
    uint16_t count;
} message_queue_t;

typedef struct mv2_ud_ctx_t mv2_ud_ctx_t;
/* ud context */
struct mv2_ud_ctx_t{
    int hca_num;
    int send_wqes_avail;
    int num_recvs_posted;
    int default_mtu_sz;
    int credit_preserve;
    struct ibv_qp *qp;
    message_queue_t ext_send_queue;
    message_queue_t unack_queue;
    uint64_t ext_sendq_count;
};

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
}mv2_ud_vc_info_t;

/* ud exhange info */
typedef struct _mv2_ud_exch_info_t
{
    uint16_t lid;
    uint32_t qpn;
}mv2_ud_exch_info_t;

/* create UD context */
static mv2_ud_ctx_t* mv2_ud_create_ctx (mv2_ud_qp_info_t *qp_info);

static int mv2_ud_qp_transition(struct ibv_qp *qp);

/* create UD QP */
static struct ibv_qp *mv2_ud_create_qp (mv2_ud_qp_info_t *qp_info);

/* create ud vc */
int mv2_ud_set_vc_info (mv2_ud_vc_info_t *,
        mv2_ud_exch_info_t *rem_info, 
        struct ibv_pd *pd, int rdma_default_port);

/* destroy ud context */
void mv2_ud_destroy_ctx (mv2_ud_ctx_t *ctx);
void mv2_ud_resend(vbuf *v);

#endif /* _IB_INTERNAL_H */
