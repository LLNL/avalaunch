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

#include <netdb.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <mv2_spawn_net_clock.h>
#include <spawn_net.h>
#include <spawn_util.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <mv2_spawn_net_debug_utils.h>
#include <infiniband/verbs.h>
#include <infiniband/umad.h>

/* Enable UD */
#define _ENABLE_UD_     (1)

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
#define MAX_NUM_CQS                     (1)
#define MAX_NUM_PORTS                   (1)
#define MAX_NUM_HCAS                    (1)
#define RDMA_DEFAULT_PSN                (0)
#define RDMA_DEFAULT_PORT               (1)
#define RDMA_CONNECTION_INFO_LEN        (32)
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
#define RDMA_UD_NUM_MSG_LIMIT           (4096)
#define RDMA_UD_VBUF_POOL_SIZE          (8192)
#define DEFAULT_CM_THREAD_STACKSIZE     (1024*1024)

#define DEFAULT_NUM_RPOOLS         (1)

#   define MPIDI_Pkt_init(pkt_, type_)              \
    {                               \
    memset((void *) (pkt_), 0xfc, sizeof(*pkt_));   \
    (pkt_)->type = (type_);                 \
    }

#define GEN_EXIT_ERR     -1     /* general error which forces us to abort */
#define GEN_ASSERT_ERR   -2     /* general assert error */
#define IBV_RETURN_ERR   -3     /* gen2 function return error */
#define IBV_STATUS_ERR   -4     /*  gen2 function status error */

#define ibv_va_error_abort(code, message, args...)  {           \
    if (errno) {                                                \
        PRINT_ERROR_ERRNO( "%s:%d: " message, errno, __FILE__, __LINE__, ##args);     \
    } else {                                                    \
        PRINT_ERROR( "%s:%d: " message "\n", __FILE__, __LINE__, ##args);     \
    }                                                           \
    fflush (stderr);                                            \
    exit(code);                                                 \
}

#define ibv_error_abort(code, message)                          \
{                                                               \
    if (errno) {                                                \
        PRINT_ERROR_ERRNO( "%s:%d: " message, errno, __FILE__, __LINE__);     \
    } else {                                                    \
        PRINT_ERROR( "%s:%d: " message "\n", __FILE__, __LINE__);     \
    }                                                           \
    fflush (stderr);                                            \
    exit(code);                                                 \
}

#define MIN(x, y) (((x) < (y))?(x):(y))

enum MPIDI_CH3_Pkt_types
{
    MPIDI_CH3_PKT_UD_CONNECT,
    MPIDI_CH3_PKT_UD_ACCEPT,
    MPIDI_CH3_PKT_UD_DISCONNECT,
    MPIDI_CH3_PKT_UD_DATA,
    MPIDI_CH3_PKT_ZCOPY_FINISH,
    MPIDI_CH3_PKT_ZCOPY_ACK,
    MPIDI_CH3_PKT_MCST,
    MPIDI_CH3_PKT_MCST_NACK,
    MPIDI_CH3_PKT_MCST_INIT,
    MPIDI_CH3_PKT_MCST_INIT_ACK,
    MPIDI_CH3_PKT_NOOP,
    MPIDI_CH3_PKT_FLOW_CNTL_UPDATE,
    MPIDI_CH3_PKT_END_ALL,
    MPIDI_CH3_PKT_INVALID = -1 /* forces a signed enum to quash warnings */
};

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

extern int my_pg_rank;
extern int my_pg_size;
extern mv2_hca_info_t g_hca_info;
extern int rdma_num_hcas;
extern int rdma_num_rails;
extern int rdma_vbuf_max;
extern int rdma_enable_hugepage;
extern int rdma_vbuf_total_size;
extern int rdma_max_inline_size;
extern uint8_t rdma_enable_hybrid;
extern uint8_t rdma_enable_only_ud;
extern uint8_t rdma_use_ud_zcopy;
extern uint16_t rdma_default_ud_mtu;
extern uint32_t rdma_hybrid_enable_threshold;
extern uint32_t rdma_default_max_ud_send_wqe;
extern uint32_t rdma_default_max_ud_recv_wqe;
extern uint32_t rdma_default_ud_sendwin_size;
extern uint32_t rdma_default_ud_recvwin_size;
extern long rdma_ud_progress_timeout;
extern long rdma_ud_retry_timeout;
extern long rdma_ud_max_retry_timeout;
extern long rdma_ud_last_check;
extern uint16_t rdma_ud_max_retry_count;
extern uint16_t rdma_ud_progress_spin;
extern uint16_t rdma_ud_max_ack_pending;
extern uint16_t rdma_ud_num_rndv_qps;
extern uint32_t rdma_ud_num_msg_limit;
extern uint32_t rdma_ud_vbuf_pool_size;
extern uint32_t rdma_ud_zcopy_threshold;
extern uint32_t rdma_ud_zcopy_rq_size;
extern uint16_t rdma_hybrid_max_rc_conn;
extern uint16_t rdma_hybrid_pending_rc_conn;
extern int rdma_vbuf_secondary_pool_size;

int mv2_hca_open();
void comm_lock(void);
void comm_unlock(void);
spawn_net_endpoint* mv2_init_ud();
spawn_net_channel* mv2_ep_connect(const char *name);
spawn_net_channel* mv2_ep_accept();
//int mv2_ud_send(MPIDI_VC_t* vc, const void* buf, size_t size);
//int mv2_ud_recv(MPIDI_VC_t* vc, void* buf, size_t size);

#endif /* _IB_INTERNAL_H */
