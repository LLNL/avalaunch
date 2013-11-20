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

#ifndef _VBUF_H_
#define _VBUF_H_

#include "ib_internal.h"

#define CREDIT_VBUF_FLAG (111)
#define NORMAL_VBUF_FLAG (222)
#define RPUT_VBUF_FLAG (333)
#define RGET_VBUF_FLAG (444)
#define RDMA_ONE_SIDED (555)
#define COLL_VBUF_FLAG (666)
/*
** FIXME: Change the size of VBUF_FLAG_TYPE to 4 bytes when size of
** MPIDI_CH3_Pkt_send is changed to mutliple of 4. This will fix the 
** issue of recv memcpy alignment.
*/
#define VBUF_FLAG_TYPE uint64_t

#define FREE_FLAG (0)
#define BUSY_FLAG (1)
#define PKT_NO_SEQ_NUM -2
#define PKT_IS_NULL -1

#define MRAILI_ALIGN_LEN(len, align_unit)           \
{                                                   \
    len = ((int)(((len)+align_unit-1) /             \
                align_unit)) * align_unit;          \
}

#define ROUNDUP(len, unit) ((len + unit - 1) / unit) 

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#ifdef __ia64__
/* Only ia64 requires this */
#define SHMAT_ADDR (void *)(0x8000000000000000UL)
#define SHMAT_FLAGS (SHM_RND)
#else
#define SHMAT_ADDR (void *)(0x0UL)
#define SHMAT_FLAGS (0)
#endif /* __ia64__*/
#define HUGEPAGE_ALIGN  (2*1024*1024)

/*
 * brief justification for vbuf format:
 * descriptor must be aligned (64 bytes).
 * vbuf size must be multiple of this alignment to allow contiguous allocation
 * descriptor and buffer should be contiguous to allow via implementations that
 * optimize contiguous descriptor/data (? how likely ?)
 * need to be able to store send handle in vbuf so that we can mark sends
 * complete when communication completes. don't want to store
 * it in packet header because we don't always need to send over the network.
 * don't want to store at beginning or between desc and buffer (see above) so
 * store at end.
 */

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

#define VBUF_BUFFER_SIZE (rdma_vbuf_total_size)

#define MRAIL_MAX_EAGER_SIZE VBUF_BUFFER_SIZE
typedef enum {
    IB_TRANSPORT_UD = 1,
    IB_TRANSPORT_RC = 2,
} ib_transport;

#define UD_VBUF_FREE_PENIDING       (0x01)
#define UD_VBUF_SEND_INPROGRESS     (0x02)
#define UD_VBUF_RETRY_ALWAYS        (0x04)
#define UD_VBUF_MCAST_MSG           (0x08)

#define MRAILI_Get_buffer(_vc, _v)                  \
do {                                                \
    if((_vc)->mrail.state & MRAILI_RC_CONNECTED) {  \
        (_v) = get_vbuf();                          \
    } else  {                                       \
        (_v) = get_ud_vbuf();                       \
    }                                               \
} while (0)

/* ibverbs reserves the first 40 bytes of each UD packet, this may
 * sometimes contain valid data for a Global Routine Header */
#define MV2_UD_GRH_LEN (40)

#define PKT_TRANSPORT_OFFSET(_v) ((_v->transport == IB_TRANSPORT_UD) ? MV2_UD_GRH_LEN : 0)

/* extend this macro if there is more control messages */
#define IS_CNTL_MSG(p) \
    (p->type ==  MPIDI_CH3_PKT_FLOW_CNTL_UPDATE || \
     p->type ==  MPIDI_CH3_PKT_UD_CONNECT || \
     p->type ==  MPIDI_CH3_PKT_UD_ACCEPT || \
     p->type ==  MPIDI_CH3_PKT_UD_DISCONNECT || \
     p->type ==  MPIDI_CH3_PKT_NOOP)

#define IS_MCAST_MSG(p) \
    (p->type == MPIDI_CH3_PKT_MCST || \
        p->type == MPIDI_CH3_PKT_MCST_INIT || \
            (mcast_use_mcast_nack && p->type == MPIDI_CH3_PKT_MCST_NACK))

#define SET_PKT_LEN_HEADER(_v, _wc) {                                       \
    if(IB_TRANSPORT_UD == (_v)->transport) {                                \
        (_v)->content_size = (_wc).byte_len - MV2_UD_GRH_LEN ;              \
    } else {                                                                \
        (_v)->content_size= _wc.byte_len;                                   \
    }                                                                       \
}

#define SET_PKT_HEADER_OFFSET(_v) {                                         \
    (_v)->pheader = (_v)->buffer + PKT_TRANSPORT_OFFSET(_v);                \
}

#define PKT_DATA_OFFSET(_v, _header_size) (_v)->pheader + _header_size;

#define PKT_DATA_SIZE(_v, _header_size) (_v)->content_size - _header_size;

#define MRAIL_MAX_UD_SIZE (RDMA_DEFAULT_UD_MTU - MV2_UD_GRH_LEN)

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
     * by MRAILI_Process_send
     */
    ib_transport transport;
    uint16_t seqnum;
    uint16_t retry_count;
    uint16_t pending_send_polls;
    uint8_t flags;
    double timestamp;
    uint8_t in_sendwin;
    LINK apprecvwin_msg; /* tracks in-order packets ready to be received by app */
    LINK sendwin_msg;    /* tracks outstanding sends */
    LINK recvwin_msg;    /* tracks received packets, either control msgs or out-of-order app msgs */
    LINK extwin_msg;     /* tracks messages to be sent when credits are availble */
    LINK unack_msg;
#if defined(_MCST_SUPPORT_)
    LINK mcast_sendwin_msg;
    LINK mcast_recvwin_msg;
#endif
} vbuf;

/* one for head and one for tail */
#define VBUF_FAST_RDMA_EXTRA_BYTES (2 * sizeof(VBUF_FLAG_TYPE))

#define FAST_RDMA_ALT_TAG 0x8000
#define FAST_RDMA_SIZE_MASK 0x7fff

#if defined(DEBUG)
void dump_vbuf(char* msg, vbuf* v);
#else /* defined(DEBUG) */
#define dump_vbuf(msg, v)
#endif /* defined(DEBUG) */

void mv2_print_vbuf_usage_usage();
int init_vbuf_lock(void);

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

#define RDMA_VBUF_POOL_INIT(rdma_vbuf_pool)     \
do{                                             \
    rdma_vbuf_pool.free_head = NULL;            \
    rdma_vbuf_pool.region_head = NULL;          \
    rdma_vbuf_pool.buf_size = 0;                \
    rdma_vbuf_pool.num_allocated = 0;           \
    rdma_vbuf_pool.num_free = 0;                \
    rdma_vbuf_pool.max_num_buf = -1;            \
    rdma_vbuf_pool.num_get = 0;                 \
    rdma_vbuf_pool.num_freed = 0;               \
} while(0)
    
static inline void VBUF_SET_RDMA_ADDR_KEY(
    vbuf* v, 
    int len,
    void* local_addr,
    uint32_t lkey,
    void* remote_addr,
    uint32_t rkey)
{
    v->desc.u.sr.next = NULL;
    v->desc.u.sr.opcode = IBV_WR_RDMA_WRITE;
    v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    v->desc.u.sr.wr_id = (uintptr_t) v;

    v->desc.u.sr.num_sge = 1;
    v->desc.u.sr.sg_list = &(v->desc.sg_entry);

    (v)->desc.u.sr.wr.rdma.remote_addr = (uintptr_t) (remote_addr);
    (v)->desc.u.sr.wr.rdma.rkey = (rkey);
    (v)->desc.sg_entry.length = (len);
    (v)->desc.sg_entry.lkey = (lkey);
    (v)->desc.sg_entry.addr = (uintptr_t)(local_addr);
}

int allocate_vbufs(struct ibv_pd* ptag[], int nvbufs);

void deallocate_vbufs(int);
void deallocate_vbuf_region(void);

vbuf* get_vbuf(void);

vbuf* get_ud_vbuf(void);
int allocate_ud_vbufs(int nvbufs);
void vbuf_init_ud_recv(vbuf* v, unsigned long len, int rail);

void MRAILI_Release_vbuf(vbuf* v);

void vbuf_init_rdma_write(vbuf* v);

void vbuf_init_send(vbuf* v, unsigned long len, int rail);

void vbuf_init_recv(vbuf* v, unsigned long len, int rail);

void vbuf_init_rput(
    vbuf* v,
    void* local_address,
    uint32_t lkey,
    void* remote_address,
    uint32_t rkey,
    int nbytes,
    int rail);

void vbuf_init_rget(
    vbuf* v,
    void* local_address,
    uint32_t lkey,
    void* remote_address,
    uint32_t rkey,
    int nbytes,
    int rail);

void vbuf_init_rma_get(
    vbuf* v,
    void* local_address,
    uint32_t lkey,
    void* remote_address,
    uint32_t rkey,
    int nbytes,
    int rail);

void vbuf_init_rma_put(
    vbuf* v,
    void* local_address,
    uint32_t lkey,
    void* remote_address,
    uint32_t rkey,
    int nbytes,
    int rail);

void vbuf_init_rma_compare_and_swap(
    vbuf *v, 
    void *local_address,
    uint32_t lkey,
    void *remote_address, 
    uint32_t rkey, 
    uint64_t compare,
    uint64_t swap, 
    int rail);

void vbuf_init_rma_fetch_and_add(
    vbuf *v, 
    void *local_address, 
    uint32_t lkey,
    void *remote_address, 
    uint32_t rkey, 
    uint64_t add,
    int rail);

extern vbuf_pool_t *rdma_vbuf_pools;
extern int rdma_num_vbuf_pools;

#endif /* _VBUF_H_ */
