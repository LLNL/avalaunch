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

#ifndef _SPAWN_NET_IB_VBUF_H_
#define _SPAWN_NET_IB_VBUF_H_

#include "spawn_net_ib_internal.h"

#define NORMAL_VBUF_FLAG (222)
/*
** FIXME: Change the size of VBUF_FLAG_TYPE to 4 bytes when size of
** MPIDI_CH3_Pkt_send is changed to mutliple of 4. This will fix the 
** issue of recv memcpy alignment.
*/
#define VBUF_FLAG_TYPE uint64_t

#define MRAILI_ALIGN_LEN(len, align_unit)           \
{                                                   \
    len = ((int)(((len)+align_unit-1) /             \
                align_unit)) * align_unit;          \
}

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

typedef enum {
    IB_TRANSPORT_UD = 1,
    IB_TRANSPORT_RC = 2,
} ib_transport;

#define UD_VBUF_FREE_PENIDING       (0x01)
#define UD_VBUF_SEND_INPROGRESS     (0x02)
#define UD_VBUF_RETRY_ALWAYS        (0x04)
#define UD_VBUF_MCAST_MSG           (0x08)

/* ibverbs reserves the first 40 bytes of each UD packet, this may
 * sometimes contain valid data for a Global Routine Header */
#define MV2_UD_GRH_LEN (40)

#define PKT_TRANSPORT_OFFSET(_v) ((_v->transport == IB_TRANSPORT_UD) ? MV2_UD_GRH_LEN : 0)

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
//    uint8_t eager;
//    uint8_t coalesce;
  
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
} vbuf;

int vbuf_init();

vbuf* vbuf_get(struct ibv_pd* pd);

void vbuf_release(vbuf* v);

void vbuf_prepare_recv(vbuf* v, unsigned long len, int rail);

void vbuf_prepare_send(vbuf* v, unsigned long len, int rail);

#endif /* _SPAWN_NET_IB_VBUF_H_ */
