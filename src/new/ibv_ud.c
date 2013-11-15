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

#include "vbuf.h"
#include <ib_internal.h>

#ifdef _ENABLE_UD_
#include "mv2_ud.h"
#include "mv2_ud_inline.h"
#include <debug_utils.h>

long rdma_ud_last_check;

extern mv2_proc_info_t proc;

static inline void mv2_ud_flush_ext_window(MPIDI_VC_t *vc)
{
    vbuf *next;
    message_queue_t *q = &vc->mrail.ud.ext_window;
    while (q->head != NULL && 
            vc->mrail.ud.send_window.count < rdma_default_ud_sendwin_size) {
        next = (q->head)->extwin_msg.next;
        proc.post_send(vc, q->head, (q->head)->rail, NULL);
        PRINT_DEBUG(DEBUG_UD_verbose>1,"Send ext window message(%p) nextseqno :"
                "%d\n", next, vc->mrail.seqnum_next_tosend);
        q->head = next;
        --q->count;
        vc->mrail.ud.ext_win_send_count++;
    }
    if (q->head == NULL) {
        assert(q->count == 0);
        q->tail = NULL;
    }
}

static inline void mv2_ud_process_ack(MPIDI_VC_t *vc, uint16_t acknum)
{
    vbuf *sendwin_head = vc->mrail.ud.send_window.head;

    PRINT_DEBUG(DEBUG_UD_verbose>2,"ack recieved: %d next_to_ack: %d\n",acknum, vc->mrail.seqnum_next_toack);

    while (sendwin_head != NULL && 
            INCL_BETWEEN (acknum, sendwin_head->seqnum, vc->mrail.seqnum_next_tosend))
    {
        mv2_ud_send_window_remove(&vc->mrail.ud.send_window, sendwin_head);
        mv2_ud_unack_queue_remove(&(proc.unack_queue), sendwin_head);
        MRAILI_Process_send(sendwin_head);
        sendwin_head = vc->mrail.ud.send_window.head;
    }

    /*see if we can flush from ext window queue */
    if (vc->mrail.ud.ext_window.head != NULL && 
            vc->mrail.ud.send_window.count < rdma_default_ud_sendwin_size) {
        mv2_ud_flush_ext_window(vc);
    }    
}

inline void mv2_ud_apprecv_window_add(message_queue_t *q, vbuf *v)
{
    v->apprecvwin_msg.next = v->apprecvwin_msg.prev = NULL;

    if(q->head == NULL) {
        q->head = v;
    } else {
        (q->tail)->apprecvwin_msg.next = v;
    }

    q->tail = v;
    q->count++;
}

inline vbuf* mv2_ud_apprecv_window_retrieve_and_remove(message_queue_t *q)
{
    vbuf *v = q->head;

    if (v == NULL) {
        return NULL;
    }

    q->head = v->apprecvwin_msg.next;
    q->count--;
    if (q->head == NULL ) {
        q->tail = NULL;
        assert(q->count == 0);
    }

    v->apprecvwin_msg.next = NULL;

    return v;
}

static inline void mv2_ud_place_recvwin(vbuf *v)
{
    MPIDI_VC_t *vc;
    int recv_win_start;
    int recv_win_end;
    int ret;

    vc = v->vc;
    recv_win_start = vc->mrail.seqnum_next_torecv;
    recv_win_end = (recv_win_start + rdma_default_ud_recvwin_size) % MAX_SEQ_NUM;

    /* check if the packet is in the window or not */
    if (INCL_BETWEEN(v->seqnum, recv_win_start, recv_win_end)) {
        if (v->seqnum == vc->mrail.seqnum_next_torecv) {
            PRINT_DEBUG(DEBUG_UD_verbose>2,"get one with in-order seqnum:%d \n",v->seqnum);
            /* process in-order message; add to app_recv window */
            mv2_ud_apprecv_window_add(&vc->mrail.app_recv_window, v);
            vc->mrail.seqnum_next_toack = vc->mrail.seqnum_next_torecv;
            ++vc->mrail.seqnum_next_torecv;
            if (v->transport == IB_TRANSPORT_UD) {
                MARK_ACK_REQUIRED(vc);
            }
        } else {
            /* we are not in order */
            PRINT_DEBUG(DEBUG_UD_verbose>1,"Got out-of-order packet recv:%d expected:%d\n",v->seqnum, vc->mrail.seqnum_next_torecv);
            ret = mv2_ud_recv_window_add(&vc->mrail.ud.recv_window, v, vc->mrail.seqnum_next_torecv);
            if (ret == MSG_IN_RECVWIN) {
                MPIDI_CH3I_MRAIL_Release_vbuf(v);
            }

            if (v->transport == IB_TRANSPORT_UD) {
                MARK_ACK_REQUIRED(vc);
            }
        }

        /* process in-order messages in recv windiw head */
        while ( vc->mrail.ud.recv_window.head != NULL && 
                (vc->mrail.ud.recv_window.head->seqnum == 
                 vc->mrail.seqnum_next_torecv)) {
            PRINT_DEBUG(DEBUG_UD_verbose>1,"get one with in-order seqnum:%d \n",vc->mrail.seqnum_next_torecv);
            /* process in-order message; add to app_recv window */
            mv2_ud_apprecv_window_add(&vc->mrail.app_recv_window, vc->mrail.ud.recv_window.head);

            mv2_ud_recv_window_remove(&vc->mrail.ud.recv_window);
            vc->mrail.seqnum_next_toack = vc->mrail.seqnum_next_torecv;
            ++vc->mrail.seqnum_next_torecv;
        }
    } else {
        PRINT_DEBUG(DEBUG_UD_verbose>1,"Message is not in recv window seqnum:%d win start:%d win end:%d\n", v->seqnum, recv_win_start, recv_win_end);
        MPIDI_CH3I_MRAIL_Release_vbuf(v);
        if (v->transport == IB_TRANSPORT_UD) {
            MARK_ACK_REQUIRED(vc);
        }
    }
}

int post_ud_send(MPIDI_VC_t* vc, vbuf* v, int rail, mv2_ud_ctx_t *send_ud_ctx)
{
    mv2_ud_ctx_t *ud_ctx = send_ud_ctx; 
    MPIDI_CH3I_MRAILI_Pkt_comm_header *p = v->pheader;

    assert(v->desc.sg_entry.length <= MRAIL_MAX_UD_SIZE);
    if (send_ud_ctx == NULL ) {
        ud_ctx = proc.ud_ctx;
    }
    v->vc = (void *)vc;
    p->rail = rail;
    p->src.rank = vc->mrail.writeid;
    assert(v->transport == IB_TRANSPORT_UD);

    SEND_WINDOW_CHECK(&vc->mrail.ud, v);

    v->seqnum = p->seqnum = vc->mrail.seqnum_next_tosend;
    vc->mrail.seqnum_next_tosend++;

    p->acknum = vc->mrail.seqnum_next_toack;
    MARK_ACK_COMPLETED(vc);
    MV2_UD_RESET_CREDITS(vc, v);
    v->flags |= UD_VBUF_SEND_INPROGRESS;

    PRINT_DEBUG(DEBUG_UD_verbose>1,"UD Send : to:%d seqnum:%d acknum:%d len:%d\n", 
                vc->pg_rank, p->seqnum, p->acknum, v->desc.sg_entry.length);

    IBV_UD_POST_SR(v, vc->mrail.ud, ud_ctx);

    mv2_ud_track_send(&vc->mrail.ud, &proc.unack_queue, v);     

    return 0;
}

void mv2_send_control_msg(MPIDI_VC_t *vc, vbuf *v)
{

    mv2_ud_ctx_t *ud_ctx;
    MPIDI_CH3I_MRAILI_Pkt_comm_header *p = v->pheader;

    ud_ctx = proc.ud_ctx;
    v->vc = (void *)vc;
    p->src.rank = vc->mrail.writeid;
    assert(v->transport == IB_TRANSPORT_UD);

    v->seqnum = p->seqnum = -1;
    MARK_ACK_COMPLETED(vc);
    MV2_UD_RESET_CREDITS(vc, v);

    IBV_UD_POST_SR(v, vc->mrail.ud, ud_ctx);

    vc->mrail.ud.cntl_acks++;
}

static inline void mv2_ud_ext_sendq_send(MPIDI_VC_t *vc, mv2_ud_ctx_t *ud_ctx)
{
    vbuf *v;
    while (ud_ctx->send_wqes_avail > 0 && ud_ctx->ext_send_queue.head) {
        v = ud_ctx->ext_send_queue.head;
        ud_ctx->ext_send_queue.head = ud_ctx->ext_send_queue.head->desc.next;
        if (NULL == ud_ctx->ext_send_queue.head) {
            ud_ctx->ext_send_queue.tail = NULL;
        }
        ud_ctx->ext_send_queue.count--;
        v->desc.next = NULL;

        /* can we reset ack to latest? */
        ud_ctx->send_wqes_avail--;
        if (ibv_post_send(ud_ctx->qp, &(v->desc.u.sr),&(v->desc.y.bad_sr))) {
            ibv_error_abort(-1, "extend sendq send  failed");
        }
        ud_ctx->ext_sendq_count++;
        PRINT_DEBUG(DEBUG_UD_verbose>1,"sending from ext send queue seqnum :%d qlen:%d\n", v->seqnum, ud_ctx->ext_send_queue.count);
    } 
}

void mv2_ud_update_send_credits(vbuf *v)
{
    mv2_ud_ctx_t *ud_ctx;
    ud_ctx = proc.ud_ctx;
    ud_ctx->send_wqes_avail++;
    PRINT_DEBUG(DEBUG_UD_verbose>2,"available wqes : %d seqno:%d \n",ud_ctx->send_wqes_avail, v->seqnum);
    if (NULL != ud_ctx->ext_send_queue.head 
            && ud_ctx->send_wqes_avail > 0) {
        mv2_ud_ext_sendq_send(v->vc, ud_ctx);   
    }
}

void mv2_send_explicit_ack(MPIDI_VC_t *vc)
{
    vbuf *v = get_ud_vbuf();

    MPIDI_CH3I_MRAILI_Pkt_comm_header *ack_pkt = v->pheader;
    MPIDI_Pkt_init(ack_pkt, MPIDI_CH3_PKT_FLOW_CNTL_UPDATE);
    ack_pkt->acknum = vc->mrail.seqnum_next_toack;
    vbuf_init_send(v, sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header), 0);
    ack_pkt->rail = v->rail;

    mv2_send_control_msg(vc, v);
    PRINT_DEBUG(DEBUG_UD_verbose>1,"Sent explicit ACK to :%d acknum:%d\n", vc->pg_rank, ack_pkt->acknum);
}

void mv2_ud_resend(vbuf *v)
{

    MPIDI_CH3I_MRAILI_Pkt_comm_header *p;
    MPIDI_VC_t *vc;
    mv2_ud_ctx_t *ud_ctx;

    if (v->flags & UD_VBUF_SEND_INPROGRESS && 
            !(v->flags & UD_VBUF_RETRY_ALWAYS)) {
        return;
    }

    v->retry_count++;
    if (v->retry_count > rdma_ud_max_retry_count) {
        PRINT_ERROR ("UD reliability error. Exeeced max retries(%d) "
                "in resending the message(%p). current retry timeout(us): %lu. "
                "This Error may happen on clusters based on the InfiniBand "
                "topology and traffic patterns. Please try with increased "
                "timeout using MV2_UD_RETRY_TIMEOUT\n", 
                v->retry_count, v, rdma_ud_retry_timeout );
        exit(EXIT_FAILURE);
    }

    p = v->pheader;
    vc = v->vc;
#if 0
    if (p->type == MPIDI_CH3_PKT_ZCOPY_FINISH) {
        int found;
        int hca_index = ((MPIDI_CH3_Pkt_zcopy_finish_t *)p)->hca_index;
        ud_ctx = proc.zcopy_info.rndv_ud_qps[hca_index];
        do {
            mv2_ud_zcopy_poll_cq(&proc.zcopy_info, 
                                            ud_ctx, v, hca_index, &found);
        } while( ud_ctx->send_wqes_avail <=0 || found);
    } else
#endif
    {
        ud_ctx = proc.ud_ctx;
    }
    p->acknum = vc->mrail.seqnum_next_toack;
    MARK_ACK_COMPLETED(vc);
    v->flags |= UD_VBUF_SEND_INPROGRESS;

    if (ud_ctx->send_wqes_avail <= 0) {
        mv2_ud_ext_sendq_queue(&ud_ctx->ext_send_queue, v);
    } else {
        ud_ctx->send_wqes_avail--;
        if (ibv_post_send(ud_ctx->qp, &(v->desc.u.sr),&(v->desc.y.bad_sr))) {
            ibv_error_abort(-1, "reliability resend failed");
        }
    }
    vc->mrail.ud.resend_count++;
}    

void MRAILI_Process_recv(vbuf *v) 
{
    MPIDI_CH3I_MRAILI_Pkt_comm_header *p = v->pheader;

    mv2_ud_process_ack(v->vc, p->acknum);

    if (IS_CNTL_MSG(p) &&
        p->type != MPIDI_CH3_PKT_UD_ACCEPT)
    {
        PRINT_DEBUG(DEBUG_UD_verbose>1,"recv cntl message ack:%d \n", p->acknum);
        MRAILI_Release_vbuf(v);
        goto fn_exit;
    }

    MV2_UD_ACK_CREDIT_CHECK(((MPIDI_VC_t *)v->vc), v);
    mv2_ud_place_recvwin(v); 

fn_exit:
    return;
}

void mv2_check_resend()
{
    mv2_ud_unackq_traverse(&proc.unack_queue);
}

#endif
