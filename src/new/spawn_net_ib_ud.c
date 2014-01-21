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

#include "spawn_net_ib_vbuf.h"
#include "spawn_net_ib_internal.h"
#include "spawn_net_ib_ud.h"
#include "spawn_net_ib_ud_inline.h"
#include "spawn_net_ib_debug_utils.h"

long rdma_ud_last_check;

extern mv2_proc_info_t proc;

/* churn through and send as many as packets as we can from the
 * VC extended send queue */
static inline void mv2_ud_flush_ext_window(MPIDI_VC_t *vc)
{
    /* get pointer to ud info, send queue, and extended send queue */
    message_queue_t* sendwin = &vc->send_window;
    message_queue_t* extwin  = &vc->ext_window;

    /* get pointer to head of extended send queue */
    vbuf* cur = extwin->head;
    while (cur != NULL &&
           sendwin->count < rdma_default_ud_sendwin_size)
    {
        /* get pointer to next element in list */
        vbuf* next = cur->extwin_msg.next;

        /* remove item from head of list */
        extwin->head = next;
        extwin->count--;

        /* send item, associated vbuf will be released when send
         * completion event is processed */
        proc.post_send(vc, cur, cur->rail, NULL);

        /* track number of sends from extended send queue */
        vc->ext_win_send_count++;

        /* go on to next item */
        cur = next;
    }

    /* update queue fields if we emptied the list */
    if (extwin->head == NULL) {
        extwin->tail = NULL;
        assert(extwin->count == 0);
    }

    return;
}

/* given a VC and a seq number, remove all items in send and unack'd
 * queues up to and including this seq number */
static inline void mv2_ud_process_ack(MPIDI_VC_t *vc, uint16_t acknum)
{
    PRINT_DEBUG(DEBUG_UD_verbose>2,"ack recieved: %d next_to_ack: %d\n",
        acknum, vc->seqnum_next_toack);

    /* get pointer to ud info, send queue, and extended send queue */
    message_queue_t* sendwin = &vc->send_window;
    message_queue_t* extwin  = &vc->ext_window;

    /* while we have a vbuf, and while its seq number is before seq
     * number in ack, remove it from send and unack queues */
    vbuf* cur = sendwin->head;
    while (cur != NULL &&
           INCL_BETWEEN(acknum, cur->seqnum, vc->seqnum_next_tosend))
    {
        /* the current vbuf has been ack'd, so remove it from the send
         * window and also the unack'd list */
        mv2_ud_send_window_remove(sendwin, cur);
        mv2_ud_unack_queue_remove(&proc.unack_queue, cur);

        /* release vbuf */
        MRAILI_Process_send(cur);

        /* get next packet in send window */
        cur = sendwin->head;
    }

    /* see if we can flush from ext window queue */
    if (extwin->head != NULL &&
        sendwin->count < rdma_default_ud_sendwin_size)
    {
        mv2_ud_flush_ext_window(vc);
    }

    return;
}

/* add vbuf to tail of apprecv queue */
inline void mv2_ud_apprecv_window_add(message_queue_t *q, vbuf *v)
{
    /* set next and prev pointers ton vbuf */
    v->apprecvwin_msg.next = NULL;
    v->apprecvwin_msg.prev = NULL;

    /* for empty list, update head, otherwise update next pointer
     * of last item in list to point to vbuf */
    if(q->head == NULL) {
        q->head = v;
    } else {
        (q->tail)->apprecvwin_msg.next = v;
    }

    /* point tail to vbuf and increase count */
    q->tail = v;
    q->count++;

    return;
}

/* remove and return vbuf from apprecv queue */
inline vbuf* mv2_ud_apprecv_window_retrieve_and_remove(message_queue_t *q)
{
    /* get pointer to first item in list */
    vbuf* v = q->head;

    /* return right away if it's empty */
    if (v == NULL) {
        return NULL;
    }

    /* update head to point to next item and decrement length of queue */
    q->head = v->apprecvwin_msg.next;
    q->count--;

    /* if we emptied the list, update the tail */
    if (q->head == NULL ) {
        q->tail = NULL;
        assert(q->count == 0);
    }

    /* clear next pointer in vbuf before return it */
    v->apprecvwin_msg.next = NULL;

    return v;
}

static inline void mv2_ud_place_recvwin(vbuf *v)
{
    int ret;

    /* get VC vbuf is for */
    MPIDI_VC_t* vc = v->vc;

    /* determine bounds of recv window */
    int recv_win_start = vc->seqnum_next_torecv;
    int recv_win_end = recv_win_start + rdma_default_ud_recvwin_size;
    while (recv_win_end > MAX_SEQ_NUM) {
        recv_win_end -= MAX_SEQ_NUM;
    }

    /* check if the packet seq num is in the window or not */
    if (INCL_BETWEEN(v->seqnum, recv_win_start, recv_win_end)) {
        /* get pointer to recv window */
        message_queue_t* recvwin = &vc->recv_window;

        /* got a packet within range, now check whether its in order or not */
        if (v->seqnum == vc->seqnum_next_torecv) {
            PRINT_DEBUG(DEBUG_UD_verbose>2,"get one with in-order seqnum:%d \n",
              v->seqnum);

            /* packet is the one we expect, add to tail of VC receive queue */
            mv2_ud_apprecv_window_add(&vc->app_recv_window, v);

            /* update our ack seq number to attach to outgoing packets */
            vc->seqnum_next_toack = vc->seqnum_next_torecv;

            /* increment the sequence number we expect to get next */
            vc->seqnum_next_torecv++;

            /* mark VC that we need to send an ack message */
            if (v->transport == IB_TRANSPORT_UD) {
                vc->ack_need_tosend = 1;
            }
        } else {
            PRINT_DEBUG(DEBUG_UD_verbose>1,"Got out-of-order packet recv:%d expected:%d\n",
                v->seqnum, vc->seqnum_next_torecv);

            /* in this case, the packet does not match the expected
             * sequence number, but it is within the window range,
             * add it to our (out-of-order) receive queue */
            ret = mv2_ud_recv_window_add(recvwin, v, vc->seqnum_next_torecv);
            if (ret == MSG_IN_RECVWIN) {
                /* release buffer if it is already in queue */
                MPIDI_CH3I_MRAIL_Release_vbuf(v);
            }

            /* mark VC that we need to send an ack message */
            if (v->transport == IB_TRANSPORT_UD) {
                vc->ack_need_tosend = 1;
            }
        }

        /* if we have items at front of (out-of-order) receive queue
         * whose seq num matches expected seq num, extract them from
         * out-of-order recv queue and add them to app recv queue */
        while (recvwin->head != NULL && 
               recvwin->head->seqnum == vc->seqnum_next_torecv)
        {
            PRINT_DEBUG(DEBUG_UD_verbose>1,"get one with in-order seqnum:%d \n",
                vc->seqnum_next_torecv);

            /* move item to VC apprecv queue */
            mv2_ud_apprecv_window_add(&vc->app_recv_window, recvwin->head);

            /* remove item from head of out-of-order recv queue */
            mv2_ud_recv_window_remove(recvwin);

            /* update our ack seq number to attach to outgoing packets */
            vc->seqnum_next_toack = vc->seqnum_next_torecv;

            /* increment the sequence number we expect to get next */
            vc->seqnum_next_torecv++;
        }
    } else {
        PRINT_DEBUG(DEBUG_UD_verbose>1,"Message is not in recv window seqnum:%d win start:%d win end:%d\n",
            v->seqnum, recv_win_start, recv_win_end);

        /* we got a packet that is not within the receive window,
         * just throw it away */
        MPIDI_CH3I_MRAIL_Release_vbuf(v);

        /* TODO: why? */
        /* mark VC that we need to send an ack message */
        if (v->transport == IB_TRANSPORT_UD) {
            vc->ack_need_tosend = 1;
        }
    }

    return;
}

static inline void ibv_ud_post_sr(
    vbuf* v,
    MPIDI_VC_t* vc,
    mv2_ud_ctx_t* ud_ctx)
{
    if(v->desc.sg_entry.length <= rdma_max_inline_size) {
        v->desc.u.sr.send_flags = (enum ibv_send_flags)
                (IBV_SEND_SIGNALED | IBV_SEND_INLINE);
    } else {
        v->desc.u.sr.send_flags = IBV_SEND_SIGNALED;
    }
    v->desc.u.sr.wr.ud.ah = vc->ah;
    v->desc.u.sr.wr.ud.remote_qpn = vc->qpn;
    if (ud_ctx->send_wqes_avail <= 0 ||
        ud_ctx->ext_send_queue.head != NULL)
    {
        mv2_ud_ext_sendq_queue(&ud_ctx->ext_send_queue, v);
    } else {
        ud_ctx->send_wqes_avail--;
        int ret = ibv_post_send(ud_ctx->qp, &(v->desc.u.sr), &(v->desc.y.bad_sr));
        if(ret != 0) {
            SPAWN_ERR("failed to send (ibv_post_send rc=%d %s)", ret, strerror(ret));
            exit(-1);
        }
    }

    return;
}

int post_ud_send(MPIDI_VC_t* vc, vbuf* v, int rail, mv2_ud_ctx_t *send_ud_ctx)
{
    /* check that vbuf is for UD and that data fits within UD packet */
    assert(v->transport == IB_TRANSPORT_UD);
    assert(v->desc.sg_entry.length <= MRAIL_MAX_UD_SIZE);

    /* record pointer to VC in vbuf */
    v->vc = (void *)vc;

    /* write rail id and send context into packet header */
    MPIDI_CH3I_MRAILI_Pkt_comm_header* p = v->pheader;
    p->rail  = rail;
    p->srcid = vc->writeid;

    /* if we have too many outstanding sends, or if we have other items
     * on the extended send queue, insert vbuf in extended send queue
     * to be resent later */
    message_queue_t* sendwin = &vc->send_window;
    message_queue_t* extwin  = &vc->ext_window;
    if (sendwin->count > rdma_default_ud_sendwin_size ||
       (extwin->head != NULL && extwin->head != v))
    {
        mv2_ud_ext_window_add(extwin, v);

        PRINT_DEBUG(DEBUG_UD_verbose>1,"msg(%p) queued to ext window size:%d\n",
            v, extwin->count);

        return 0;
    }

    /* otherwise, we're ok to send packet now, set send sequence number
     * in vbuf and packet header */
    v->seqnum = vc->seqnum_next_tosend;
    p->seqnum = vc->seqnum_next_tosend;
    vc->seqnum_next_tosend++;

    /* piggy-back ack in this message */
    p->acknum = vc->seqnum_next_toack;
    vc->ack_need_tosend = 0;
    vc->ack_pending = 0;

    /* mark vbuf as send-in-progress */
    v->flags |= UD_VBUF_SEND_INPROGRESS;

#if 0
    PRINT_DEBUG(DEBUG_UD_verbose>1,"UD Send : to:%d seqnum:%d acknum:%d len:%d\n", 
                vc->pg_rank, p->seqnum, p->acknum, v->desc.sg_entry.length);
#endif

    /* get pointer to UD context */
    mv2_ud_ctx_t* ud_ctx = send_ud_ctx; 
    if (ud_ctx == NULL ) {
        ud_ctx = proc.ud_ctx;
    }

    /* send packet */
    ibv_ud_post_sr(v, vc, ud_ctx);

    /* record packet and time in our send queue */
    rdma_ud_last_check = mv2_get_time_us();

    /* TODO: what's this mean? */
    /* dont' track messages in this case */
    if(v->in_sendwin) {
        return 0;
    }

    v->timestamp = mv2_get_time_us();

    /* Add vbuf to the send window */
    mv2_ud_send_window_add(&(vc->send_window), v);

    /* Add vbuf to global unack queue */
    mv2_ud_unack_queue_add(&proc.unack_queue, v);

    return 0;
}

/* when a send work element completes, issue another */
void mv2_ud_update_send_credits(int num)
{
    /* increment number of available send work queue elements */
    mv2_ud_ctx_t* ud_ctx = proc.ud_ctx;
    ud_ctx->send_wqes_avail += num;

    /* get pointer to UD context extended send queue */
    message_queue_t* q = &ud_ctx->ext_send_queue;

    /* while we have slots available in the send queue and items on
     * the UD context extended send queue, send them */
    vbuf* cur = q->head;
    while (cur != NULL && ud_ctx->send_wqes_avail > 0) {
        /* get pointer to next item */
        vbuf* next = cur->desc.next;

        /* remove item from extended send queue */
        q->head = next;
        if (q->head == NULL) {
            q->tail = NULL;
        }
        q->count--;

        /* sever item from list */
        cur->desc.next = NULL;

        /* TODO: can we reset ack to latest? */
        /* send item */
        ud_ctx->send_wqes_avail--;
        int ret = ibv_post_send(ud_ctx->qp, &(cur->desc.u.sr), &(cur->desc.y.bad_sr));
        if (ret != 0) {
            SPAWN_ERR("extend sendq send failed (ibv_post_send rc=%d %s)", ret, strerror(ret));
            exit(-1);
        }

        /* track number of sends from extended queue */
        ud_ctx->ext_sendq_count++;

        PRINT_DEBUG(DEBUG_UD_verbose>1,"sending from ext send queue seqnum: %d qlen: %d\n",
            cur->seqnum, q->count);

        /* go on to next item in queue */
        cur = next;
    }

    return;
}

/* send control message with ack update */
void mv2_send_explicit_ack(MPIDI_VC_t *vc)
{
    /* get a vbuf to build our packet */
    vbuf *v = get_ud_vbuf();

    /* ensure packet is for UD */
    assert(v->transport == IB_TRANSPORT_UD);

    /* record pointer to VC in vbuf */
    v->vc = (void *)vc;

    /* prepare vbuf for sending */
    int rail = 0;
    unsigned long size = sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header);
    vbuf_init_send(v, size, rail);

    /* get pointer to packet header */
    MPIDI_CH3I_MRAILI_Pkt_comm_header* p = v->pheader;
    memset((void*)p, 0xfc, sizeof(MPIDI_CH3I_MRAILI_Pkt_comm_header));

    /* write type, rail, and send context into packet header */
    p->type  = MPIDI_CH3_PKT_UD_ACK;
    p->rail  = rail;
    p->srcid = vc->writeid;

    /* control messages don't have a seq numer */
    v->seqnum = -1;
    p->seqnum = -1;

    /* fill in ACK info */
    p->acknum = vc->seqnum_next_toack;
    vc->ack_need_tosend = 0;
    vc->ack_pending = 0;

    /* get pointer to UD context */
    mv2_ud_ctx_t* ud_ctx = proc.ud_ctx;

    /* send packet */
    ibv_ud_post_sr(v, vc, ud_ctx);

    /* keep track of total number of control messages sent */
    vc->cntl_acks++;

#if 0
    PRINT_DEBUG(DEBUG_UD_verbose>1,"Sent explicit ACK to :%d acknum:%d\n",
        vc->pg_rank, p->acknum);
#endif

    return;
}

static void mv2_ud_resend(vbuf *v)
{
    /* if vbuf is marked as send-in-progress, don't send again,
     * unless "always retry" flag is set */
    if (v->flags & UD_VBUF_SEND_INPROGRESS && 
        ! (v->flags & UD_VBUF_RETRY_ALWAYS))
    {
        return;
    }

    /* increment our retry count */
    v->retry_count++;

    /* give up with fatal error if we exceed the retry count */
    if (v->retry_count > rdma_ud_max_retry_count) {
        PRINT_ERROR ("UD reliability error. Exeeced max retries(%d) "
                "in resending the message(%p). current retry timeout(us): %lu. "
                "This Error may happen on clusters based on the InfiniBand "
                "topology and traffic patterns. Please try with increased "
                "timeout using MV2_UD_RETRY_TIMEOUT\n", 
                v->retry_count, v, rdma_ud_retry_timeout );
        exit(EXIT_FAILURE);
    }

    /* get VC to send vbuf on */
    MPIDI_VC_t* vc = v->vc;

    /* get pointer to packet header */
    MPIDI_CH3I_MRAILI_Pkt_comm_header* p = v->pheader;

    /* piggy-back ack on message and mark VC as ack completed */
    p->acknum = vc->seqnum_next_toack;
    vc->ack_need_tosend = 0;

    /* TODO: why not set ack_pending to 0 here? */

    /* mark vbuf as send-in-progress */
    v->flags |= UD_VBUF_SEND_INPROGRESS;

    /* get pointer to UD context */
    mv2_ud_ctx_t* ud_ctx = proc.ud_ctx;

    /* send vbuf (or add to extended UD send queue if we don't have credits) */
    if (ud_ctx->send_wqes_avail > 0) {
        ud_ctx->send_wqes_avail--;
        int ret = ibv_post_send(ud_ctx->qp, &(v->desc.u.sr), &(v->desc.y.bad_sr));
        if (ret != 0) {
            SPAWN_ERR("reliability resend failed (ibv_post_send rc=%d %s)", ret, strerror(ret));
            exit(-1);
        }
    } else {
        mv2_ud_ext_sendq_queue(&ud_ctx->ext_send_queue, v);
    }

    /* increment our total resend count */
    vc->resend_count++;

    return;
}    

void MRAILI_Process_recv(vbuf *v) 
{
    /* TODO: consider sending immedate ack with each receive,
     * trades bandwidth for latency */

    /* get VC of vbuf */
    MPIDI_VC_t* vc = v->vc;

    /* get pointer to packet header */
    MPIDI_CH3I_MRAILI_Pkt_comm_header *p = v->pheader;

    /* read ack seq number from incoming packet and clear packets
     * up to and including this number from send and unack'd queues */
    mv2_ud_process_ack(vc, p->acknum);

    /* TODO: test a bit in packet type rather than one-by-one */

    /* if this is a control message, we're done (no need to send ack
     * or add packet to receive queues) */
    if ((p->type & MPIDI_CH3_PKT_CONTROL_BIT) &&
        p->type != MPIDI_CH3_PKT_UD_ACCEPT)
    {
        PRINT_DEBUG(DEBUG_UD_verbose>1,"recv cntl message ack: %d\n", p->acknum);
        MRAILI_Release_vbuf(v);
        goto fn_exit;
    }

    /* send an explicit ack if we've exceeded our pending ack count */
    if (v->transport == IB_TRANSPORT_UD) {
        /* get pointer to ud info on vc */
        vc->ack_pending++;
        if (vc->ack_pending > rdma_ud_max_ack_pending) {
            mv2_send_explicit_ack(vc);
        }
    }

    /* insert packet in receive queues (or throw it away if seq num
     * is out of current range) */
    mv2_ud_place_recvwin(v); 

fn_exit:
    return;
}

/* iterates over all items on unack queue, checks time since last send,
 * and resends if timer has expired */
void mv2_check_resend()
{
    /* get pointer to unack queue */
    message_queue_t* q = &proc.unack_queue;

    /* get current time */
    double timestamp = mv2_get_time_us();

    /* walk through unack'd list */
    vbuf* cur = q->head;
    while (cur != NULL) {
        //TODO:: if (cur->left_to_send == 0 || cur->retry_always) {

        /* get log of retry count for this packet */
        int r;
        if (cur->retry_count > 1) {
            LOG2(cur->retry_count, r);
        } else {
            r = 1;
        }

        /* compute time this packet has been waiting since we
         * last sent (or resent) it */
        long delay = timestamp - cur->timestamp;
        if ((delay > (rdma_ud_retry_timeout * r)) ||
            (delay > rdma_ud_max_retry_timeout))
        {
            PRINT_DEBUG(DEBUG_UD_verbose>1,"resend seqnum: %d retry: %d\n",
                cur->seqnum, cur->retry_count);

            /* we've waited long enough, try again and update
             * its send timestamp */
            mv2_ud_resend(cur);
            cur->timestamp = timestamp;

            /* since this may have taken some time, update our current
             * timestamp */
            timestamp = mv2_get_time_us();
        }

        /* go on to next item in list */
        cur = cur->unack_msg.next;
    }

    return;
}
