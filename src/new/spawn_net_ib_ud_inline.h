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
#include <spawn_net_ib_internal.h>
#include <spawn_net_ib_vbuf.h>

enum {
    MSG_QUEUED_RECVWIN,
    MSG_IN_RECVWIN
};

static inline void mv2_ud_ext_sendq_queue(message_queue_t *q, vbuf *v)
{
    v->desc.next = NULL;
    if (q->head == NULL) {
        q->head = v;
    } else {
        q->tail->desc.next = v;
    }
    q->tail = v;
    q->count++;
    PRINT_DEBUG(DEBUG_UD_verbose>1,"queued to ext send queue, queue len:%d seqnum:%d\n", q->count, v->seqnum);
}

/* adds vbuf to extended send queue, which tracks messages we will be
 * sending but haven't yet */
static inline void mv2_ud_ext_window_add(message_queue_t *q, vbuf *v)
{
    v->extwin_msg.next = v->extwin_msg.prev = NULL;
    if (q->head == NULL) {
        q->head = v;
    } else {
        (q->tail)->extwin_msg.next = v;
    }
    q->tail = v;
    q->count++;
}

/* adds vbuf to the send queue, which tracks packets we've actually
 * sent but not yet gotten an ack for */
static inline void mv2_ud_send_window_add(message_queue_t *q, vbuf *v)
{
    v->sendwin_msg.next = v->sendwin_msg.prev = NULL;
    v->in_sendwin = 1;

    if(q->head == NULL) {
        q->head = v;
    } else {
        (q->tail)->sendwin_msg.next = v;
    }

    q->tail = v;
    q->count++;
}

static inline void mv2_ud_send_window_remove(message_queue_t *q, vbuf *v)
{
    assert (q->head == v);
    v->in_sendwin = 0;
    q->head = v->sendwin_msg.next;
    q->count--;
    if (q->head == NULL ) {
        q->tail = NULL;
        assert(q->count == 0);
    }

    v->sendwin_msg.next = NULL;
}    

static inline void mv2_ud_unack_queue_add(message_queue_t *q, vbuf *v)
{
    v->unack_msg.next = NULL;

    if (q->head == NULL) {
        q->head = v;
        v->unack_msg.prev = NULL;
    } else {
        (q->tail)->unack_msg.next = v;
        v->unack_msg.prev = q->tail;
    }

    q->tail = v;
    q->count++;
}

static inline void mv2_ud_unack_queue_remove(message_queue_t *q, vbuf *v)
{
    vbuf *next = v->unack_msg.next;
    vbuf *prev = v->unack_msg.prev;

    if (prev == NULL) {
        q->head = next;
    } else {
        prev->unack_msg.next = next;
    }

    if (next == NULL) {
        q->tail = prev;
    } else {
        next->unack_msg.prev = prev;
    }
    v->unack_msg.next = v->unack_msg.prev = NULL;
    q->count--;
}

static inline int mv2_ud_recv_window_add(message_queue_t *q, vbuf *v, int recv_win_start)
{
    PRINT_DEBUG(DEBUG_UD_verbose>1,"recv window add recv_win_start:%d rece'd seqnum:%d\n",
        recv_win_start, v->seqnum);

    /* clear next and previous pointers in vbuf */
    v->recvwin_msg.next = v->recvwin_msg.prev = NULL;

    /* insert vbuf into recv queue in order by its sequence number */
    if(q->head == NULL) {
        /* trivial insert if list is empty */
        q->head = q->tail = v;
    } else {
        /* otherwise, we have at least one item already in list,
         * get a pointer to current head */ 
        vbuf* cur_buf = q->head;

        /* if our sequence number is greater than start of window */
        if (v->seqnum > recv_win_start) {
            /* current seq num is higher than start seq number, */
            /* iterate until we find the first item in the list
             * whose sequence number is greater or equal to vbuf,
             * or until we hit first item whose seq wraps (less
             * than or equal to start seq num) */
            if (cur_buf->seqnum < recv_win_start) {
                /* first item already wraps */
            } else {
                /* otherwise, search */
                while (cur_buf != NULL &&
                       cur_buf->seqnum < v->seqnum &&
                       cur_buf->seqnum > recv_win_start)
                {
                    cur_buf = cur_buf->recvwin_msg.next;
                }
            }
        } else {
            /* vbuf seq num is less than or equal to start seq num,
             * iterate until we find the first item in the list
             * whose sequence number is greater or equal to vbuf,
             * or until we hit first item whose seq wraps (less */
            if (cur_buf->seqnum > recv_win_start) {
                /* first item in list is greater than start, iterate
                 * until we find an item that wraps and then keep
                 * going until we find one that is equal or greater
                 * than vbuf */
                while (cur_buf != NULL &&
                       ((cur_buf->seqnum >= recv_win_start) ||
                        (cur_buf->seqnum  < v->seqnum)))
                { 
                    cur_buf = cur_buf->recvwin_msg.next;
                }
            } else {
                /* first item already wraps, just iterate until
                 * we find an item equal or greater than vbuf */
                while (cur_buf != NULL &&
                       cur_buf->seqnum < v->seqnum)
                {
                    cur_buf = cur_buf->recvwin_msg.next;
                }
            }
        }

        /* check whether we found an item with a sequence number equal
         * to or after vbuf seq number */
        if (cur_buf != NULL) {
            /* check whether item in list matches seq number of vbuf */
            if (cur_buf->seqnum == v->seqnum) {
                /* we found a matching item already in the queue */
                return MSG_IN_RECVWIN;
            }

            /* otherwise current item is larger, so insert vbuf
             * just before it */
            vbuf* prev_buf = cur_buf->recvwin_msg.prev;
            v->recvwin_msg.prev = prev_buf;
            v->recvwin_msg.next = cur_buf;

            /* update list pointers */
            if (cur_buf == q->head) {
                /* item is at front of list, so update head to
                 * point to vbuf */
                q->head = v;
            } else {
                /* otherwise item is somewhere in the middle,
                 * so update next pointer of previous item */
                prev_buf->recvwin_msg.next = v;
            }
            cur_buf->recvwin_msg.prev = v;
        } else {
            /* all items in queue come before vbuf, so tack vbuf on end */
            v->recvwin_msg.next = NULL;
            v->recvwin_msg.prev = q->tail;
            q->tail->recvwin_msg.next = v;
            q->tail = v;
        }

        /* increment size of queue */
        q->count++;
    }

    /* return code to indicate we inserted vbuf in queue */
    return MSG_QUEUED_RECVWIN; 
}

/* remove item from head of recv queue */
static inline void mv2_ud_recv_window_remove(message_queue_t *q)
{
    vbuf *next = (q->head)->recvwin_msg.next;
    q->head = next;
    if (next != NULL) {
        next->recvwin_msg.prev = NULL;
    } else {
        q->head = q->tail = NULL;
    }
    q->count--;
}

