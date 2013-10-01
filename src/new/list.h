#ifndef __MYLIST_H__
#define __MYLIST_H__

struct list_head {
    struct list_head *next, *prev;
};

static inline void SPAWN_INIT_LIST_HEAD(struct list_head *list)
{
    list->next = list;
    list->prev = list;
}

#define spawn_offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)

#define spawn_container_of(ptr, type, member) ({        \
     const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
     (type *)( (char *)__mptr - spawn_offsetof(type,member) );})

#define spawn_list_entry(ptr, type, member) \
    spawn_container_of(ptr, type, member)

#define spawn_list_for_each(pos, head) \
    for (pos = (head)->next; pos != (head); pos = pos->next)

#define spawn_list_for_each_safe(pos, n, head) \
     for (pos = (head)->next, n = pos->next; pos != (head); \
           pos = n, n = pos->next)

#define spawn_list_for_each_entry(pos, head, member)        \
      for (pos = spawn_list_entry((head)->next, typeof(*pos), member);      \
        &pos->member != (head);        \
        pos = spawn_list_entry(pos->member.next, typeof(*pos), member))

#define spawn_list_for_each_entry_safe(pos, n, head, member)                  \
  for (pos = spawn_list_entry((head)->next, typeof(*pos), member),      \
          n = spawn_list_entry(pos->member.next, typeof(*pos), member); \
          &pos->member != (head);                                    \
          pos = n, n = spawn_list_entry(n->member.next, typeof(*n), member))

static inline void spawn_imp_list_add(struct list_head *new, struct list_head *prev, struct list_head *next)
{
    next->prev = new;
    new->next = next;
    new->prev = prev;
    prev->next = new;
}

static inline void spawn_list_add(struct list_head *new, struct list_head *head)
{
    spawn_imp_list_add(new, head, head->next);
}

static inline void spawn_list_add_tail(struct list_head *new, struct list_head *head)
{
    spawn_imp_list_add(new, head->prev, head);
}

static inline void spawn_imp_list_del(struct list_head *prev, struct list_head *next)
{
    next->prev = prev;
    prev->next = next;
}

static inline void spawn_list_del(struct list_head *entry)
{
    spawn_imp_list_del(entry->prev, entry->next);
}

static inline int spawn_list_empty(const struct list_head *head)
{
    return head->next == head;
}

#endif
