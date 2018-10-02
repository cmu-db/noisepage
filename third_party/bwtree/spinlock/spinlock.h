
#pragma once

#ifndef _SPINLOCK_H
#define _SPINLOCK_H

#include<atomic>
#include<cassert>

#define SPIN_LOCK_UNLOCK 0
#define SPIN_LOCK_WRITE_LOCK -1

using spinlock_t = std::atomic<int>;

void rwlock_init(spinlock_t &l);
void read_lock(spinlock_t &l);
void read_unlock(spinlock_t &l);
void write_lock(spinlock_t &l);
void write_unlock(spinlock_t &l);

#endif
