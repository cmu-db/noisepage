
/*
 * spinlock.cpp - Simple spin lock implementation
 *
 * This piece of code is copied from the following addresses.
 *   https://github.com/boytm/rw_spinlock/blob/master/spinlock.cpp
 *   http://www.cnblogs.com/JesseFang/p/3521282.html
 * Original authored by JesseFang (github: boytm)
 */ 

#include "spinlock.h"

using std::atomic;
using std::atomic_int;
using std::atomic_store_explicit;
using std::atomic_load_explicit;
using std::atomic_compare_exchange_weak_explicit;
using std::memory_order_relaxed;
using std::memory_order_acquire;
using std::memory_order_release;

void rwlock_init(spinlock_t &l)
{
    atomic_store_explicit(&l, SPIN_LOCK_UNLOCK, memory_order_relaxed);
}

void read_lock(spinlock_t &l)
{
    int expected;
    int desired;

    while(true)
    {
        expected = atomic_load_explicit(&l, memory_order_relaxed);

        if(expected >= 0)
        {
            desired = 1 + expected;
            if(atomic_compare_exchange_weak_explicit(&l, &expected, desired, memory_order_relaxed, memory_order_relaxed))
                break; // success
        }
    }

    atomic_thread_fence(memory_order_acquire); // sync
}

void read_unlock(spinlock_t &l)
{
    int expected;
    int desired;

    while(true)
    {
        expected = atomic_load_explicit(&l, memory_order_relaxed);

        if(expected > 0)
        {
            desired = expected - 1;

            atomic_thread_fence(memory_order_release); // sync
            if(atomic_compare_exchange_weak_explicit(&l, &expected, desired, memory_order_relaxed, memory_order_relaxed))
                break; // success
        }
        else
        {
            assert(false);
        }
    }
}

void write_lock(spinlock_t &l)
{
    int expected;
    int desired;

    while(true)
    {
        expected = atomic_load_explicit(&l, memory_order_relaxed);

        if(expected == SPIN_LOCK_UNLOCK)
        {
            desired = SPIN_LOCK_WRITE_LOCK;
            if(atomic_compare_exchange_weak_explicit(&l, &expected, desired, memory_order_relaxed, memory_order_relaxed))
                break; // success
        }
    }

    atomic_thread_fence(memory_order_release); // sync
}

void write_unlock(spinlock_t &l)
{
    int expected;
    int desired;

    while(true)
    {
        expected = atomic_load_explicit(&l, memory_order_relaxed);

        if(expected == SPIN_LOCK_WRITE_LOCK)
        {
            desired = SPIN_LOCK_UNLOCK;

            atomic_thread_fence(memory_order_release); // sync
            if(atomic_compare_exchange_weak_explicit(&l, &expected, desired, memory_order_relaxed, memory_order_relaxed))
                break; // success
        }
        else
        {
            assert(false);
        }
    }
}


