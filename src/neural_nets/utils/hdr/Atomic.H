#ifndef __OMBT_ATOMIC_H
#define __OMBT_ATOMIC_H

// atomic operation on an integer

template <class CounterType = unsigned long>
class Atomic
{
public:
    // ctor and dtor
    Atomic(CounterType val): atom_(val) { }
    ~Atomic() { };

    // operations
#if 0
    inline CounterType increment() {
        return(++atom_);
    }
    inline CounterType decrement() {
        return(--atom_);
    }
    inline bool increment_and_test(CounterType testval) {
        return((++atom_) == testval);
    }
    inline bool decrement_and_test(CounterType testval) {
        return((--atom_) == testval);
    }
    inline bool test_and_set(CounterType oldval, CounterType newval) {
        oldval = (oldval != 0) ? 1 : 0;
        newval = (newval != 0) ? 1 : 0;
        if (atom_ == oldval)
        {
            atom_ = newval;
            return true;
        }
        else
        {
            return false;
        }
    }
#else
    inline CounterType increment() {
        return(__sync_add_and_fetch(&atom_, (CounterType)1));
    }
    inline CounterType decrement() {
        return(__sync_sub_and_fetch(&atom_, (CounterType)1));
    }
    inline bool increment_and_test(CounterType testval) {
        return(__sync_add_and_fetch(&atom_, (CounterType)1) == testval);
    }
    inline bool decrement_and_test(CounterType testval) {
        return(__sync_sub_and_fetch(&atom_, (CounterType)1) == testval);
    }
    inline bool test_and_set(CounterType oldval, CounterType newval) {
        oldval = (oldval != 0) ? 1 : 0;
        newval = (newval != 0) ? 1 : 0;
        return(__sync_bool_compare_and_swap(&atom_, oldval, newval));
    }
#endif

private:
    // leper colony
    Atomic(const Atomic &src);
    Atomic &operator=(const Atomic &rhs);

private:
    volatile CounterType atom_;
};

#endif
