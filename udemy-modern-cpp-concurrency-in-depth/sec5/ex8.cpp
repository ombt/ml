//
// memory-ordering options
//
// memory_order_seq_cst
// memory_order_relaxed
// memory_order_acquire
// memory_order_release
// memory_order_acc_rel
// memory_order_consume
// 
// combine with atomic lead to synch options.
//

#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>

#define DUMP(X) \
    std::cout << #X << " = " << (X) << std::endl;

void run_code()
{
    std::atomic<int> x;
    x.store(5, std::memory_order_seq_cst); // default ordering

    x.store(10, std::memory_order_release);
    x.load(std::memory_order_acquire);

    int value = 11;
    bool ret_val = x.compare_exchange_weak(value, 13,
                                           std::memory_order_release,
                                           std::memory_order_relaxed);
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}





