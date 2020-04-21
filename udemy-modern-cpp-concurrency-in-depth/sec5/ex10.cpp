
#include <iostream>
#include <atomic>
#include <memory>
#include <thread>
#include <assert.h>

//
// using std::memory_order_relaxed allows the ordering of memory
// reads and writes to occur in any order. 
//
// BAD but can increase speed since no ordering is forced !!!
//

std::atomic<bool> x,y;
std::atomic<int> z;

void write_x_then_y()
{
    x.store(true, std::memory_order_relaxed);
    y.store(true, std::memory_order_relaxed);
}

void read_y_then_x()
{
    while ( ! y.load(std::memory_order_relaxed)) ;

    if (x.load(std::memory_order_relaxed))
    {
        z++;
    }
}

void run_code()
{
    x = false;
    y = false;
    z = 0;

    std::thread thread_c(write_x_then_y);
    std::thread thread_d(read_y_then_x);

    thread_c.join();
    thread_d.join();

    assert(z != 0);
}

int
main(int argc, char **argv)
{
    run_code();
    return 0;
}

