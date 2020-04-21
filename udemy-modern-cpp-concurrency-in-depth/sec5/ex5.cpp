
#include <iostream>
#include <thread>
#include <atomic>

//
// functions which can be used with atomic<...>
//
// is_lock_free
// Store
// load
// Exchange
// Compare_exchange_weak
// Compare_exchange_strong
//

#define DUMP(X) \
    std::cout << #X << " = " << X << std::endl;

void run_code()
{
    int values[20];

    for (int i=0; i<20; i++)
    {
        values[i] = i+1;
    }

    std::atomic<int *> x_pointer;
    x_pointer = values;
    std::cout << "atomic pointer is lock-free? " 
              << (x_pointer.is_lock_free() ? " yes" : "no")
              << std::endl;

    int *y_pointer = values + 3;

    x_pointer.store(y_pointer);
    std::cout << "value referenced by pointer : " 
              << *(x_pointer.load())
              << std::endl;

    bool ret_val = x_pointer.compare_exchange_weak(y_pointer, values+10);
    std::cout << "store operation successful : "
              << (ret_val ? "yes" : "no")
              << std::endl;
    std::cout << "new value pointer by atomic pointer : "
              << *x_pointer
              << std::endl;

}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}



