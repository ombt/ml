
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
    std::atomic<bool> x(false);
    DUMP(x.is_lock_free());

    std::atomic<bool> y(true);
    x.store(false);
    x.store(y);

    DUMP(y.load());

    bool z = x.exchange(false);
    DUMP(x.load());
    DUMP(z);
    
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}


