
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
    std::atomic<int> x(20);

    int expected_value = 20;
    DUMP(expected_value);

    bool return_value = x.compare_exchange_weak(expected_value, 6);

    std::cout << "operation successful? " 
              << (return_value ? "yes" : "no")
              << std::endl;
    std::cout << "current expected value -  " 
              << expected_value
              << std::endl;
    std::cout << "current x -  " 
              << x.load()
              << std::endl;

    expected_value = 10;
    DUMP(expected_value);

    return_value = x.compare_exchange_weak(expected_value, 15);

    std::cout << "operation successful? " 
              << (return_value ? "yes" : "no")
              << std::endl;
    std::cout << "current expected value -  " 
              << expected_value
              << std::endl;
    std::cout << "current x -  " 
              << x.load()
              << std::endl;
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}


