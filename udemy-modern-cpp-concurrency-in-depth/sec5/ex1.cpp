
#include <iostream>
#include <thread>
#include <atomic>

void run_code()
{
    // this fails to compile
    // std::atomic_flag flag1 = true; 

    std::atomic_flag flag2 = ATOMIC_FLAG_INIT;

    std::cout << "1. previous flag value: "
              << flag2.test_and_set()
              << std::endl;
    std::cout << "2. previous flag value: "
              << flag2.test_and_set()
              << std::endl;

    flag2.clear();
    std::cout << "3. previous flag value: "
              << flag2.test_and_set()
              << std::endl;
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}

