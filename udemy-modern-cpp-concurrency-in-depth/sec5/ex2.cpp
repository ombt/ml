
#include <iostream>
#include <thread>
#include <atomic>

void run_code()
{
    std::atomic<bool> flag_1;
    std::cout << "flag 1 is ... " << flag_1 << std::endl;

    // not allowed
    // std::atomic<bool> flag_2(flag_1);

    // not allowed
    // std::atomic<bool> flag_3;
    // flag_3 = flag_1;

    bool non_atomic_bool = true;
    std::atomic<bool> flag_4(non_atomic_bool);

    std::atomic<bool> flag_5;
    flag_5 = non_atomic_bool;
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}


