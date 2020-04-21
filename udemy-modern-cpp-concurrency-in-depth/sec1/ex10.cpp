#include <iostream>
#include <thread>
#include <stdexcept>

#include "common.h"

//
// this code demonstrates a race condition where thread 1 exits, but
// thread 2 is accessing by reference a variable in thread 1's stack.
// this causes an exception.

void func_2(int &x)
{
    while ( true )
    {
        try {
            std::cout << "X is ... " << x << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        catch (...)
        {
            throw std::runtime_error("this is a runtime error");
        }
    }
}

void func_1()
{
    int x = 5;

    std::thread t2(func_2, std::ref(x));

    t2.detach();

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    std::cout << "thread_1 finished execution" << std::endl;
}


void run()
{
    std::thread t(func_1);
    thread_guard tg(t);
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}




