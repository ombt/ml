#include <iostream>
#include <thread>

#include "common.h"

void func_1(int x, int y)
{
    printf("X + Y = %d\n", x+y);
}

void func_2(int &x)
{
    while ( true )
    {
        printf("Thread 1 value of X is %d\n", x);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void run()
{
    int x = 9;
    printf("Main thread value of X is %d\n", x);

    std::thread t(func_2, std::ref(x));
    thread_guard tg(t);

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    x = 15;
    printf("Main thread value of X has been changed to %d\n", x);

}

int main(int argc, const char **argv)
{
    run();

    return 0;
}



