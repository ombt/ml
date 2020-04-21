#include <iostream>
#include <thread>

#include "common.h"

void func_1(int x, int y)
{
    printf("X + Y = %d\n", x+y);
}

void run()
{
    std::thread t(func_1, 1, 2);
    thread_guard tg(t);
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}


