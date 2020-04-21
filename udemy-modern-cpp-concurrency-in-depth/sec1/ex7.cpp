#include <iostream>
#include <thread>
#include <chrono>
#include <stdexcept>

#include "common.h"

void foo()
{
    printf("Hello from foo\n");
}

void other_operations()
{
    std::cout << "This is other operation" << std::endl;
    throw std::runtime_error("this is a runtime error");
}

void run()
{
    std::thread foo_thread(foo);
    thread_guard tg(foo_thread);

    try {
        other_operations();
    }
    catch (...) { }

}

int main(int argc, const char **argv)
{
    run();

    return 0;
}

