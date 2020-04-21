#include <iostream>
#include <thread>
#include <stdexcept>

#include "common.h"

//
// moving threads
//

void foo()
{
    std::cout << "foo ... snoring ..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

void bar()
{
    std::cout << "bar ... snoring ..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

void run()
{
    std::thread t1(foo);

    std::thread t2 = std::move(t1);

    t1 = std::thread(bar);
    std::thread t3(foo);

    t1 = std::move(t3);

    t1.join();
    t2.join();
    t3.join();
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}





