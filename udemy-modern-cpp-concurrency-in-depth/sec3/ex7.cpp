
#include <iostream>
#include <functional>
#include <thread>
#include <future>
#include <stdexcept>

void print_int(std::future<int> &fut)
{
    std::cout << "waiting for value from print thread ... " << std::endl;
    std::cout << "value is ... " << fut.get() << std::endl;
}

void run()
{
    std::promise<int> prom;
    std::future<int> fut = prom.get_future();

    std::thread print_thread(print_int, std::ref(fut));

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    std::cout << "setting the value in main thread ... " << std::endl;

    //
    // careful in the order you do the following two calls. if 
    // you reverse them, this you get a deadlock.
    //
    prom.set_value(10);

    print_thread.join();
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}


