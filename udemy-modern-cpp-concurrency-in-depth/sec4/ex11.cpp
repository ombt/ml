//
// shows the error of calling get() twice on a future that 
// has already returned its value. 
//
// show how to fix it.
//

#include <iostream>
#include <functional>
#include <thread>
#include <future>
#include <stdexcept>
#include <cmath>
#include <chrono>

void print_result(std::shared_future<int> &fut, int seconds)
{
    if (fut.valid())
    {
        std::cout << "valid future. value is ... " << fut.get() << std::endl;
    }
    else
    {
        std::cout << "invalid future." << std::endl;
    }
}

void run()
{
    std::promise<int> prom;
    std::shared_future<int> fut(prom.get_future());

    std::thread th1(print_result, std::ref(fut), 0);
    std::thread th2(print_result, std::ref(fut), 3);

    prom.set_value(5);

    th1.join();
    th2.join();
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}


