//
// shows the error of calling get() twice on a future that 
// has already returned its value.
//

#include <iostream>
#include <functional>
#include <thread>
#include <future>
#include <stdexcept>
#include <cmath>

void print_result(std::future<int> &fut)
{
    std::cout << "value is ... " << fut.get() << std::endl;
}

void run()
{
    std::promise<int> prom;
    std::future<int> fut = prom.get_future();

    std::thread th1(print_result, std::ref(fut));
    std::thread th2(print_result, std::ref(fut));

    prom.set_value(5);

    th1.join();
    th2.join();
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}


