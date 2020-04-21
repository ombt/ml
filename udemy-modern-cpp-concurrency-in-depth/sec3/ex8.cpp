
#include <iostream>
#include <functional>
#include <thread>
#include <future>
#include <stdexcept>
#include <cmath>

void throw_exception()
{
    throw std::runtime_error(std::string("Square root X < 0!"));
}

void calculate_square_root(std::promise<int> &prom)
{
    int x = 1;
    std::cout << "please enter an integer value: ";
    try {
        std::cin >> x;
        if (x < 0)
        {
            throw_exception();
        }
        prom.set_value(std::sqrt(x));
    } catch (std::exception & ) {
        prom.set_exception(std::current_exception());
    }
}

void print_result(std::future<int> &fut)
{
    try {
        int x = fut.get();
        std::cout << "value is ... " << x << std::endl;
    } catch ( std::exception &e) {
        std::cout << "[exception caught: " 
                  << e.what() 
                  << "]" << std::endl;
    }
}

void run()
{
    std::promise<int> prom;
    std::future<int> fut = prom.get_future();

    std::thread printing_thread(print_result, std::ref(fut));
    std::thread calculation_thread(calculate_square_root, std::ref(prom));

    printing_thread.join();
    calculation_thread.join();
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}


