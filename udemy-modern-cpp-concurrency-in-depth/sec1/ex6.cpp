#include <iostream>
#include <thread>
#include <chrono>
#include <stdexcept>

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

    try {
        other_operations();
        foo_thread.join();
    }
    catch (...) {
        foo_thread.join();
    }

}

int main(int argc, const char **argv)
{
    run();

    return 0;
}

