#include <iostream>
#include <thread>
#include <stdexcept>

void foo()
{
    std::cout << "This is thread ... " 
              << std::this_thread::get_id()
              << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

void run()
{
    std::thread t1(foo);
    std::thread t2(foo);
    std::thread t3(foo);
    std::thread t4;

    std::cout << "Thread 1 ... " << t1.get_id() << std::endl;
    std::cout << "Thread 2 ... " << t2.get_id() << std::endl;
    std::cout << "Thread 3 ... " << t3.get_id() << std::endl;
    std::cout << "Thread 4 ... " << t4.get_id() << std::endl;

    t1.join();
    t2.join();
    t3.join();

    std::cout << "after join Thread 1 ... " << t1.get_id() << std::endl;
    std::cout << "after join Thread 2 ... " << t2.get_id() << std::endl;
    std::cout << "after join Thread 3 ... " << t3.get_id() << std::endl;
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}






