//
// other useful system calls
//
// sleep()
// std::this_thread::yield()
// std::thread::hardware_concurrency()
//

#include <iostream>
#include <thread>
#include <stdexcept>

void run()
{
    std::cout << "Hardware Concurrency is ... " 
              << std::thread::hardware_concurrency()
              << std::endl;
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}






