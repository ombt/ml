
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>

#define DUMP(X) \
    std::cout << #X << " = " << (X) << std::endl;

std::atomic<bool> data_ready(false);
std::vector<int> data_vector;

//
// inter-thread happen-before relationship between reader and writer threads
//
void reader_func()
{
    while ( ! data_ready)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    std::cout << data_vector[0] << std::endl;
}

void writer_func()
{
    data_vector.push_back(3);
    data_ready.store(true);
}

void run_code()
{
    std::thread reader_thread(reader_func);
    std::thread writer_thread(writer_func);

    reader_thread.join();
    writer_thread.join();
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}





