
#include <iostream>
#include <thread>

void test()
{
    printf("Hello from test\n");
}

int main(int argc, const char **argv)
{
    std::thread thread1(test);

    if (thread1.joinable())
    {
        printf("Thread 1 is joinable before ...\n");
    }
    else
    {
        printf("Thread 1 is NOT joinable before ...\n");
    }

    thread1.join();

    if (thread1.joinable())
    {
        printf("Thread 1 is joinable after ...\n");
    }
    else
    {
        printf("Thread 1 is NOT joinable after ...\n");
    }

    return 0;
}


