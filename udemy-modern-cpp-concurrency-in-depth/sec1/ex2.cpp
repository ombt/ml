
#include <iostream>
#include <thread>

void test()
{
    printf("Hello from test\n");
}

int main(int argc, const char **argv)
{
    std::thread thread1(test);

    return 0;
}


