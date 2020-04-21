#include <iostream>
#include <thread>
#include <chrono>
#include <stdexcept>

#include "common.h"

void clean()
{
    printf("clean start\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    printf("clean end\n");
}

void full_speed_ahead()
{
    printf("full_speed_ahead start\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    printf("full_speed_ahead end\n");
}

void full_stop()
{
    printf("full_stop start\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    printf("full_stop end\n");
}

void sail_ship()
{
    while ( true )
    {
        int cmd = -1;

        std::cout << "What is you demand, Captain? ";
        std::cin >> cmd;

        if (cmd == 1)
        {
            std::thread t(clean);
            t.detach();
            thread_guard tg(t);
        }
        else if (cmd == 2)
        {
            std::thread t(full_speed_ahead);
            thread_guard tg(t);
        }
        else if (cmd == 3)
        {
            std::thread t(full_stop);
            thread_guard tg(t);
        }
        else if (cmd == 100)
        {
            std::cout << "Outta here, Captain!" << std::endl;
            break;
        }
        else
        {
            std::cout << "DUH, Captain?" << std::endl;
        }
    }
}

int main(int argc, const char **argv)
{
    sail_ship();

    return 0;
}

