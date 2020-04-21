
#include <iostream>
#include <mutex>
#include <thread>
#include <string>
#include <chrono>
#include <condition_variable>

bool have_i_arrived = false;
int total_distance = 10;
int distance_covered = 0;

std::condition_variable cv;
std::mutex m;

bool keep_moving()
{
    while ( true )
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        distance_covered += 1;
        if (distance_covered == total_distance)
        {
            cv.notify_one();
        }
    }
}

void ask_driver_to_wake_u_up_at_right_time()
{
    std::unique_lock<std::mutex> ul(m);
    cv.wait(ul, [] { return distance_covered == total_distance; });
    std::cout << "I'm there, distance covered = " 
              << distance_covered << std::endl;
}

void run_code()
{
    std::thread driver_thread(keep_moving);
    std::thread passenger_thread(ask_driver_to_wake_u_up_at_right_time);

    passenger_thread.join();
    driver_thread.join();
}

int
main(int argc, char **argv)
{
    run_code();
    return 0;
}
