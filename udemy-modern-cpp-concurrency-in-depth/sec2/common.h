#pragma once

#include <vector>
#include <thread>

class thread_guard
{
public:
    explicit thread_guard(std::thread &_t) : t(_t)
    {
        // do nothing
    }

    ~thread_guard()
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    thread_guard(const thread_guard & ) = delete;
    thread_guard &operator=(const thread_guard & ) = delete;

protected:
    std::thread &t;
} ;

