
#include <iostream>
#include <mutex>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <queue>

template <typename T>
class thread_safe_queue
{
protected:
    std::mutex my_mutex;
    std::condition_variable my_cv;
    std::queue<std::shared_ptr<T>> my_queue;

public:
    thread_safe_queue() { };
    ~thread_safe_queue() { };

    thread_safe_queue(const thread_safe_queue &src) = delete;
    thread_safe_queue &operator=(const thread_safe_queue &rhs) = delete;

    void push(T item)
    {
        std::lock_guard<std::mutex> lg(my_mutex);
        my_queue.push(std::make_shared<T>(item));
        my_cv.notify_one();
    }

    std::shared_ptr<T> pop()
    {
        std::lock_guard<std::mutex> lg(my_mutex);
        if (my_queue.empty())
        {
            return std::shared_ptr<T>();
        }
        else
        {
            std::shared_ptr<T> ref(my_queue.front());
            my_queue.pop();
            return ref;
        }
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lg(my_mutex);
        return my_queue.empty();
    }

    std::shared_ptr<T> wait_pop()
    {
        std::unique_lock<std::mutex> lg(my_mutex);
        my_cv.wait(lg, [this] {
            return ! my_queue.empty();
        });
        std::shared_ptr<T> ref = my_queue.front();
        my_queue.pop();
        return ref;
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> lg(my_mutex);
        return my_queue.size();
    }
};

void run_code()
{
    thread_safe_queue<int> stk;
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}

