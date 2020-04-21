
#include <iostream>
#include <mutex>
#include <stack>
#include <thread>

#include "thread_guard.h"

template <typename T>
class thread_safe_stack
{
protected:
    std::stack<std::shared_ptr<T>> my_stack;
    std::mutex my_mutex;

public:
    thread_safe_stack() { }
    ~thread_safe_stack() { }

    thread_safe_stack(const thread_safe_stack & ) = delete;
    thread_safe_stack &operator=(const thread_safe_stack & ) = delete;

    void push(T item)
    {
        thread_guard tg(my_mutex);
        my_stack.push(std::make_shared<T>(item));
    }
    std::shared_ptr<T> pop()
    {
        thread_guard tg(my_mutex);
        if (my_stack.empty())
        {
            throw std::runtime_error("stack is empty");
        }
        std::shared_ptr<T> res(my_stack.pop());
        my_stack.pop();
        return res;
    }
    void pop(T &value)
    {
        thread_guard tg(my_mutex);
        if (my_stack.empty())
        {
            throw std::runtime_error("stack is empty");
        }
        value = *(my_stack.top().get());
        my_stack.pop();
    }
    bool empty() const 
    {
        thread_guard tg(my_mutex);
        return my_stack.empty();
    }
    size_t size() const
    {
        thread_guard tg(my_mutex);
        return my_stack.size();
    }

};

void run_code()
{
    thread_safe_stack<int> stk;
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}

#if 0

const int MIN_BLOCK_SIZE = 1000;

template<typename iterator, typename T>
T accumulate(iterator start, iterator end, T &result)
{
    result = std::accumulate(start, end, 0);
}

template<typename iterator, typename T>
T parallel_accumulate(iterator start, iterator end, T &ref)
{
    int input_size = std::distance(start, end);
    int allowed_threads_by_elements = (input_size)/MIN_BLOCK_SIZE;
    int allowed_threads_by_hardware = std::thread::hardware_concurrency();

    int num_threads = std::min(allowed_threads_by_elements,
                               allowed_threads_by_hardware);

    int block_size = (input_size + 1)/num_threads;

    std::vector<T> results(num_threads);
    std::vector<std::thread> threads(num_threads-1);

    iterator last;

    for (int i=0; i<num_threads-1; i++)
    {
        last = start;
        std::advance(last, block_size);
        threads[i] = std::thread(accumulate<iterator, T>,
                                 start, last,
                                 std::ref(results[i]));
        start = last;
    }

    results[num_threads-1] = std::accumulate(start, end, 0);

    std::for_each(threads.begin(), 
                  threads.end(),
                  std::mem_fn(&std::thread::join));

    return std::accumulate(results.begin(), results.end(), ref);
}

void run()
{
    const int size = 8000;
    int *my_array = new int[size];
    int ref = 0;

    srand(0);

    for (size_t i=0; i<size; i++)
    {
        my_array[i] = rand() % 10;
    }

    int result = parallel_accumulate<int *, int>(my_array, my_array+size, ref);
    printf("Accumulated amount = %d\n", result);
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}

#endif
