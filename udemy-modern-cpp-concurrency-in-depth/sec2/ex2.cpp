
#include <iostream>
#include <mutex>
#include <list>
#include <thread>

class list_wrapper
{
protected:
    std::list<int> my_list;
    std::mutex m;

public:
    void add_to_list(int const &x)
    {
        std::lock_guard<std::mutex> lg(m);
        my_list.push_front(x);
    }

    void size()
    {
        std::lock_guard<std::mutex> lg(m);
        int size = my_list.size();
        std::cout << "size of list is ... " << size << std::endl;
    }

    // deliberately causes problems for demo case by ignoring thread-safety.
    std::list<int> *get_data()
    {
        return &my_list;
    }
};

class data_object
{
};

class data_wrapper
{
protected:
    data_object protected_data;
    std::mutex m;

public:
    template <typename function>
    void do_some_work(function f)
    {
        std::lock_guard<std::mutex> lg(m);
        f(protected_data);
    }

};

data_object *unprotected_data;

void maclicious_function(data_object &data)
{
    unprotected_data = &data;
}

void run_code()
{
    data_wrapper wrapper;
    wrapper.do_some_work(malicious_function);
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
