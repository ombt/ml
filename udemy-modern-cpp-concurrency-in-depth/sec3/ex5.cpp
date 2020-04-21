//
// futures and asynchronous operations
//

#include <iostream>
#include <future>
#include <thread>
#include <vector>
#include <stdexcept>
#include <functional>
#include <algorithm>
#include <numeric>

const int MIN_BLOCK_SIZE = 1000;

template<typename iterator>
int parallel_accumulate(iterator begin, iterator end)
{
    int length = std::distance(begin, end);

    if (length <= MIN_BLOCK_SIZE)
    {
        std::cout << std::this_thread::get_id() << std::endl;
        return std::accumulate(begin, end, 0);
    }

    iterator mid = begin;
    std::advance(mid, (length+1)/2);

    std::future<int> f1 = std::async(std::launch::deferred |
                                     std::launch::async,
                                     parallel_accumulate<iterator>,
                                     mid, end);
    auto sum = parallel_accumulate(begin, mid);
    return sum + f1.get();
}

void run()
{
    std::vector<int> v(10000, 1);
    std::cout << "the sum is ... "
              << parallel_accumulate(v.begin(), v.end())
              << std::endl;
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}

