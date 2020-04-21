#include <iostream>
#include <thread>
#include <vector>
#include <stdexcept>
#include <functional>
#include <numeric>

//
// T accumulate(It first, It last, T init);
// T accumulate(If first, It last, T init, BinaryOperation op);
//


void sequential_accumulate_teat()
{
    std::vector<int> v{ 1,2,3,4,5,6,7,8,9,10 };

    int sum = std::accumulate(v.begin(), v.end(), 0 );

    int product = std::accumulate(v.begin(), v.end(), 1, 
                                  std::multiplies<int>());

    auto dash_fold =[](std::string a, int b)
    {
        return std::move(a) + "-" + std::to_string(b);
    };

    std::string s = std::accumulate(std::next(v.begin()), v.end(),
                                    std::to_string(v[0]),
                                    dash_fold);

    std::cout << "Sum ... " << sum << std::endl;
    std::cout << "Product ... " << product << std::endl;
    std::cout << "String ... " << s << std::endl;
}

void run()
{
    sequential_accumulate_teat();
}

int main(int argc, const char **argv)
{
    run();

    return 0;
}

