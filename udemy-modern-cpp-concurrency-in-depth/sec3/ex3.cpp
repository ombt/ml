
//
// futures and asynchronous operations
//

#include <iostream>
#include <future>

int find_answer_how_old_universe_is()
{
    return 5000;
}

void do_other_calculations()
{
    std::cout << "do other stuff ..." << std::endl;
}

void run_code()
{
    std::future<int> the_answer_future = 
        std::async(find_answer_how_old_universe_is);
    do_other_calculations();
    std::cout << "the answer is ... " 
              << the_answer_future.get() 
              << std::endl;
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}

