
#include <iostream>
#include <thread>
#include <atomic>

//
// functions which can be used with atomic<...>
//
// is_lock_free
// Store
// load
// Exchange
// Compare_exchange_weak
// Compare_exchange_strong
//

#define DUMP(X) \
    std::cout << #X << " = " << (X) << std::endl;

void run_code()
{
    int values[20];

    for (int i=0; i<20; i++)
    {
        values[i] = i+1;
    }

    std::atomic<int *> x_pointer;
    x_pointer = values;
    DUMP(*x_pointer);

    int *prev_pointer_val_1 = x_pointer.fetch_add(12);
    DUMP(*prev_pointer_val_1);
    DUMP(*x_pointer);

    int *prev_pointer_val_2 = x_pointer.fetch_sub(3);
    DUMP(*prev_pointer_val_2);
    DUMP(*x_pointer);

    DUMP(*(++x_pointer));
    DUMP(*(--x_pointer));

}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}




