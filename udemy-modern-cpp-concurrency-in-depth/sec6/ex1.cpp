
#include <iostream>
#include <thread>
#include <atomic>

#include "pch.h"

template<typename T>
class lock_free_stack
{
private:
    struct node
    {
        std::shared_ptr<T> data;
        node *next;

        node(T const &_data) : data(std::make_shared<T>(_data)) { }
    } ;

    std::atomic< node * > head;

public:

    void push(T const &_data)
    {
        node *const new_node = new node(data);
        new_node->next = head.load();
        while ( ! head.compare_exchange_weak(new_node->next, new_node)) ;
    }

    // leak memory
    std::shared_ptr<T> pop()
    { 
        node *old_head = head.load();
        while (old_head && ! head.compare_exchange_weak(old_head, old_head->next)) ;
        return (old_head ? old_head->data : std::shared_ptr<T>());
    }
};

void run_code()
{
}

int main(int argc, const char **argv)
{
    run_code();

    return 0;
}

