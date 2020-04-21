
#include <iostream>
#include <mutex>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <queue>

template <typename T>
class sequential_queue
{
protected:
    struct node
    {
        T data;
        std::unique_ptr<node> next;

        node(T _data) : data(std::move(_data)
        {
        }
    };

    std::unique_ptr<node> head;
    node *tail;
    std::mutex head_mutex;
    std::mutex tail_mutex;

    std::condition_variable cv;

    node *get_tail()
    {
        std::lock_guard<std::mutex> lg(tail_mutex);
        return tail;
    }

    std::unique_ptr<node> wait_pop_head()
    {
    }

public:
    sequential_queue() : head(new node), tail(head.get()) { }

    void push(T value)
    {
        std::shared_ptr<node> new_data(std::make_shared<T>(std::move(value)));
        std::unique_ptr<node> p(new node);
        node * const new_tail = p.get();
        {
            std::lock_guard<std::mutex> lgt(tail_mutex);
            tail->data = new_data;
            tail->next = std::move(p);
            tail = new_tail;
        }
        cv.notify_one();
    }

    std::shared_ptr<T> pop()
    {
        std::lock_guard<std::mutex> lgh(head_mutex);
        if (head.get() == get_tail())
        {
            return std::shared_ptr<T>();
        }

        std::shared_ptr<T> const res(head->data);
        std::unique_ptr<node> const old_head= std::move(head);
        head = std::move(old_head->next);

        return res;
    }

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

