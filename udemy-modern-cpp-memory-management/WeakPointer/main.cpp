
#include <iostream>
#include <memory>
#include <string>

class Student
{
protected:
    std::shared_ptr<Student> _best_friend;
    std::weak_ptr<Student> _weak_best_friend;
    std::string _name;

public:
    Student(std::string name) : _name(name)
    {
        std::cout << "CTOR ... " << _name << std::endl;
    }

    ~Student()
    {
        std::cout << "DTOR ... " << _name << std::endl;
    }

    void set_best_friend(std::shared_ptr<Student> best_friend)
    {
        _best_friend = best_friend;
    }

    void set_weak_best_friend(std::shared_ptr<Student> best_friend)
    {
        _weak_best_friend = best_friend;
    }

    void best_friend()
    {
        // how to access weak pointer
        auto best_friend = _weak_best_friend.lock();
        if (best_friend)
        {
            std::cout << _name 
                      << " best friends with " 
                      << _weak_best_friend.lock()->_name
                      << std::endl;
        }
    }
} ;

void run()
{
    auto alice = std::make_shared<Student>("Alice");
    auto bob = std::make_shared<Student>("Bob");

    // create a cycle in reference-counted pointer !!!!
    alice->set_best_friend(bob);
    bob->set_best_friend(alice);

    auto chrissy = std::make_shared<Student>("Chrissy");
    auto stanley = std::make_shared<Student>("Stanley");

    // no cycle !!!
    chrissy->set_weak_best_friend(stanley);
    stanley->set_weak_best_friend(chrissy);

    chrissy->best_friend();
    stanley->best_friend();
}

int main(int argc, char **argv)
{
    run();
    return 0;
}

