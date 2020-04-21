//
//  main.cpp
//  SharedPointer
//

#include <iostream>
#include <memory>
#include "JarJar.h"

std::unique_ptr<JarJar> create_gungan()
{
    return std::make_unique<JarJar>();
}

void make_talk(std::unique_ptr<JarJar> &j)
{
    j->talk();
}

void make_talk(std::unique_ptr<JarJar> &&j)
{
    j->talk();
}

void make_talk2(std::unique_ptr<JarJar> j)
{
    j->talk();
}

void use_unique_ptr()
{
    auto jarjar = create_gungan();

    std::cout << __LINE__ << ": " << jarjar.get() << std::endl;

    make_talk(jarjar);

    std::cout << __LINE__ << ": " << jarjar.get() << std::endl;

    make_talk(std::move(jarjar));

    std::cout << __LINE__ << ": " << jarjar.get() << std::endl;

    make_talk2(std::move(jarjar));

    std::cout << __LINE__ << ": " << jarjar.get() << std::endl;
}

int main(int argc, const char * argv[])
{
    auto jarjar = std::make_unique<JarJar>();

    auto jarjar2 = std::unique_ptr<JarJar>(new JarJar);

    std::unique_ptr<JarJar> jarjar3;
    jarjar3.reset(new JarJar);

    use_unique_ptr();
  
    return 0;
}
