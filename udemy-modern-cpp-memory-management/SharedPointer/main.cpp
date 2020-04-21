//
//  main.cpp
//  SharedPointer
//

#include <iostream>
#include <memory>
#include "JarJar.h"

std::shared_ptr<JarJar> create()
{
    return std::make_shared<JarJar>();
}

void use_shared_ptr(std::shared_ptr<JarJar>& jarjar)
{
    std::cout << jarjar.get() << std::endl;
    
    jarjar->talk();
    
    jarjar.reset();
    
    std::cout << jarjar.get() << std::endl;
}

int main(int argc, const char * argv[])
{
    auto jarjar = create();
   
    use_shared_ptr(jarjar);
    
    std::cout << jarjar.get() << std::endl;
  
    return 0;
}
