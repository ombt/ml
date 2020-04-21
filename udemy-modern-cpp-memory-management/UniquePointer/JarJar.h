//
//  JarJar.h
//  Copyright © 2016 Mattias Johansson. All rights reserved.
//

#ifndef JarJar_h
#define JarJar_h

#include <iostream>

class JarJar
{
public:
    // Constructor
    JarJar()
    {
        std::cout << "Constructing JarJar at " << this << std::endl;
    }
    
    // Destructor
    ~JarJar()
    {
        std::cout << "Deleting JarJar at " << this << std::endl;
    }
    
    // Copy constructor
    JarJar(const JarJar& old)
    {
        std::cout << "Copy constructing JarJar " << std::endl;
    }
    
    // Copy assignment operator
    JarJar& operator=(const JarJar& old)
    {
        std::cout << "Copy assignment of JarJar " << std::endl;
        return *this;
    }
    
    // Move constructor
    JarJar(JarJar&& old)
    {
        std::cout << "Move constructing JarJar " << std::endl;
    }
    
    // Move assignment operator
    JarJar& operator=(JarJar&& old)
    {
        std::cout << "Move assignment of JarJar " << std::endl;
        return *this;
    }
    
    void talk()
    {
        std::cout << "Misa talka" << std::endl;
    }
};


#endif /* JarJar_h */
