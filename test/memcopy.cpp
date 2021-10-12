//#include"stdio.h"
#include<iostream>
#include"stdio.h"
#include"stdlib.h"
 #include <string.h>
int main(int argc, char const *argv[]){
    
    char buf[100000];




    uint64_t num = 0x0102030405060708;
    printf(" %lX \n", num);

    for(int i  = 0; i < 8; i++)
        buf[i] = 'a';
    for(int i  = 0; i < 8; i++)
        std::cout << buf[i];
    std::cout << "\n";

    memcpy(buf, (void *)&num, sizeof(size_t));
    for(int i  = 0; i < 8; i++)
       printf(" %02X", buf[i]);

    size_t counter = 0;
    memcpy(&counter, buf, 8);

    printf(" %lX", counter);

    std::cout << "\n" ;

    return 0;
}
