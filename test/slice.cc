#include "leveldb/slice.h"

#include<iostream>
#include<map>
using namespace leveldb;


int main(int argc, char const *argv[]){
    std::map<Slice,int> m;
    Slice a[5];
    a[1] = Slice("a1");
    a[2] = Slice("a2");
    a[3] = Slice("a3");
  

    if (a[1] > a[2]){
        a[1] = a[2];
    }  
     std::cout << a[1].ToString() << " \n";

    if (a[1] < a[2]){
        a[1] = a[2];
    }  
     std::cout << a[1].ToString() << " \n";

  Slice tmp = a[1] ;
    a[1] = a[2];
    a[2] = a[3];
    a[3] = tmp;  


    std::cout << a[1].ToString()<< " " << a[2].ToString() << " " << a[3].ToString() << "\n";
    /* Slice maxS = std::max(str, tmp);

    //if (str < tmp){
    str = tmp;
    //}
    std::cout << str.ToString()<< " " << tmp.ToString() << "\n";

    std::cout << maxS.ToString() << "\n";
    for(int i = 0; i < 100; i++){
        m[Slice(std::to_string(i))] = i;
    }

    std::cout<< "m's size: " << m.size() << "\n";

  

    for(auto it : m){
        std::cout << it.first.ToString() << " " << it.second << "\n";
    }
    str = std::move(tmp); */

    return 0;   
}
