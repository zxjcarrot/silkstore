#include "map.h"
using namespace std;

int main(){
    btree::map<int,uint64_t> tree;

    //m.insert(1)
   for(int i = 0; i < 100; i++){
        //std::string key = std::to_string(i);
        int key = i;
        uint64_t addres = i;
        tree[key] =  addres;
        std::cout<<  " insert key : " << key << " value: " << addres <<" \n";
    }
    for(int i = 0; i < 100; i++){
        //std::string key = std::to_string(i);
        int key = i;        
        uint64_t addres = tree[key];
        bool suc = tree.count(key);
        std::cout<<  " key : " << key << " value: " << addres <<" \n";
    }

}