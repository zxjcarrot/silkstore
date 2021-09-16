
#include<iostream>
#include<memory.h>
#include<string>
#include<vector>
#include <deque>
using namespace std;



int main(int argc, char const *argv[]){

    deque<int> de;
    de.push_back(1);
    de.push_back(2);
    de.push_back(3);

    for(int i = 0; i < de.size(); i++){
        cout << de[i] << " ";
    }
    cout << endl;
    int size = de.size();
   /*  for(int i = 1; i < size; i++){
        de.erase(de.begin() + i);
        // cout << de[i] << " ";
    } */
    de.erase(de.begin() + 1);
    de.erase(de.begin() + 4);

    size = de.size();
    for(int i = 0; i < size; i++){
    //    de.erase(de.begin() + 1);
        cout << de[i] << " ";
    }
    cout << endl;

    return 0;
}
