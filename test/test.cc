
#include<iostream>
#include<memory.h>
#include<string>
using namespace std;

int main(int argc, char const *argv[]){
    string str[10];
    for(int i = 0; i < 10; i ++){
        str[i] = to_string(i);
    }
    swap(str[1], str[2]);
    // memmove(str + 1, str, sizeof(string) * (9));
    for(int i = 0; i < 10; i++){
        cout << str[i] << " ";
    }
    return 0;
}
