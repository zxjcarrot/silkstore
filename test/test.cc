#include <stdio.h>
#include <string.h>

int main() {
    unsigned int aux2 = 0x01020304;
    char aux[sizeof(unsigned int)]; 
    memcpy(&aux, (void*) &aux2, sizeof(aux));
    printf("%X is represented in memory as", aux2);
    for (size_t i = 0; i < sizeof(aux); i++)
        printf(" %02X", aux[i]);


    printf("\n");
    return 0;
}