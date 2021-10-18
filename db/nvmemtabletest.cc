#include "db/nvmemtable.h"
#include"port/atomic_pointer.h"
#include "db/dbformat.h"

#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "nvm/nvmmanager.h"
#include <cmath>  
#include <iostream>

//namespace leveldb {


void addget(){
        leveldb::DynamicFilter *dynamic_filter =  leveldb::NewDynamicFilterBloom(1000, 0.1);
        leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
        size_t gb = GB;
        size_t size = 40 * gb;
        std::cout << "size " << size << "\n";
        leveldb::silkstore::NvmManager *manager 
                  = new leveldb::silkstore::NvmManager("/mnt/myPMem/pmem.002", size);
        leveldb::silkstore::Nvmem *nvm = manager->allocate(100*MB);
        //nvm->init(4000);
        leveldb::NvmemTable *table = new leveldb::NvmemTable(cmp , dynamic_filter, nvm); // = new  silkstore::NvmemTable();

        //for(int i =)
        leveldb::SequenceNumber seq = 1;
        leveldb::ValueType type = leveldb::kTypeValue ;

        int N = 10;
        for(int i = 0; i < N; i++){
            std::string k = std::to_string(i);
           leveldb::Slice key(k);
           std::string v = std::to_string(i+200) + "12asda3" ;
           leveldb::Slice value(v);
           table->Add(seq, type, key, value);
        }

        for(int i = 0; i < N; i++){
           leveldb::LookupKey lookupkey(leveldb::Slice(std::to_string(i)), seq);
         //  leveldb::Slice key();
          // leveldb::Slice value(std::to_string(i*i));
          std::string res;
          leveldb::Status status;
          bool suc = table->Get(lookupkey, &res, &status);
          if (suc){
                std::cout<< " find " << i << "'s res : "<< res << " \n";
          }else{
                std::cout << "can't find key " << status.ToString() << "\n";
          }        
        }
        delete table;
}


void IteratorTest(){


        leveldb::DynamicFilter *dynamic_filter =  leveldb::NewDynamicFilterBloom(1000, 0.1);
        leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
        
        size_t gb = GB;
        size_t size = 10 * gb;
        std::cout << "size " << size << "\n";
        leveldb::silkstore::NvmManager *manager 
                  = new leveldb::silkstore::NvmManager("/mnt/myPMem/pmem.002", size);

        leveldb::silkstore::Nvmem *nvm = manager->allocate(4131457280);

       // nvm->init(4000);
        leveldb::NvmemTable *table = new leveldb::NvmemTable(cmp , dynamic_filter, nvm); // = new  silkstore::NvmemTable();

        leveldb::SequenceNumber seq = 1;
        leveldb::ValueType type = leveldb::kTypeValue ;
        leveldb::ValueType Deletetype = leveldb::kTypeDeletion ;


        for(int i = 0; i < 10000; i++){
           leveldb::Slice key(std::to_string(i));
           leveldb::Slice value(std::to_string(random()));
           table->Add(seq++, type, key, value);

            std::string res;
            leveldb::Status status;
            leveldb::LookupKey lookupkey(leveldb::Slice(std::to_string(i)), 2);
            table->Get(lookupkey, &res, &status);
        }


        leveldb::Iterator* it = table->NewIterator();
        
        it->SeekToFirst();
        
        int count = 0;
        while(it->Valid()){
               it->key();
                it->value();
            std::cout<<"key: " << it->key().ToString() << " value: " << it->value().ToString() << std::endl;
           /*  if (i++ > 10){
                  break;
            } */
             count++;
            it->Next();
        } 
        std::cout<<count <<"\n"; 

        delete table; 
}



void deleteUpdate(){

        leveldb::DynamicFilter *dynamic_filter =  leveldb::NewDynamicFilterBloom(1000, 0.1);
        leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());

        size_t gb = GB;
        size_t size = 40 * gb;
        std::cout << "size " << size << "\n";
        leveldb::silkstore::NvmManager *manager 
                  = new leveldb::silkstore::NvmManager("/mnt/myPMem/pmem.002", size);

        leveldb::silkstore::Nvmem *nvm = manager->allocate(100*MB);

        leveldb::NvmemTable *table = new leveldb::NvmemTable(cmp , dynamic_filter, nvm); // = new  silkstore::NvmemTable();

        leveldb::SequenceNumber seq = 1;
        leveldb::ValueType type = leveldb::kTypeValue ;
        leveldb::ValueType Deletetype = leveldb::kTypeDeletion ;
       
       for(int i = 0; i < 10; i++){
           leveldb::Slice key(std::to_string(i));
           leveldb::Slice value(std::to_string(i+2));
           table->Add(1, type, key, value);
        }
            
      


      std::cout<< " #####  Update  #####  \n";

      for(int j = 0; j < 5; j++){
            std::string s = std::to_string(j);
           leveldb::Slice key1(s);
           std::string str = std::to_string(666) + " new value ";
           leveldb::Slice value1(str); 
           std::cout<< " key " << key1.ToString() << " value " << value1.ToString() << std::endl;
           table->Add(1, type, key1, value1);
        } 
        for(int i = 5; i < 8; i++){
           leveldb::Slice key(std::to_string(i));
           leveldb::Slice value(std::to_string(i+2));
           table->Add(2, type, key, value);
        } 

      //   find 

        
        for(int i = 0; i < 8; i++){
           leveldb::LookupKey lookupkey(leveldb::Slice(std::to_string(i)), 2);
         //  leveldb::Slice key();
          // leveldb::Slice value(std::to_string(i*i));
          std::string res;
          leveldb::Status status;
          bool suc = table->Get(lookupkey, &res, &status);
          if (suc){
                std::cout<< " find " << i << "'s res : "<< res << status.ToString() << " \n";
          }else{
                std::cout << "can't find key " << status.ToString();
          }        
        }
}

void copyNvmem(){

      leveldb::DynamicFilter *dynamic_filter =  leveldb::NewDynamicFilterBloom(1000, 0.1);
      leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
      
      size_t gb = GB;
      size_t size = 10 * gb;
      std::cout << "size " << size << "\n";
      leveldb::silkstore::NvmManager *manager 
            = new leveldb::silkstore::NvmManager("/mnt/myPMem/pmem.002", size);

      size_t asize = 4000;
      asize *=  MB;
      std::cout << "asize " << asize << "\n";

      leveldb::silkstore::Nvmem *nvm = manager->allocate(asize);

      // nvm->init(4000);
      leveldb::NvmemTable *table = new leveldb::NvmemTable(cmp , dynamic_filter, nvm); // = new  silkstore::NvmemTable();

   /*    leveldb::NvmemTable *table1 = new leveldb::NvmemTable(cmp , dynamic_filter,  manager->allocate(10 * MB)); // = new  silkstore::NvmemTable();

      table->print();
      table1->print();

      table = table1;
 */
      leveldb::ValueType type = leveldb::kTypeValue ;
      
      for(int i = 0; i < 10000000; i++){
           leveldb::Slice key(std::to_string(i) + "yunxiao");
           leveldb::Slice value(std::to_string(i+2) + 
            "sdfsfsfdsfsfdssghfsfhidshgdhgkjhdkjfsghkjdsghfkdjshgdfkjsghkh" );
           table->Add(1, type, key, value);

           leveldb::LookupKey getKey(std::to_string(i) + "yunxiao", 1);
           std::string getValue;
           leveldb::Status s;
           table->Get(getKey, &getValue, &s);
           table->Get(getKey, &getValue, &s);

        }



       

}



void CompareMemAndImm(){

      leveldb::DynamicFilter *dynamic_filter =  leveldb::NewDynamicFilterBloom(1000, 0.1);
      leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
      
      size_t gb = GB;
      size_t size = 10 * gb;
      std::cout << "size " << size << "\n";
      leveldb::silkstore::NvmManager *manager 
            = new leveldb::silkstore::NvmManager("/mnt/myPMem/pmem.002", size);

      size_t asize = 50;
      asize *=  MB;
      std::cout << "asize " << asize << "\n";

      leveldb::silkstore::Nvmem *nvmem = manager->allocate(asize);

      // nvm->init(4000);
      leveldb::NvmemTable *nvm = new leveldb::NvmemTable(cmp , dynamic_filter, nvmem); // = new  silkstore::NvmemTable();

      leveldb::MemTable   *mem = new leveldb::MemTable(cmp , nullptr);

      //   
      leveldb::ValueType type = leveldb::kTypeValue ;
      size_t seq = 0;

      std::map<std::string, std::string> m;

      for(int i = 0; i < 300000; i++){
        //    std::cout<< i << "\n";
            std::string strkey = std::to_string(rand() % 10000);
            std::string strvalue = std::to_string(rand() );
            leveldb::Slice key(strkey);
            leveldb::Slice value(strvalue);
          // leveldb::Slice key(std::to_string(i) );
          // leveldb::Slice value(std::to_string(i) );
           m[strkey] = strvalue;
           nvm->Add(i, type, key, value);
           mem->Add(i, type, key, value);

           // std:: cout << key.ToString()<<" " << value.ToString() << "\n"; 
            std::string str = std::to_string(rand());
           leveldb::LookupKey getKey(str, i);
           std::string getMem;
           std::string getNvm;
           leveldb::Status s;
           mem->Get(getKey, &getMem, &s);
           nvm->Get(getKey, &getNvm, &s);
           if (getMem != getNvm ){
                 std::cout << "getMem != getImm \n";
                 return ;
           }

        }

      leveldb::Iterator* itnvm = nvm->NewIterator();
      leveldb::Iterator* itmem = mem->NewIterator();

      itnvm->SeekToFirst();
      itmem->SeekToFirst();
      
      auto mapit = m.begin();

      int count = 0;
      /*   while(mapit != m.end()){
            count++;
            std::cout<< mapit->first <<" ";
            ++mapit;
        } */

      std::cout << "#### Test Iterator @@@@ "<< m.size() <<"\n" ;
      
      // while(itnvm->Valid()){
      while(mapit != m.end()){
           /*  if (itnvm->key().ToString() != itmem->key().ToString() ){
                  std::cout << "Key ont equal "<< count << "\n" ;
                  return ;
            } */

            if (itnvm->value().ToString() != mapit->second ){
                  std::cout << "Value ont equal \n";
                  return ;                  
            }
           
            itmem->Next();
            itnvm->Next();
            ++mapit;
            count++;
      } 

      std::cout << "number :" << count << "\n";

}



void WriteData(){


      leveldb::DynamicFilter *dynamic_filter =  leveldb::NewDynamicFilterBloom(1000, 0.1);
      leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
      size_t gb = GB;
      size_t size = 10 * gb;
      std::cout << "size " << size << "\n";
      leveldb::silkstore::NvmManager *manager 
            = new leveldb::silkstore::NvmManager("/mnt/myPMem/pmem.002", size);
      size_t asize = 50;
      asize *=  MB;
      std::cout << "asize " << asize << "\n";
      leveldb::silkstore::Nvmem *nvmem = manager->allocate(asize);
      // nvm->init(4000);
      leveldb::NvmemTable *nvm = new leveldb::NvmemTable(cmp , dynamic_filter, nvmem); // = new  silkstore::NvmemTable();
    

       leveldb::ValueType type = leveldb::kTypeValue ;


      for(int i = 0; i < 300; i++){
            std::string strkey = std::to_string(i) + "yunxiao";
            std::string strvalue = std::to_string(rand() ) + "du";
            leveldb::Slice key(strkey);
            leveldb::Slice value(strvalue);
            nvm->Add(i, type, key, value);
            std::cout<<strkey << " ";
        }
        std::cout<< "\n";

}

void Recovery(){


      leveldb::DynamicFilter *dynamic_filter =  leveldb::NewDynamicFilterBloom(1000, 0.1);
      leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
      size_t gb = GB;
      size_t size = 10 * gb;
      std::cout << "size " << size << "\n";
      leveldb::silkstore::NvmManager *manager 
            = new leveldb::silkstore::NvmManager("/mnt/myPMem/pmem.002", size);
      size_t asize = 50;
      asize *=  MB;
      std::cout << "asize " << asize << "\n";
      leveldb::silkstore::Nvmem *nvmem = manager->allocate(asize);
      // nvm->init(4000);
      leveldb::NvmemTable *nvm = new leveldb::NvmemTable(cmp , dynamic_filter, nvmem); // = new  silkstore::NvmemTable();
      nvm->Recovery();
}
int main(int argc, char** argv){
      //deleteUpdate();
      CompareMemAndImm();
     // IteratorTest();

      WriteData();
      Recovery();
   /*    
      addget(); */
     // std::cout<< " &&&&&&&&&&&&&&&&&&&&&&&&&&&&&& \n $$$$$$$$$$$$$$$$$$$$$$$$$$ \n";
      // copyNvmem();
     //  IteratorTest();
     //  CompareMemAndImm();
      //memtableTest();
       return 0;
}




//}
