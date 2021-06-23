#include <assert.h>
#include <string.h>
//#include "silkstore/db.h"
#include "leveldb/db.h"
#include "leveldb/status.h"
#include <iostream>
#include <vector>
#include <map>
#include <unistd.h>


using namespace leveldb;
using namespace std;



class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0

    uint64_t product = seed_ * A;

    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));

    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  uint32_t Uniform(int n) { return Next() % n; }

  bool OneIn(int n) { return (Next() % n) == 0; }
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
  }
  return Slice(*dst);
}



std::string RandomNumberKey(Random* rnd) {
    char key[100];
    snprintf(key, sizeof(key), "%016d\n", rand() % 1000000);
    return std::string(key, 16);
}

std::string RandomString(Random* rnd, int len) {
    std::string r;
    RandomString(rnd, len, &r);
    return r;
}



void SequentialWrite(){
  
        leveldb::DB* db_ = nullptr;
        leveldb::Options options;
        options.create_if_missing = true;
        options.compression = leveldb::kNoCompression;
        options.enable_leaf_read_opt = true;
        options.memtbl_to_L0_ratio = 15;
        options.write_buffer_size = 64UL * 1024 * 1024;
        options.leaf_max_num_miniruns = 15;
        options.maximum_segments_storage_size = 90UL*1024*1024*1024;
        //options.nvm_size = 20UL*(1024*1024*1024);
        leveldb::Status s = leveldb::DB::OpenSilkStore(options, "./silkdb", &db_);
        assert(s.ok()==true);
        std::cout << " ######### Open DB ######## \n";

        static const int kNumOps = 30000000;
        static const long int kNumKVs = 30000000;
        static const int kValueSize = 100;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = RandomNumberKey(&rnd);
        }

        sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        


        std::cout << " ######### Begin Test ######## \n";


        clock_t start, finish;  
        start = clock();  

        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                //std::string value = std::to_string(i);
                db_->Put(WriteOptions(),key, value);
                
                std::string res;
              //  s = db_->Get(ReadOptions(), "test", &res);
              //  std::cout<< " Get " << i << s.ToString() << std::endl;
        }

        finish = clock();  
        double time = (double)(finish - start) / CLOCKS_PER_SEC;  
        printf( "the total time's  %f seconds\n", time ); 



        printf( "%f seconds\n",  time / kNumOps );  
        int insertSize = RandomNumberKey(&rnd).size() + RandomString(&rnd, kValueSize).size();

        printf( "%f throught MB\n",  (double) (insertSize * kNumOps) / (1024.0 * 1024.0) / time );  

       /*  s = db_->Put(WriteOptions(), "test", "Hello World!");
        assert(s.ok());
        std::string res;
        s = db_->Get(ReadOptions(), "test", &res);
        assert(s.ok()==true);
        std::cout << res << std::endl; */
        delete db_;
        std::cout << " Delete Open Db \n";
}


void RandomWrite(){


        leveldb::DB* db_ = nullptr;
        leveldb::Options options;
        options.create_if_missing = true;
        options.compression = leveldb::kNoCompression;
        options.enable_leaf_read_opt = true;
        options.memtbl_to_L0_ratio = 15;
        options.write_buffer_size = 64UL * 1024 * 1024;
        options.leaf_max_num_miniruns = 15;
        options.maximum_segments_storage_size = 90UL*1024*1024*1024;
       // options.nvm_size = 20UL*(1024*1024*1024);
      //  options.use_memtable_dynamic_filter = true;
        leveldb::Status s = leveldb::DB::OpenSilkStore(options, "./silkdb", &db_);
        assert(s.ok()==true);
        std::cout << " ######### Open DB ######## \n";

        static const int kNumOps = 30000000;
        static const long int kNumKVs = 30000000;
        static const int kValueSize = 100;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
            keys[i] = RandomNumberKey(&rnd) ;
        }
        std::map<std::string, std::string> m;

        std::cout << " ######### Begin Test ######## \n";
        size_t countNum = 0;


        for (int i = 0; i < kNumOps; i++) {
            std::string key = keys[i % kNumKVs];
            std::string value = RandomString(&rnd, kValueSize);
            //std::string value = std::to_string(i);
            auto s = db_->Put(WriteOptions(),key, value);
            // ASSERT_OK( );
            m[key] = value;
          /*   if (key == "0000000000636915"){
                  std::cout<< " insert :"<< key << " value " <<  m[key]
                  << s.ToString()<< " \n";
            } */
           

        for (int j = 0; j < 1; ++j) {
            int idx = std::min(rand(), i) % kNumKVs;
            string res = "";
         //   std::cout << "before " << res << "\n";
            auto s = db_->Get(ReadOptions(), keys[idx], &res);
         //   std::cout << "after " << res << "\n";
            countNum++;
 
            auto ans = m[keys[idx]];
            if (res != ans) {
                fprintf(stderr, "Key %s has wrong value %s \n",keys[idx].c_str(), res.c_str() );
                fprintf(stderr, "correct value is %s \n status: %s \n",  ans.c_str(), s.ToString().c_str());
                fprintf(stderr, "count %ld \n",  countNum);

            
	    	//res = Get(keys[idx]);
                return ;
            }
            //ASSERT_EQ(res, ans);
        }
    }
    sleep(1);
    std::cout << "@@@ Get 2 ###\n";

   // Reopen(nullptr);
    for (int i = 0; i < kNumOps; ++i) {
        int idx = rand() % kNumOps;
        string res;
        db_->Get(ReadOptions(), keys[idx], &res);
        auto ans = m[keys[idx]];
         if (res != ans) {
                fprintf(stderr, "2 Key %s has wrong value %s \n",keys[idx].c_str(), res.c_str() );
                fprintf(stderr, "correct value is %s \n status: %s \n",  ans.c_str(), s.ToString().c_str());
                //res = Get(keys[idx]);
                return ;
            }
      //  ASSERT_EQ(res, ans);
    }

    std::cout << "@@@ Iterator ###\n";

    auto it = db_->NewIterator(ReadOptions());
    it->SeekToFirst();
    auto mit = m.begin();
    int count = 0;
    while (mit != m.end() && it->Valid()) {
     //   std::cout << count ++ << endl;
        auto res_key = it->key();
        auto res_value = it->value();
        auto ans_key = mit->first;
        auto ans_value = mit->second;


        
        assert(res_key == ans_key);
        assert(res_value == ans_value);
        it->Next();
        ++mit;
        count++;
    }
    std::cout << count ++ << endl;
  //  ASSERT_EQ(mit == m.end() && !it->Valid(), true);
        delete db_;
        std::cout << " Delete Open Db \n";
        
}



int main(){
        //SequentialWrite();
        RandomWrite();
        return 0;
}
