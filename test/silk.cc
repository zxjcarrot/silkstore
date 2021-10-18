/* 
   Sequential and Random Write / Get  with 100 million operations
*/

#include <assert.h>
#include <string.h>
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
    snprintf(key, sizeof(key), "%016d\n", rand() % 3000000);
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
        options.nvm_size = 20UL*(1024*1024*1024);
        leveldb::Status s = leveldb::DB::OpenSilkStore(options, "./silkdb", &db_);
        assert(s.ok()==true);
        std::cout << " ######### SequentialWrite Open DB ######## \n";

        static const int kNumOps = 30000;
        static const long int kNumKVs = 30000;
        static const int kValueSize = 100;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = RandomNumberKey(&rnd);
        }
        sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Sequential Insert And Get Test ######## \n";
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                db_->Put(WriteOptions(),key, value);
                m[key] = value;          
                std::string res;
                s = db_->Get(ReadOptions(), key, &res);
                if (res != value){
                   fprintf(stderr, "Key %s has wrong value %s \n",key.c_str(), res.c_str() );
                   return ;
                }
        }
        std::cout << " @@@@@@@@@ PASS #########\n";
        std::cout << " ######### Begin Sequential Get Test ######## \n";
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string res;
                s = db_->Get(ReadOptions(), key, &res);
                auto ans = m[key];
                if (res != ans){
                   fprintf(stderr, "Key %s has wrong value %s \n",key.c_str(), res.c_str() );
                   return ;
                }
        }

        std::cout << " @@@@@@@@@ PASS #########\n";
        std::cout << " ######### Begin Sequential Iterator Test ######## \n";

        auto it = db_->NewIterator(ReadOptions());
        it->SeekToFirst();
        auto mit = m.begin();
        int count = 0;
        while (mit != m.end() && it->Valid()) {
            auto res_key = it->key();
            auto res_value = it->value();
            auto ans_key = mit->first;
            auto ans_value = mit->second;
            std::cout << res_key.ToString() << " " << ans_key << "\n"; 
            std::cout << res_value.ToString() << " " << ans_value << "\n";        
                   
            assert(res_key == ans_key);
            assert(res_value == ans_value);
            it->Next();
            ++mit;
            count++;
        }
        std::cout << " @@@@@@@@@ PASS #########\n";
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
        options.nvm_size = 20UL*(1024*1024*1024);
        options.use_memtable_dynamic_filter = true;
        leveldb::Status s = leveldb::DB::OpenSilkStore(options, "./silkdb", &db_);
        assert(s.ok()==true);
        std::cout << " ######### Open DB ######## \n";

       /*  static const int kNumOps = 100000000;
        static const long int kNumKVs = 30000000; */
        static const int kNumOps = 30000000;
        static const long int kNumKVs = 300000;
        static const int kValueSize = 100;
        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
            keys[i] = RandomNumberKey(&rnd) ;
        }
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Random Insert And Get Test ######## \n";
        size_t countNum = 0;
        for (int i = 0; i < kNumOps; i++) {
          std::string key = keys[i % kNumKVs];
          std::string value = RandomString(&rnd, kValueSize);
          auto s = db_->Put(WriteOptions(),key, value);
          m[key] = value;
          
          for (int j = 0; j < 3; ++j) {
              int idx = std::min(rand(), i) % kNumKVs;
              string res = "";
              auto s = db_->Get(ReadOptions(), keys[idx], &res);
              countNum++;
              auto ans = m[keys[idx]];
              if (res != ans) {
                  fprintf(stderr, "Key %s has wrong value %s \n",keys[idx].c_str(), res.c_str() );
                  fprintf(stderr, "correct value is %s \n status: %s \n",  ans.c_str(), s.ToString().c_str());
                  fprintf(stderr, "count %ld \n",  countNum);
                  return ;
              }
          }
    }

    std::cout << " @@@@@@@@@ PASS #########\n";
    std::cout << " ######### Begin Random Get Test ######## \n";

    for (int i = 0; i < kNumOps; ++i) {
        int idx = rand() % kNumKVs;
        string res;
        db_->Get(ReadOptions(), keys[idx], &res);
        auto ans = m[keys[idx]];
         if (res != ans) {
                fprintf(stderr, "2 Key %s has wrong value %s \n",keys[idx].c_str(), res.c_str() );
                fprintf(stderr, "correct value is %s \n status: %s \n",  ans.c_str(), s.ToString().c_str());
                return ;
            }
    }
    std::cout << " @@@@@@@@@ PASS #########\n";
    std::cout << " ######### Begin Iterator Test ######## \n";

    auto it = db_->NewIterator(ReadOptions());
    it->SeekToFirst();
    auto mit = m.begin();
    int count = 0;
    while (mit != m.end() && it->Valid()) {
        auto res_key = it->key();
        auto res_value = it->value();
        auto ans_key = mit->first;
        auto ans_value = mit->second;
        std::cout << res_key.ToString() << " " << ans_key << "\n";
        assert(res_key == ans_key);
        assert(res_value == ans_value);
        it->Next();
        ++mit;
        count++;
    }
    std::cout << " @@@@@@@@@ PASS #########\n";
    delete db_;
    std::cout << " Delete Open Db \n";
}

int main(){
        //SequentialWrite();
        RandomWrite();
        return 0;
}