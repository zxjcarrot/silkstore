//
// Created by zxjcarrot on 2019-11-07.
//
#include <vector>
#include <cstdio>

#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "silkstore/util.h"

namespace leveldb {

class SegmenterTest { };

TEST(SegmenterTest, classify) {
    using namespace std;
    leveldb::silkstore::Segmenter * segmenter = new leveldb::silkstore::KMeansSegmenter;
    //vector<double> data_points{1,1,2,3,10,11,13,67,71};
    vector<double> data_points{1,1,1,1,1,1,1,1,1};
    vector<int> ans = segmenter->classify(data_points, 3);
    assert(ans.size() == data_points.size());
    for (int i = 0; i < ans.size(); ++i) {
        fprintf(stderr, "%d ", ans[i]);
    }
    fprintf(stderr, "\n");
}

}  // namespace leveldb

int main(int argc, char** argv) {
    return leveldb::test::RunAllTests();
}
