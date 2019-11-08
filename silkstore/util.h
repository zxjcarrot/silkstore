//
// Created by zxjcarrot on 2019-07-18.
//

#ifndef SILKSTORE_UTIL_H
#define SILKSTORE_UTIL_H

#include <functional>
#include <vector>

namespace leveldb {
namespace silkstore {

class DeferCode {
public:
    DeferCode(std::function<void()> code): code(code) {}
    ~DeferCode() { code(); }
private:
    std::function<void()> code;
};

// Perform a KMeans clustering on one-dimensional data_points
// Produces a vector of group ids. The ith element in the returned vector indicates
// the group id of data_points[i] after clustering.
std::vector<int> KMeans(std::vector<double> & data_points, int k);


class Segmenter {
public:
    virtual std::vector<int> classify(const std::vector<double> & data_points, int k)=0;

    virtual ~Segmenter(){}
};

class KMeansSegmenter: public Segmenter {
public:
    std::vector<int> classify(const std::vector<double> & data_points, int k) override;
};

class JenksSegmenter: public Segmenter {
public:
    std::vector<int> classify(const std::vector<double> & data_points, int k) override;
};

}  // namespace silkstore
}  // namespace leveldb

#endif // SILKSTORE_UTIL_H
