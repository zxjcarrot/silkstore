//
// Created by zxjcarrot on 2019-11-07.
//

#include <limits>

#include "silkstore/util.h"

namespace leveldb {

namespace silkstore {

std::vector<int> KMeansSegmenter::classify(const std::vector<double> & data_points, int k) {
    k = std::min(k, (int)data_points.size());
    std::vector<double> centroids;
    for (int i = 0; i < k; ++i) {
        double centroid = data_points[rand() % data_points.size()];
        double duplicate = false;
        for (int j = 0; j < centroids.size(); ++j) {
            if (abs(centroid - centroids[j]) < 1e-6) {
                duplicate = true;
            }
        }
        if (duplicate == true) {
            --i;
        } else {
            centroids.push_back(centroid);
        }
    }
    std::vector<int> groups(data_points.size(), -1);
    int steps = 0;
    while (steps < 100) {
        std::vector<std::vector<double>> group_elements(k);
        for (int i = 0; i < data_points.size(); ++i) {
            double min_dis = std::numeric_limits<double>::max();
            int min_group = -1;
            for (int j = 0; j < k; ++j) {
                double dis = abs(data_points[i] - centroids[j]);
                if (min_dis > dis || min_group == -1) {
                    min_dis = dis;
                    min_group = j;
                }
            }
            group_elements[min_group].push_back(data_points[i]);
            groups[i] = min_group;
        }

        std::vector<double> new_centroids;
        for (int i = 0; i < k; ++i) {
            double S = 0;
            for (auto x : group_elements[i])
                S += x;
            new_centroids.push_back(S / group_elements[i].size());
        }
        double centroids_changed = false;
        for (int i = 0; i < k; ++i) {
            if (abs(centroids[i] - new_centroids[i]) > 1e-5) {
                centroids_changed = true;
                break;
            }
        }
        if (centroids_changed == false)
            break;
        centroids = new_centroids;
        ++steps;
    }
    return groups;
}

}// namespace silkstore

}// namespace leveldb

