//
// Created by yichen_X on 2023/4/2.
//

#ifndef LSM_KV_BLOOMFILTER_H
#define LSM_KV_BLOOMFILTER_H

#include "MurmurHash3.h"
#include <iostream>
#include <vector>
#include <cstring>

#define BLOOMFILTER_SIZE 10240

class bloomfilter {
private:

    uint64_t hash_num;

public:
    uint64_t bitset_size;
    std::vector<bool> bitset;

    bloomfilter(uint64_t size = BLOOMFILTER_SIZE){
        bitset_size = size;
        hash_num = 4;
        bitset.resize(bitset_size, false);
    }

    ~bloomfilter();
    void insert(const uint64_t key);
    bool find(const uint64_t key);

};


#endif //LSM_KV_BLOOMFILTER_H
