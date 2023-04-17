//
// Created by yichen_X on 2023/4/2.
//

#ifndef LSM_KV_BLOOMFILTER_H
#define LSM_KV_BLOOMFILTER_H

#include "MurmurHash3.h"
#include <iostream>
#include <vector>
#include <cstring>
#include <bitset>

#define BLOOMFILTER_SIZE 10240*8

class bloomfilter {
private:

    uint64_t hash_num;

public:
    uint64_t bitset_size;
//    std::vector<bool> bitset;
    std::bitset<BLOOMFILTER_SIZE> bitset;

    bloomfilter(){
        hash_num = 4;
        bitset_size = BLOOMFILTER_SIZE;
//        bitset.resize(bitset_size, false);
        bitset.reset();
    }

    ~bloomfilter();
    void insert(const uint64_t key);
    bool find(const uint64_t key);
    void saveBloomFilter(char* buf);

    void loadBloomFilter(char* buf){
        memcpy((char*)&bitset, buf, BLOOMFILTER_SIZE/8);
    }

};


#endif //LSM_KV_BLOOMFILTER_H
