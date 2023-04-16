//
// Created by yichen_X on 2023/4/1.
//

#ifndef LSM_KV_SKIPLIST_H
#define LSM_KV_SKIPLIST_H


#include <vector>
#include <iostream>
#include <random>
#include <cmath>
#include <fstream>
#include "sstable.h"
#include "utils.h"
using namespace std;

class skiplist {
private:
    int max_level;
    int cur_level;
    uint64_t memory_size;
    uint64_t length;

    int random_level();

    mt19937 gen{random_device{}()};
    uniform_real_distribution<double> dis;
public:
    struct node{
    private:
        uint64_t key;
        string value;

    public:
        vector<node*> next;
        node* prev = nullptr;
        int level;

        node(){}
        node(const uint64_t k, const string v, const int l): key(k), value(v), next(l, nullptr), level(l){}
        ~node() {}

        uint64_t get_key() const { return key;}
//        void set_key(const uint64_t k){ key = k;}
        string get_value() const { return value;}
        void set_value(const string& s) {value = s;}
    };


    node* head;

    skiplist(int maxlevel = 16);

    void put(uint64_t key, const string &s);
    string get(uint64_t key) const;
    bool del(uint64_t key);
    uint64_t get_size() const;
    uint64_t get_length() const;
    uint64_t get_min_key() const;
    uint64_t get_max_key() const;
    void store_bloomfilter(sstable_cache*& ssc);
//    sstable_cache* store_memtable(const string& dir, const uint64_t time);
    ~skiplist();
};


#endif //LSM_KV_SKIPLIST_H
