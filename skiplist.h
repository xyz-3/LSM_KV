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
    template<typename K, typename V>
    struct node{
    private:
        uint64_t key{};
        string value;

    public:
        node<K , V> **next;

        int level{};

        node()= default;
        node(const K k, const V v, const int l): key(k), value(v), level(l){
            next = new node<K,V>*[level + 1];

            memset(next, 0, sizeof(node<K, V>*)*(level + 1));
        }
        ~node() {
            delete []next;
        }

        K get_key() const { return key;}
        V get_value() const { return value;}
        void set_value(const V& s) {value = s;}
    };


    node<uint64_t, string>* head;

    explicit skiplist(int maxlevel = 16);

    void put(uint64_t key, const string &s);
    string get(uint64_t key) const;
    uint64_t get_size() const;
    uint64_t get_length() const;
    uint64_t get_min_key() const;
    uint64_t get_max_key() const;
    void store_bloomfilter(sstable_cache*& ssc) const;
    ~skiplist();
};


#endif //LSM_KV_SKIPLIST_H
