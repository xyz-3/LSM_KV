//
// Created by yichen_X on 2023/4/4.
//

#include "global.h"

sstable_cache* global::store_memtable(skiplist *memtable, const std::string &dir_path, const uint64_t time_stamp) {
    sstable_cache* new_sstable_cache = new sstable_cache();

    if(!utils::dirExists(dir_path)){
        utils::mkdir(dir_path.c_str());
    }

    /* File Path & Add time stamp */
    string file_path = dir_path + "/" + to_string(time_stamp) + ".sst";
    ofstream file;
    file.open(file_path, ios::out | ios::binary);

    if(!file.is_open()){
        return nullptr;
    }

    /* Set time stamp */
    new_sstable_cache->set_timestamp(time_stamp);
    file.write((char*)&time_stamp, sizeof(uint64_t));

    /* Set total num */
    uint32_t length = memtable->get_length();
    new_sstable_cache->set_num(length);
    file.write((const char*)&length, sizeof(uint64_t));

    /* Set min & max key */
    uint64_t min_key = memtable->get_min_key();
    new_sstable_cache->set_minkey(min_key);
    file.write((const char*)&min_key, sizeof(uint64_t));

    uint64_t max_key = memtable->get_max_key();
    new_sstable_cache->set_maxkey(max_key);
    file.write((const char*)&max_key, sizeof(uint64_t));

    /* BloomFilter */
    memtable->store_bloomfilter(new_sstable_cache);
    file.write((const char*)&new_sstable_cache->bloomfilter->bitset, new_sstable_cache->bloomfilter->bitset_size);
//    std::cout << new_sstable_cache->bloomfilter->bitset.size() << std::endl;

    /* Index Area */
    uint32_t offset = 10272 + 12 * length;
    skiplist::node* cur = memtable->head->next[0];
    while(cur){
        uint64_t cur_key = cur->get_key();
        new_sstable_cache->Indexs.push_back(index_item(cur_key, offset));
        file.write((const char*)&cur_key, sizeof(uint64_t));
        file.write((const char*)&offset, sizeof(uint32_t));
        offset += cur->get_value().size();
        cur = cur->next[0];
    }

    /* Value Area */
    cur = memtable->head->next[0];
    while(cur){
//        if(cur->get_key() == 2029){
//            std::cout << "test";
//        }
        string cur_value = cur->get_value();
        auto cur_v_str = cur_value.c_str();
//        std::cout << cur_value.size() << std::endl;
        file.write((char*)(cur_v_str), cur_value.size());
        cur = cur->next[0];
    }

    file.close();
    return new_sstable_cache;
}

string global::read_disk(index_item* index_i, const std::string &dir_path, const uint64_t &time_stamp, uint32_t &value_size) {
    if(!utils::dirExists(dir_path)) return "";

    string file_path = dir_path + "/" + to_string(time_stamp) + ".sst";
    ifstream file;
    file.open(file_path, ios_base::in|ios_base::binary);
    if(!file.is_open()) return "";

    file.seekg(0);

    /* read value */
    file.seekg(index_i->get_offset());

    if(value_size == 0){
        string value_str;
        file >> value_str;
        return value_str;
    }

    char* value = new char[value_size + 1];
    value[value_size] = '\0';
    file.read(value, value_size);

    string value_str = value;
    delete[] value;
    return value_str;
}