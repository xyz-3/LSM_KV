//
// Created by yichen_X on 2023/4/4.
//

#include "global.h"


/**
 * Store memtable to disk, return sstable_cache
 * @param memtable: memtable
 * @param dir_path: the directory path of sstable
 * @param time_stamp: time stamp for this sstable
 * @param tag: tag for this sstable
 */
sstable_cache* global::store_memtable(skiplist *memtable,
                                      const std::string &dir_path,
                                      const uint64_t time_stamp,
                                      const uint64_t tag) {
    auto* new_sstable_cache = new sstable_cache();

    /* set tag */
    new_sstable_cache->set_tag(tag);

    if(!utils::dirExists(dir_path)){
        utils::mkdir(dir_path.c_str());
    }

    /* File Path & Add time stamp */
    string file_path = dir_path + "/" + to_string(time_stamp) + "_" + to_string(tag) + ".sst";
    ofstream file;
    file.open(file_path, ios::out | ios::binary);

    if(!file.is_open()){
        return nullptr;
    }

    file.seekp(0);

    /* Set time stamp */
    new_sstable_cache->set_timestamp(time_stamp);
    file.write((char*)&time_stamp, 8);

    /* Set total num */
    uint64_t length = memtable->get_length();
    new_sstable_cache->set_num(length);
    file.write((const char*)&length, 8);

    /* Set min & max key */
    uint64_t min_key = memtable->get_min_key();
    new_sstable_cache->set_minkey(min_key);
    file.write((const char*)&min_key, 8);

    uint64_t max_key = memtable->get_max_key();
    new_sstable_cache->set_maxkey(max_key);
    file.write((const char*)&max_key, 8);

    /* BloomFilter */
    memtable->store_bloomfilter(new_sstable_cache);
    char* bloom_buffer = new char[10240];
    new_sstable_cache->bloomfilter->saveBloomFilter(bloom_buffer);
    file.write(bloom_buffer, 10240);
    delete[] bloom_buffer;

    /* Index Area */
    uint32_t offset = 10272 + 12 * length;
    skiplist::node<uint64_t, string>* cur = memtable->head->next[0];
    while(cur){
        uint64_t cur_key = cur->get_key();
        new_sstable_cache->Indexs.emplace_back(cur_key, offset);
        file.write((const char*)&cur_key, 8);
        file.write((const char*)&offset, 4);
        offset += cur->get_value().size();
        cur = cur->next[0];
    }

    /* Value Area */
    cur = memtable->head->next[0];
    while(cur){
        string cur_value = cur->get_value();
        auto cur_v_str = cur_value.c_str();
        file.write((char*)(cur_v_str), cur_value.size());
        cur = cur->next[0];
    }

    file.close();
    return new_sstable_cache;
}

/**
 * Read value from disk by given index item
 * @param index_i: index item(key & value)
 * @param dir_path: the directory path of sstable
 * @param time_stamp: time stamp for this sstable
 * @param tag: tag for this sstable
 * @param value_size: value size to be set
 */
string global::read_disk(index_item* index_i,
                         const std::string &dir_path,
                         const uint64_t &time_stamp,
                         const uint64_t &tag,
                         uint32_t &value_size) {
    if(!utils::dirExists(dir_path)) return "";

    string file_path = dir_path + "/" + to_string(time_stamp) + "_" + to_string(tag) + ".sst";
    ifstream file;
    file.open(file_path, ios_base::in|ios_base::binary);
    if(!file.is_open()) return "";

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

/**
 * Read file from disk and store information to sstable_cache
 * @param file_path: file path of the file
 */
sstable_cache* global::read_sstable(const std::string &file_path) {
    ifstream file;
    file.open(file_path, ios_base::in|ios_base::binary);
    if(!file.is_open()) return nullptr;

    auto* new_sstable_cache = new sstable_cache();

    /* read time stamp */
    uint64_t time_stamp;
    file.read((char*)&time_stamp, 8);
    new_sstable_cache->set_timestamp(time_stamp);

    /* read total num */
    uint64_t num;
    file.read((char*)&num, 8);
    new_sstable_cache->set_num(num);

    /* read min key */
    uint64_t min_key;
    file.read((char*)&min_key, 8);
    new_sstable_cache->set_minkey(min_key);

    /* read max key */
    uint64_t max_key;
    file.read((char*)&max_key, 8);
    new_sstable_cache->set_maxkey(max_key);

    /* read bloomfilter */
    char* bloom_buffer = new char[10240];
    file.read(bloom_buffer, 10240);
    new_sstable_cache->bloomfilter = new bloomfilter();
    new_sstable_cache->bloomfilter->loadBloomFilter(bloom_buffer);

    /* read index area */
    for(int i = 0; i < num; i++){
        uint64_t key;
        uint32_t offset;
        file.read((char*)&key, 8);
        file.read((char*)&offset, 4);
        new_sstable_cache->Indexs.emplace_back(key, offset);
    }

    file.close();
    return new_sstable_cache;
}