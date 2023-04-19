//
// Created by yichen_X on 2023/4/4.
//

#include "global.h"

//sstable_cache* global::store_memtable(skiplist *memtable, const std::string &dir_path, const uint64_t time_stamp) {
//    sstable_cache* new_sstable_cache = new sstable_cache();
//    auto size = memtable->get_size();
//    char* buffer = new char [memtable->get_size()];
//    skiplist::node* cur = memtable->head->next[0];
//
////     set time stamp
//    new_sstable_cache->set_timestamp(time_stamp);
//    memcpy(buffer, &time_stamp, 8);
//
////     set total num
//    uint64_t length = memtable->get_length();
//    new_sstable_cache->set_num(length);
//    memcpy(buffer + 8, &length, 8);
//
////     set min key
//    uint64_t min_key = memtable->get_min_key();
//    new_sstable_cache->set_minkey(min_key);
//    memcpy(buffer + 16, &min_key, 8);
//
//    char* index_area = buffer + 10272;
//    uint32_t offset = 10272 + 12 * length;
//    while(cur){
//        uint64_t cur_key = cur->get_key();
//        new_sstable_cache->bloomfilter->insert(cur_key);
//        //write index area
//        *(uint64_t*) index_area = cur_key;
//        index_area += 8;
//        *(uint32_t*) index_area = offset;
//        index_area += 4;
//
//        new_sstable_cache->Indexs.push_back(index_item(cur_key, offset));
//        //write value area
//        uint32_t value_size = cur->get_value().size();
//        memcpy(buffer + offset, cur->get_value().c_str(), value_size);
//        offset += value_size;
//
//        if(!cur->next[0]){
//            //write max key
//            uint64_t max_key = cur->get_key();
//            memcpy(buffer + 24, &max_key, 8);
//            new_sstable_cache->set_maxkey(max_key);
//        }
//        cur = cur->next[0];
//    }
//    //write bloomfilter
////    memcpy(buffer + 32, (char*)&new_sstable_cache->bloomfilter->bitset, 10240);
//    new_sstable_cache->bloomfilter->saveBloomFilter(buffer + 32);
//
//    //open file
//    if(!utils::dirExists(dir_path)){
//        utils::mkdir(dir_path.c_str());
//    }
//
//    string file_path = dir_path + "/" + to_string(time_stamp) + ".sst";
//    ofstream file;
//    file.open(file_path, ios::out | ios::binary);
//
//    if(!file.is_open()){
//        return nullptr;
//    }
//
//    file.seekp(0);
//    file.write(buffer, memtable->get_size());
//
//    file.close();
//    delete[] buffer;
//    return new_sstable_cache;
//}

sstable_cache* global::store_memtable(skiplist *memtable, const std::string &dir_path, const uint64_t time_stamp, const uint64_t tag) {
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

//add tag
string global::read_disk(index_item* index_i, const std::string &dir_path, const uint64_t &time_stamp, const uint64_t &tag, uint32_t &value_size) {
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