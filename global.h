//
// Created by yichen_X on 2023/4/4.
//

#ifndef LSM_KV_GLOBAL_H
#define LSM_KV_GLOBAL_H
#include "sstable.h"
#include "skiplist.h"
#include "bloomfilter.h"
#include "utils.h"
#include <filesystem>

class global {
public:
    static sstable_cache* store_memtable(skiplist* memtable,
                                         const std::string& dir_path,
                                         uint64_t time_stamp,
                                         uint64_t tag);
    static string read_disk(index_item* index_i, const std::string& dir_path, const uint64_t& time_stamp, const uint64_t& tag, uint32_t& value_size);
    static sstable_cache* read_sstable(const std::string& file_path);

};


#endif //LSM_KV_GLOBAL_H
