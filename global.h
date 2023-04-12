//
// Created by yichen_X on 2023/4/4.
//

#ifndef LSM_KV_GLOBAL_H
#define LSM_KV_GLOBAL_H
#include "sstable.h"
#include "skiplist.h"
#include "utils.h"

class global {
public:
    static sstable_cache* store_memtable(skiplist* memtable, const std::string& dir_path, const uint64_t time_stamp);
    static string read_disk(index_item* index_i, const std::string& dir_path, const uint64_t& time_stamp, uint32_t& value_size);
};


#endif //LSM_KV_GLOBAL_H
