#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "utils.h"
#include "global.h"
#include <fstream>
#include <vector>
#include <map>

#define MEMTABLE_SIZE 2*1024*1024
#define MODE_FILE_PATH "../config/default.conf"

class KVStore : public KVStoreAPI {
private:
    /* DRAM */
    skiplist* mem_table;
    /* DISK */
    map<uint64_t, map<uint64_t, sstable_cache*>> sstable; //level, time_stamp, sstable_cache

    /* time stamp */
    uint64_t time;
    /* dir */
    string dir_name;

    /* read level mode */
    enum mode{
        Tiering,
        Leveling
    };
    map<uint64_t, pair<uint64_t, mode>> level_mode;
    void read_mode();

    void read_data_from_disk();

    void get_level_timeStamp(uint64_t& level, uint64_t& time_stamp, const string& file_name);

    void compaction(uint64_t level_x, uint64_t level_y);
    void select_file(vector<pair<uint64_t, uint64_t>>& x_select_files, vector<pair<uint64_t, uint64_t>>& y_select_files, uint64_t level_x, uint64_t level_y);

public:
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};

