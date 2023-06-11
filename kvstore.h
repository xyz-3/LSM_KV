#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "utils.h"
#include "global.h"
#include <fstream>
#include <vector>
#include <map>
#include <queue>
#include <set>

#define MEMTABLE_SIZE (2*1024*1024)
#define MODE_FILE_PATH "../config/default.conf"

class cmp{
public:
    bool operator()(sstable_cache* sa, sstable_cache* sb){  //big heap
        if(sa->get_timestamp() == sb->get_timestamp()){
            if(sa->get_min_key() == sb->get_min_key()){
                return sa->get_max_key() < sb->get_max_key();
            }else{
                return sa->get_min_key() < sb->get_min_key();
            }
        }else {
            return sa->get_timestamp() < sb->get_timestamp();
        }
    }
};


class KVStore : public KVStoreAPI {
private:
    /* DRAM */
    skiplist* mem_table;
    /* DISK */
    map<uint64_t, map<pair<uint64_t, uint64_t>, sstable_cache*>> sstable; //level, time_stamp, tag, sstable_cache

    /* time stamp */
    uint64_t time;
    /* tag */
    uint64_t TAG;
    /* dir */
    string dir_name;

    /* read level mode */
    enum mode{
        Tiering,
        Leveling
    };
    map<uint64_t, pair<uint64_t, mode>> level_mode; //level, <limits_number, mode>
    void read_mode();

    void read_data_from_disk();

    static void get_level_timeStamp_tag(uint64_t& level, uint64_t& time_stamp, uint64_t& tag, const string& file_name);


    /* Compaction functions */
    void compaction(uint64_t level_x, uint64_t level_y);

    void select_file(vector<pair<uint64_t, uint64_t>>& x_select_files,
                     vector<pair<uint64_t, uint64_t>>& y_select_files,
                     uint64_t level_x,
                     uint64_t level_y);

    void read_key_value_from_disk(uint64_t& level, uint64_t& time_stamp, uint64_t& tag,
                                  sstable_cache* cur_sstable,
                                  map<uint64_t, pair<pair<uint64_t, uint64_t>, string>>& key_value,
                                  set<uint64_t>& keys);

    void compaction_write(map<uint64_t, pair<pair<uint64_t, uint64_t>, string>>& key_value,
                          uint64_t& level,
                          uint64_t& new_time_stamp);

    void write(uint64_t& level,
               uint64_t& num,
               uint64_t& min_key,
               uint64_t& max_key,
               uint64_t& t_s,
               uint64_t& size,
               vector<pair<uint64_t, string>>& kvs);

public:
    explicit KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;
};

