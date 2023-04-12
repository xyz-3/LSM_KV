#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    time = 1;
    dir_name = dir;
    mem_table = new skiplist();
    sstable.resize(1);
}

KVStore::~KVStore()
{
    global::store_memtable(mem_table, dir_name + "/level-0", time);
//    mem_table->store_memtable("", time);
    time++;
    delete mem_table;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(mem_table->get_size() + s.size() + 12 < MEMTABLE_SIZE){
        mem_table->put(key, s);
        return;
    }
    //size > 2MB, save data into level0(currently just store in level 0)
    sstable_cache* res = global::store_memtable(mem_table, dir_name + "/level-0", time);
    sstable[0].push_back(res);
    delete mem_table;
    mem_table = new skiplist();
//  std::cout << mem_table->get_size() << std::endl;
    time++;
    mem_table->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string get_value = mem_table->get(key);

    if(get_value == "~DELETED~") return "";

    if(get_value == ""){
        bool is_exist = false;
        for(int i = 0; i < sstable.size(); ++i){
            for(int j = 0; j < sstable[i].size(); ++j){
                auto cur_table = sstable[i][j];
                if(cur_table->find_key_in_bloomfilter(key)){
                    uint32_t value_size = 0;
                    index_item* index_i = cur_table->find_key_in_indexs(key, value_size);
                    if(index_i){
                        is_exist = true;
                        //read data from files
                        //check directory exist
                        string dir_path = dir_name + "/level-" + to_string(i);
                        uint64_t time_stamp = cur_table->get_timestamp();

                        string value = global::read_disk(index_i, dir_path, time_stamp, value_size);
                        return value;
                    }
                }
            }
        }
        if(!is_exist) return "";
    }else {
        return get_value;
    }
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string res = get(key);
    if(res == "") return false;

    put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    /* Remove memtable */
    delete mem_table;

    mem_table = new skiplist();

    /* Remove sstables files */
    for(int i = 0; i < sstable.size(); ++i){
        for(int j = 0; j < sstable[i].size(); ++j){
            delete sstable[i][j];
        }
    }
}

/**
 * Scan is optional
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
}