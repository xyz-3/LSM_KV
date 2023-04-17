#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    time = 1;
    dir_name = dir;
    mem_table = new skiplist();

    /* read data from disk and config sstable */
    read_data_from_disk();
    read_mode();
}

KVStore::~KVStore()
{
    if(mem_table && mem_table->head && mem_table->head->next[0]) {
        global::store_memtable(mem_table, dir_name + "/level-0", time);
    }
    time++;
    delete mem_table;
    /* Remove sstable */
//    for(int i = 0; i < sstable.size(); ++i){
//        for(int j = 0; j < sstable[i].size(); ++j){
//            delete sstable[i][j];
//        }
//    }
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
    uint64_t cur_time_stamp = res->get_timestamp();
    sstable[0][cur_time_stamp] = res;
    delete mem_table;
    mem_table = new skiplist();
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
    if(get_value != "") return get_value;

    //value not exist in skiplist
    uint64_t value_time_stamp = 0;
    uint32_t value_size = 0;
    string value = "";

    for(auto& level_sstable : sstable){
        for(auto& time_sstable : level_sstable.second){
            auto cur_table = time_sstable.second;
            if(!cur_table) continue;
            if(!cur_table->is_in_range(key) || value_time_stamp >= time_sstable.first) continue;
            if(cur_table->find_key_in_bloomfilter(key)){
                //check if it really exists
                index_item* index_i = cur_table->find_key_in_indexs(key, value_size);
                if(index_i){
                    //read data
                    string dir_path = dir_name + "/level-" + to_string(level_sstable.first);
                    uint64_t time_stamp = cur_table->get_timestamp();
                    string tmp_value = global::read_disk(index_i, dir_path, time_stamp, value_size);
                    if(tmp_value != ""){
                        value = tmp_value;
                        value_time_stamp = time_sstable.first;
                    }
                }
            }
        }
    }

    if(value == "~DELETED~") return "";

    return value;
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

    /* Remove sstable */
//    for(int i = 0; i < sstable.size(); ++i){
//        for(int j = 0; j < sstable[i].size(); ++j){
//            delete sstable[i][j];
//        }
//    }

    /* Remove sstable files and directories */
    vector<string> dir_list;
    utils::scanDir(dir_name, dir_list);
    for(auto dir_nm : dir_list){
        if(dir_nm == "." || dir_nm == "..") continue;
        string dir_path = dir_name + "/" + dir_nm;
        vector<string> file_list;
        utils::scanDir(dir_path, file_list);
        for(auto file_name : file_list){
            if(file_name == "." || file_name == "..") continue;
            string file_path = dir_path + "/" + file_name;
            utils::rmfile(file_path.c_str());
        }
        utils::rmdir(dir_path.c_str());
    }

    /* Reset time stamp */
    time = 1;
}

void KVStore::read_data_from_disk() {
    vector<string> dir_list;
    utils::scanDir(dir_name, dir_list);
    for(auto dir_nm : dir_list){
        if(dir_nm == "." || dir_nm == "..") continue;
        string dir_path = dir_name + "/" + dir_nm;
        vector<string> file_list;
        utils::scanDir(dir_path, file_list);
        for(auto file_name : file_list){
            if(file_name == "." || file_name == "..") continue;
            string file_path = dir_path + "/" + file_name;
            //get level and time stamp
            uint64_t level, time_stamp;
            get_level_timeStamp(level, time_stamp, file_path);

            //open file and read data
            sstable_cache* history_sstable = global::read_sstable(file_path);
            //store history data to sstable
            sstable[level][time_stamp] = history_sstable;
        }
    }
}

void KVStore::get_level_timeStamp(uint64_t &level, uint64_t &time_stamp, const std::string &file_name) {
    string tmp = file_name;
    //filename format: ./data/level-20/123.sst
    tmp = tmp.substr(0, tmp.size() - 4);
    tmp = tmp.substr(tmp.find_last_of("/") + 1);
    time_stamp = stoull(tmp);
    tmp = file_name;
    tmp = tmp.substr(0, tmp.find_last_of("/"));
    tmp = tmp.substr(tmp.find_last_of("-") + 1);
    level = stoull(tmp);
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
}


void KVStore::select_file(vector<pair<uint64_t, uint64_t>> &x_select_files,
                          vector<pair<uint64_t, uint64_t>> &y_select_files, uint64_t level_x, uint64_t level_y) {
    //select level_x
    //if level_x mode is Tiering, select all files
    if(level_mode[level_x].second == Tiering){
        for(auto& time_sstable : sstable[level_x]){
            x_select_files.push_back(make_pair(level_x, time_sstable.first));
        }
    }else{
        //select files
        uint64_t select_num = sstable[level_x].size() - level_mode[level_x].first;

    }


}

void KVStore::compaction(uint64_t level_x, uint64_t level_y) {
    //check if level_x is empty
    if(sstable[level_x].empty()) return;
    //check if level_x file nums is greater than limits
    if(sstable[level_x].size() <= level_mode[level_x].first) return;

    //select sstable
    vector<pair<uint64_t, uint64_t>> x_select_files, y_select_files;  //<level, time stamp>
    select_file(x_select_files, y_select_files, level_x, level_y);



    /*//get all sstable in level_x
    vector<sstable_cache*> level_x_sstable;
    for(auto& time_sstable : sstable[level_x]){
        level_x_sstable.push_back(time_sstable.second);
    }

    //merge all sstable in level_x
    sstable_cache* merged_sstable = global::merge_sstable(level_x_sstable);

    //write merged sstable to disk
    string dir_path = dir_name + "/level-" + to_string(level_y);
    string file_path = dir_path + "/" + to_string(time) + ".sst";
    global::write_sstable(merged_sstable, file_path);

    //update sstable
    sstable[level_y][time] = merged_sstable;

    //delete old sstable
    for(auto& time_sstable : sstable[level_x]){
        delete time_sstable.second;
    }
    sstable[level_x].clear();

    //update time
    time++;*/
}

//pass
void KVStore::read_mode() {
    ifstream file;
    file.open(MODE_FILE_PATH, ios::in);

    if(!file.is_open()){
        return;
    }

    //read lines from file
    //line format: level file_max_num mode
    string line;
    uint64_t level, num;
    mode l_mode;
    while(getline(file, line)){
        stringstream ss(line);
        ss >> level >> num;
        string mode_str;
        ss >> mode_str;
        if(mode_str == "Tiering"){
            l_mode = Tiering;
        }else{
            l_mode = Leveling;
        }
        level_mode[level] = make_pair(num, l_mode);
    }

    file.close();
}