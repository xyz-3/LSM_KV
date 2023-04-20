#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    time = 1;
    TAG = 1;
    dir_name = dir;
    mem_table = new skiplist();

    /* read data from disk */
    read_data_from_disk();
    /* read mode config */
    read_mode();
}

KVStore::~KVStore()
{
    /* store mem_table to disk if it is not empty */
    if(mem_table && mem_table->head && mem_table->head->next[0]) {
        sstable_cache* res = global::store_memtable(mem_table, dir_name + "/level-0", time, TAG);
        uint64_t cur_ts = res->get_timestamp();
        uint64_t cur_tg = res->get_tag();
        sstable[0][{cur_ts, cur_tg}] = res;
        delete mem_table;
        time++;
        TAG++;
        compaction(0, 1);
    }
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
    sstable_cache* res = global::store_memtable(mem_table, dir_name + "/level-0", time, TAG);
    uint64_t cur_time_stamp = res->get_timestamp();
    uint64_t cur_tag = res->get_tag();
    sstable[0][{cur_time_stamp, cur_tag}] = res;
    delete mem_table;
    mem_table = new skiplist();
    time++;
    TAG++;
    mem_table->put(key, s);
    compaction(0, 1);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string get_value = mem_table->get(key);

    if(get_value == "~DELETED~") return "";
    if(!get_value.empty()) return get_value;

    //value not exist in mem_table, search the disk
    uint64_t value_time_stamp = 0;
    uint64_t value_tag = 0;
    uint32_t value_size = 0;
    string value;

    for(auto& level_sstable : sstable){
        for(auto& time_tag_sstable : level_sstable.second){
//            for(auto&_sstable : time_sstable.second) {
                auto cur_table = time_tag_sstable.second;
                if (!cur_table) continue;
                if (!cur_table->is_in_range(key) || value_time_stamp > time_tag_sstable.first.first || (value_time_stamp == time_tag_sstable.first.first && value_tag > time_tag_sstable.first.second)) continue;
                if (cur_table->find_key_in_bloomfilter(key)) {
                    //check if it really exists
                    index_item *index_i = cur_table->find_key_in_indexs(key, value_size);
                    if (index_i) {
                        //read data
                        string dir_path = dir_name + "/level-" + to_string(level_sstable.first);
                        uint64_t time_stamp = cur_table->get_timestamp();
                        uint64_t tag = cur_table->get_tag();
                        string tmp_value = global::read_disk(index_i, dir_path, time_stamp, tag, value_size);
                        if (!tmp_value.empty()) {
                            value = tmp_value;
                            value_time_stamp = time_tag_sstable.first.first;
                            value_tag = time_tag_sstable.first.second;
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
    if(res.empty()) return false;

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

    /* Remove sstable files and directories */
    vector<string> dir_list;
    utils::scanDir(dir_name, dir_list);
    for(const auto& dir_nm : dir_list){
        if(dir_nm == "." || dir_nm == "..") continue;
        string dir_path = dir_name + "/" + dir_nm;
        vector<string> file_list;
        utils::scanDir(dir_path, file_list);
        for(const auto& file_name : file_list){
            if(file_name == "." || file_name == "..") continue;
            string file_path = dir_path + "/" + file_name;
            utils::rmfile(file_path.c_str());
        }
        utils::rmdir(dir_path.c_str());
    }

    /* Reset time stamp */
    time = 1;
}

/**
 * This function is used to read data from disk when the kvstore is initialized.
 */
void KVStore::read_data_from_disk() {
    vector<string> dir_list;
    utils::scanDir(dir_name, dir_list);
    for(const auto& dir_nm : dir_list){
        if(dir_nm == "." || dir_nm == "..") continue;
        string dir_path = dir_name + "/" + dir_nm;
        vector<string> file_list;
        utils::scanDir(dir_path, file_list);
        for(const auto& file_name : file_list){
            if(file_name == "." || file_name == "..") continue;
            string file_path = dir_path + "/" + file_name;
            //get level and time stamp
            uint64_t level, time_stamp, tag;
            get_level_timeStamp_tag(level, time_stamp, tag, file_path);
            //open file and read data
            sstable_cache* history_sstable = global::read_sstable(file_path);
            history_sstable->set_tag(tag);
            //store history data to sstable
            sstable[level][{time_stamp, tag}] = history_sstable;
        }
    }
}

/**
 * given a file_name as input, split the name and get the level and time stamp
 */
void KVStore::get_level_timeStamp_tag(uint64_t &level, uint64_t &time_stamp, uint64_t& tag, const std::string &file_name) {
    string tmp = file_name;
    //filename format: ./data/level-20/123_130.sst
    //time_stamp
    tmp = tmp.substr(0, tmp.size() - 4);
    tmp = tmp.substr(tmp.find_last_of("/") + 1);
    tmp = tmp.substr(0, tmp.find_last_of("_"));
    time_stamp = stoull(tmp);
    //tag
    tmp = file_name;
    tmp = tmp.substr(0, tmp.size() - 4);
    tmp = tmp.substr(tmp.find_last_of("/") + 1);
    tmp = tmp.substr(tmp.find_last_of("_") + 1);
    tag = stoull(tmp);
    //level
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

/**
 * Select files to be compacted
 * @param x_select_files: files to be compacted in level_x
 * @param y_select_files: files to be compacted in level_y
 * @param level_x: level_x
 * @param level_y: level_y
*/
void KVStore::select_file(vector<pair<uint64_t, uint64_t>> &x_select_files,
                          vector<pair<uint64_t, uint64_t>> &y_select_files, uint64_t level_x, uint64_t level_y) {
    //select level_x
    //if level_x mode is Tiering, select all files
    uint64_t level_x_min_key = UINT_MAX, level_x_max_key = 0;
    if(level_mode[level_x].second == Tiering){
        for(auto& time_tag_sstable : sstable[level_x]){
            x_select_files.emplace_back(time_tag_sstable.first.first, time_tag_sstable.first.second);
        }
    }else{ //level_x mode is Leveling
        //select files in level_x which has small time_stamp(if time_stamp is same, select the files has small key)
        uint64_t select_num = sstable[level_x].size() - level_mode[level_x].first;
        priority_queue<sstable_cache*, vector<sstable_cache*>, cmp> pq;
        for(auto& time_tag_sstable : sstable[level_x]){
            pq.push(time_tag_sstable.second);
            if(pq.size() > select_num) pq.pop();
        }
        while(!pq.empty()){
            x_select_files.emplace_back(pq.top()->get_timestamp(), pq.top()->get_tag());
            pq.pop();
        }
    }

    //select level_y
    //if level_y mode is Tiering, select no file
    if(level_mode[level_y].second == Tiering){
        return;
    }else { //level_y mode is Leveling
        //get the min key & max key of selected files in level_x
        for(auto& level_tag : x_select_files){
            sstable_cache* cur_sstable = sstable[level_x][make_pair(level_tag.first, level_tag.second)];
            if(cur_sstable == nullptr) continue;
            level_x_min_key = min(level_x_min_key, cur_sstable->get_min_key());
            level_x_max_key = max(level_x_max_key, cur_sstable->get_max_key());
        }
        //all the files in level_y that have key range in [min_key, max_key] will be selected
        for(auto& time_tag_sstable : sstable[level_y]){
            sstable_cache* cur_sstable = time_tag_sstable.second;
            if(cur_sstable == nullptr) continue;
            if(!(cur_sstable->get_min_key() > level_x_max_key || cur_sstable->get_max_key() < level_x_min_key)){
                y_select_files.emplace_back(time_tag_sstable.first.first, time_tag_sstable.first.second);
            }
        }
    }
}

void KVStore::read_key_value_from_disk(uint64_t &level, uint64_t &time_stamp, uint64_t &tag,
                                       sstable_cache* cur_sstable,
                                       map<uint64_t, pair<pair<uint64_t, uint64_t>, string>> &key_value,
                                       set<uint64_t> &keys) {
    //get file path
    string file_path = dir_name + "/level-" + to_string(level) + "/" + to_string(time_stamp) + "_" + to_string(tag) + ".sst";

    //check if file exists
    ifstream file(file_path);
    if(!file.is_open()){
        cout << "file not exists" << endl;
        return;
    }

    //read key value
    auto& indexs = cur_sstable->Indexs;
    for(int i = 0; i < indexs.size(); ++i){
        uint64_t key = indexs[i].get_key();
        uint32_t offset = indexs[i].get_offset();
        file.seekg(offset);
        string value;
        if(i == indexs.size() - 1){
            file >> value;
        }else{
            uint32_t next_offset = indexs[i + 1].get_offset();
            uint32_t value_size = next_offset - offset;
            char* value_arr = new char[value_size + 1];
            value_arr[value_size] = '\0';
            file.read(value_arr, value_size);
            value = value_arr;
            delete[] value_arr;
        }

        //check if key exists
        if(keys.find(key) == keys.end()){
            keys.insert(key);
            key_value[key] = make_pair(make_pair(time_stamp, tag), value);
        }else{
            //compare time stamp and tag
            if(key_value[key].first.first < time_stamp || (key_value[key].first.first == time_stamp && key_value[key].first.second < tag)){
                key_value[key] = make_pair(make_pair(time_stamp, tag), value);
            }
        }
    }
}

void KVStore::write(uint64_t &level, uint64_t &num, uint64_t &min_key,
                    uint64_t &max_key, uint64_t &t_s, uint64_t& size,
                    vector<pair<uint64_t, std::string>> &kvs) {
    auto new_sstable = new sstable_cache();
    new_sstable->set_tag(TAG);

    char* buffer = new char[size];
    //set time stamp
    new_sstable->set_timestamp(t_s);
    memcpy(buffer, &t_s, 8);
    //set total num
    new_sstable->set_num(num);
    memcpy(buffer + 8, &num, 8);
    //set min key
    new_sstable->set_minkey(min_key);
    memcpy(buffer + 16, &min_key, 8);

    char* index_area = buffer + 10272;
    uint32_t offset = 10272 + 12 * num;
    for(int i = 0; i < kvs.size(); ++i){
        auto kv = kvs[i];
        uint64_t cur_key = kv.first;
        new_sstable->bloomfilter->insert(cur_key);
        //write index area
        *(uint64_t*)index_area = cur_key;
        index_area += 8;
        *(uint32_t*)index_area = offset;
        index_area += 4;

        new_sstable->Indexs.emplace_back(index_item(cur_key, offset));
        //write value area
        uint32_t value_size = kv.second.size();
        memcpy(buffer + offset, kv.second.c_str(), value_size);
        offset += value_size;

        if(i == kvs.size() - 1){
            //write max key
            uint64_t max_key = cur_key;
            memcpy(buffer + 24, &max_key, 8);
            new_sstable->set_maxkey(max_key);
        }
    }
    //write bloomfilter
    new_sstable->bloomfilter->saveBloomFilter(buffer + 32);

    //open file
    string file_path = dir_name + "/level-" + to_string(level) + "/" + to_string(t_s) + "_" + to_string(TAG) + ".sst";
    ofstream file;
    file.open(file_path, ios::out | ios::binary);

    if(!file.is_open()){
        return;
    }

    file.seekp(0);
    file.write(buffer, size);
    file.close();
    delete[] buffer;


    /* Set time stamp *//*
    new_sstable->set_timestamp(t_s);
    file.write((char*)&t_s, 8);
    *//* Set total num *//*
    new_sstable->set_num(num);
    file.write((char*)&num, 8);
    *//* Set min key *//*
    file.write((char*)&min_key, 8);
    new_sstable->set_minkey(min_key);
    *//* Set max key *//*
    file.write((char*)&max_key, 8);
    new_sstable->set_maxkey(max_key);
    *//* Set bloom filter *//*
    //store data to bloomfilter
    for(auto& kv : kvs){
        uint64_t cur_key = kv.first;
        new_sstable->insert_key_to_bloomfilter(cur_key);
    }
    char* bloom_buffer = new char[10240];
    new_sstable->bloomfilter->saveBloomFilter(bloom_buffer);
    file.write(bloom_buffer, 10240);
    delete[] bloom_buffer;
    *//* Index Area *//*
    uint32_t offset = 10272 + 12 * num;
    for(auto& kv : kvs){
        uint64_t cur_key = kv.first;
        new_sstable->Indexs.emplace_back(cur_key, offset);
        file.write((const char*)&cur_key, 8);
        file.write((const char*)&offset, 4);
        string cur_value = kv.second;
        offset += cur_value.size();
    }
    *//* Value Area *//*
    for(auto& kv : kvs){
        string v = kv.second;
        auto v_str = v.c_str();
        file.write((char*)&v_str, v.size());
    }*/
    sstable[level][make_pair(t_s, TAG)] = new_sstable;
}

void KVStore::compaction_write(map<uint64_t, pair<pair<uint64_t, uint64_t>, std::string>> &key_value, uint64_t& level, uint64_t& new_time_stamp) {
    uint64_t memory_size = 10272;
    uint64_t min_key = UINT_MAX, max_key = 0, num = 0;
    vector<pair<uint64_t, string>> kvs;

    // read key_value pairs
    for(auto & key_value_pair : key_value){
        uint64_t key = key_value_pair.first;
        string value = key_value_pair.second.second;
        if(memory_size + 12 + value.size() >= MEMTABLE_SIZE){
            write(level, num, min_key, max_key, new_time_stamp, memory_size, kvs);
            min_key = UINT_MAX; max_key = 0;
            num = 0;
            TAG++;
            memory_size = 10272;
            kvs.clear();
        }
        if(key < min_key) min_key = key;
        if(key > max_key) max_key = key;
        num++;
        memory_size += 12 + value.size();
        kvs.emplace_back(make_pair(key, value));
    }
    if(num > 0){
        write(level, num, min_key, max_key, new_time_stamp, memory_size, kvs);
        TAG++;
    }
}


void KVStore::compaction(uint64_t level_x, uint64_t level_y) {
    //check if level_x is empty
    if(sstable[level_x].empty()) return;
    //check if level_y config exists
    if(!level_mode.count(level_y)){
        uint64_t limits_number = level_mode[level_x].first * 2;
        level_mode[level_y] = make_pair(limits_number, Leveling);
    }
    //check if level_x file nums is greater than limits
    uint64_t limits = level_mode[level_x].first;
    if(sstable[level_x].size() <= limits) return;

    //i select sstable
    vector<pair<uint64_t, uint64_t>> x_select_files, y_select_files;  //time_stamp, tag
    select_file(x_select_files, y_select_files, level_x, level_y);

    //ii merge sstable
    //0 set new time stamp
    uint64_t new_time_stamp = 1;
    //1 set a new sstable for files to be compacted
    map<uint64_t, map<pair<uint64_t, uint64_t>, sstable_cache*>> temp_sstable; //level, time_stamp, tag, sstable
    for(auto& time_tag_files : x_select_files){
        uint64_t cur_timestamp = time_tag_files.first;
        uint64_t cur_tag = time_tag_files.second;
        temp_sstable[level_x][make_pair(cur_timestamp, cur_tag)] = sstable[level_x][make_pair(cur_timestamp, cur_tag)];
        new_time_stamp = max(new_time_stamp, cur_timestamp);
    }
    for(auto& time_tag_files : y_select_files){
        uint64_t cur_timestamp = time_tag_files.first;
        uint64_t cur_tag = time_tag_files.second;
        temp_sstable[level_y][make_pair(cur_timestamp, cur_tag)] = sstable[level_y][make_pair(cur_timestamp, cur_tag)];
        new_time_stamp = max(new_time_stamp, cur_timestamp);
    }

    //2 read key_value;
    map<uint64_t, pair<pair<uint64_t, uint64_t>, string>> key_value;  //key, <time_stamp&tag, value>
    set<uint64_t> keys;
    for(auto time_tag_files : temp_sstable[level_x]){
            sstable_cache* cur_sstable = time_tag_files.second;
            //read key value pairs
            read_key_value_from_disk(level_x, const_cast<uint64_t &>(time_tag_files.first.first),
                                     const_cast<uint64_t &>(time_tag_files.first.second), cur_sstable, key_value, keys);
    }
    for(auto time_tag_files : temp_sstable[level_y]){
            sstable_cache* cur_sstable = time_tag_files.second;
            if(cur_sstable == nullptr) continue;
            //read key value pairs
            read_key_value_from_disk(level_y, const_cast<uint64_t &>(time_tag_files.first.first),
                                     const_cast<uint64_t &>(time_tag_files.first.second), cur_sstable, key_value, keys);
    }

    //3 delete old files
//    std::cout << "deleted files: ";
    for(const auto& time_tag_files : temp_sstable[level_x]){
            //delete file
            string file_path = dir_name + "/level-" + to_string(level_x) + "/" + to_string(time_tag_files.first.first) + "_" + to_string(time_tag_files.first.second) + ".sst";
            utils::rmfile(file_path.c_str());
            sstable[level_x].erase(make_pair(time_tag_files.first.first, time_tag_files.first.second));
    }
    for(const auto& time_tag_files : temp_sstable[level_y]){
        //delete file
        string file_path = dir_name + "/level-" + to_string(level_y) + "/" + to_string(time_tag_files.first.first) + "_" + to_string(time_tag_files.first.second) + ".sst";
        utils::rmfile(file_path.c_str());
        sstable[level_y].erase(make_pair(time_tag_files.first.first, time_tag_files.first.second));
    }

    //4 write merged sstable to disk

    string level_y_path = dir_name + "/level-" + to_string(level_y);
    if(!utils::dirExists(level_y_path)) utils::mkdir(level_y_path.c_str());
    compaction_write(key_value, level_y, new_time_stamp);

    //iii recursive compaction
    compaction(level_y, level_y + 1);
}


/**
 * This function is used to read mode config from disk when the kvstore is initialized.
 */
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