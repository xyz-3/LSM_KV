//
// Created by yichen_X on 2023/4/1.
//

#include "skiplist.h"

skiplist::skiplist(int maxlevel){
    max_level = maxlevel;
    cur_level = 0;
    length = 0;
    memory_size = 10272;

    head = new node<uint64_t , string>(0, "", max_level);
}

string skiplist::get(uint64_t key) const{
    node<uint64_t , string>* cur = head;
    for(int i = cur_level - 1; i >= 0; --i){
        while(cur->next[i] && (cur->next[i]->get_key() < key)){
            cur = cur->next[i];
        }
    }

    if(cur->next[0] && (cur->next[0]->get_key() == key)){
        return cur->next[0]->get_value();
    }

    return "";
}

void skiplist::put(uint64_t key, const string& s){
    node<uint64_t , string>* store[max_level + 1];
    memset(store, 0, sizeof(node<uint64_t , string>*)*(max_level + 1));
    //copy head to store
//    memcpy(store, head, sizeof(node<uint64_t , string>*)*(max_level + 1));

    node<uint64_t , string>* cur = head;
    for(int i = cur_level; i >= 0; --i){
        while(cur->next[i] && (cur->next[i]->get_key() < key)){
            cur = cur->next[i];
        }
        store[i] = cur;
    }

    cur = cur->next[0];

    //have same key
    if(cur && key == cur->get_key()){
        if(s == cur->get_value()) return;

        const string old_s = cur->get_value();
        memory_size += s.size() - old_s.size();

        int tmp_level = cur_level;
        if(store[0] && store[0]->next[0] && store[0]->next[0]->level < tmp_level){
            tmp_level = store[0]->next[0]->level;
        }

        for(int i = 0; i < tmp_level; ++i){
            store[i]->next[i]->set_value(s);
        }
        return;
    }


    int ran_level = random_level();
    if(ran_level > cur_level){
        for (int i = cur_level + 1; i < ran_level + 1; i++) {
            store[i] = head;
        }
        cur_level = ran_level;
    }
    auto* new_node = new node<uint64_t , string>(key, s, ran_level);

    for(int i = 0; i < ran_level; ++i){
        new_node->next[i] = store[i]->next[i];
        store[i]->next[i] = new_node;
//        new_node->prev = store[i];
    }

    //offset: uint32_t  key:uint64_t
    memory_size += s.size() + 12;
    length++;
}

bool skiplist::del(uint64_t key){
    string s = get(key);
    if(s == "" || s == "~DELETED~") return false;

    put(key, "~DELETED~");
    return true;
}

int skiplist::random_level(){
    int random_level = 1;
    while (dis(gen) < 0.5 && random_level < max_level) {
        random_level++;
    }
    return random_level;
}

uint64_t skiplist::get_size() const{
    return memory_size;
}

uint64_t skiplist::get_length() const{
    return length;
}

uint64_t skiplist::get_min_key() const {
    node<uint64_t , string>* cur = head->next[0];
    uint64_t min_key = cur->get_key();
    return min_key;
}

uint64_t skiplist::get_max_key() const {
    node<uint64_t , string>* cur = head->next[0];
    while(cur->next[0]){
        cur = cur->next[0];
    }
    uint64_t max_key = cur->get_key();
    return max_key;
}

void skiplist::store_bloomfilter(sstable_cache *&ssc) {
    node<uint64_t , string>* cur = head->next[0];
    while(cur){
        uint64_t cur_key = cur->get_key();
        ssc->insert_key_to_bloomfilter(cur_key);
        cur = cur->next[0];
    }
}

/*
sstable_cache* skiplist::store_memtable(const string& dir, const uint64_t time){

    sstable_cache* new_sstable_cache = new sstable_cache();

    if(!utils::dirExists(dir)){
        utils::mkdir(dir.c_str());
    }

    */
/* File Path & add time stamp *//*

    string file_path = dir + "/" + to_string(time) + ".sst";
    ofstream file;
    file.open(file_path, ios_base::out|ios_base::binary);

    // file open failed
    if(!file.is_open()){
        return nullptr;
    }

    */
/* set time stamp *//*

    new_sstable_cache->set_timestamp(time);
    file.write((char*)&time, sizeof(uint64_t));

    */
/* set total num *//*

    new_sstable_cache->set_num(length);
    file.write((const char*)&length, sizeof(uint32_t));

    */
/* set min & max key *//*

    node* cur = head->next[0];
    uint64_t min_key = cur->get_key();
    new_sstable_cache->set_minkey(min_key);
    file.write((const char*)&min_key, sizeof(uint64_t));

    while(cur->next[0]){
        cur = cur->next[0];
    }
    uint64_t max_key = cur->get_key();
    new_sstable_cache->set_maxkey(max_key);
    file.write((const char*)&max_key, sizeof(uint64_t));

    */
/* BloomFilter *//*

    cur = head->next[0];
    while(cur->next[0]){
        uint64_t cur_key = cur->get_key();
        new_sstable_cache->insert_key_to_bloomfilter(cur_key);
        cur = cur->next[0];
    }
    file.write((const char*)&new_sstable_cache->bloomfilter->bitset, new_sstable_cache->bloomfilter->bitset_size);

    */
/* Index Area *//*

    uint32_t offset = 10272 + 12 * length;
    cur = head->next[0];
    while(cur->next[0]){
        uint64_t cur_key = cur->get_key();
        new_sstable_cache->Indexs.push_back(index_item(cur_key, offset));
        file.write((const char*)&cur_key, sizeof(uint64_t));
        file.write((const char*)&offset, sizeof(uint32_t));
        offset += cur->get_value().size();
        cur = cur->next[0];
    }

    */
/* Value Area *//*

    cur = head->next[0];
    while(cur->next[0]){
        string cur_value = cur->get_value();
        auto cur_v_str = cur_value.c_str();
        file.write((char*)(cur_v_str), sizeof(cur_value));
    }

    file.close();
    return new_sstable_cache;
}
*/

skiplist::~skiplist(){
//    auto cur = head;
//    while(cur){
//        auto tmp = cur;
//        cur = cur->next[0];
//        delete tmp;
//    }
//    head = nullptr;
    delete head;
    memory_size = 10272;
    cur_level = 0;
    length = 0;
}