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


/*
  get value by key
 */
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

/*
 put key-value pair into skiplist
 */
void skiplist::put(uint64_t key, const string& s){
    node<uint64_t , string>* store[max_level + 1];  //update array
    memset(store, 0, sizeof(node<uint64_t , string>*)*(max_level + 1));
    //copy head to store
    node<uint64_t , string>* cur = head;
    for(int i = cur_level; i >= 0; --i){
        while(cur->next[i] && (cur->next[i]->get_key() < key)){
            cur = cur->next[i];
        }
        store[i] = cur;
    }

    cur = cur->next[0];  //level 0

    //if exists same key
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

    //if not exists same key, generate a random level for new key
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
    }

    //offset: uint32_t  key:uint64_t
    memory_size += s.size() + 12;
    length++;
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

/*
 * insert all key-value pairs to bloomfilter, and store bloomfilter to sstable_cache
 */
void skiplist::store_bloomfilter(sstable_cache *&ssc) const {
    node<uint64_t , string>* cur = head->next[0];
    while(cur){
        uint64_t cur_key = cur->get_key();
        ssc->insert_key_to_bloomfilter(cur_key);
        cur = cur->next[0];
    }
}

skiplist::~skiplist(){
    delete head;
    memory_size = 10272;
    cur_level = 0;
    length = 0;
}