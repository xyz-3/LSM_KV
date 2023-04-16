//
// Created by yichen_X on 2023/4/1.
//

#include "sstable.h"

sstable_cache::sstable_cache(){
    time_stamp = 0;
    pair_num = 0;
    min_key = 0;
    max_key = 0;
    bloomfilter = new class bloomfilter();
}

sstable_cache::~sstable_cache(){

}

bool sstable_cache::is_in_range(uint64_t &key) {
    return (key >= min_key && key <= max_key);
}

index_item* sstable_cache::find_key_in_indexs(const uint64_t key, uint32_t& value_size){
    //binary search
    int left = 0, right = Indexs.size() - 1;
    while(left <= right){
        int mid = (left + right) / 2;
        if(Indexs[mid].get_key() == key) {
            if(mid == pair_num - 1){
                 value_size = 0;
            }else{
                value_size = Indexs[mid + 1].get_offset() - Indexs[mid].get_offset();
            }
            return &Indexs[mid];
        }
        else if(Indexs[mid].get_key() < key) left = mid + 1;
        else right = mid - 1;
    }

    value_size = 0;
    return nullptr;
}