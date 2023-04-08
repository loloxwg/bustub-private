//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  auto directory_index = IndexOf(key);
  auto target_bucket = dir_[directory_index];
  return static_cast<bool>(target_bucket->Find(key, value));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  auto directory_index = IndexOf(key);
  auto target_bucket = dir_[directory_index];
  return static_cast<bool>(target_bucket->Remove(key));
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto directory_index = IndexOf(key);
  auto target_bucket = dir_[directory_index];

  while (!target_bucket->Insert(key, value)) {
    // 1. If the local depth of the bucket is equal to the global depth,
    //  increment the global depth and double the size of the directory.
    // LOG_DEBUG("stage1: dir_.size() =  %ld", dir_.size());
    if (GetLocalDepth(directory_index) == GetGlobalDepth()) {
      global_depth_++;
      LOG_DEBUG("start loop.. dir_.size() =  %ld", dir_.size());
      int length = dir_.size();
      dir_.resize(length << 1);  // <<1 is same as *2
      for (int i = 0; i < length; i++) {
        dir_[i + length] = dir_[i];  // length =4
                                     //  dir[4]=dir[0] 100 000
                                     //  dir[5]=dir[1] 101 001
                                     //  dir[6]=dir[2] 110 010
                                     //  dir[7]=dir[3] 111 011
      }
    }
    // LOG_DEBUG("stage2: Increment the local depth of the bucket.");
    // 2. If local depth lower than global depth , Increment the local depth of the bucket.
    target_bucket->IncrementDepth();

    // 3. Split the bucket and redistribute
    //  directory pointers & the kv pairs in the bucket.
    // LOG_DEBUG("stage3:  Split the bucket and redistribute");
    auto mask = 1 << (target_bucket->GetDepth() - 1);
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth());
    auto old_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth());
    num_buckets_++;
    // 3.1. Redistribute the directory pointers.
    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) == 0) {
          dir_[i] = new_bucket;
        } else {
          dir_[i] = old_bucket;
        }
      }
    }
    // 3.2. Move the kv pairs from the target bucket to the new two bucket.
    for (auto &item : target_bucket->GetItems()) {
      auto cur_directory_index = IndexOf(item.first);
      dir_[cur_directory_index]->Insert(item.first, item.second);
    }
    directory_index = IndexOf(key);
    target_bucket = dir_[directory_index];
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      value = (*it).second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      (*it).second = value;
      // std::cout << "insert_update "<< std::endl;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(std::make_pair(key, value));
  // std::cout << "insert_new "<< std::endl;
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
