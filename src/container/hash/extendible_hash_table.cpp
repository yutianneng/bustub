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

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto bucket = new Bucket(bucket_size_);
  dir_.push_back(std::shared_ptr<Bucket>(bucket));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Hash(const K &key) -> size_t {
  return std::hash<K>()(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
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
  size_t index = IndexOf(key);
  // 计算bucket index
  auto bucket = dir_[index];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  size_t index = IndexOf(key);
  // 计算bucket index
  auto bucket = dir_[index];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //  UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  size_t index = IndexOf(key);
  // 计算bucket index
  auto bucket = dir_[index];
  if (!bucket->IsFull()) {
    bucket->Insert(key, value);
    return;
  }
  if (GetLocalDepthInternal(index) == GetGlobalDepthInternal()) {
    // 扩容目录
    GrowGlobal(index);
    index = IndexOf(key);
  }
  if (GetLocalDepthInternal(index) >= GetGlobalDepthInternal()) {
    return;
  }
  GrowLocal(index);

  latch_.unlock();
  // 递归插入本来要插入的值，
  // 因为扩容后，bucket还是有可能是满的
  Insert(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::GrowGlobal(size_t index) {
  size_t sz = dir_.size();
  // 目录扩容
  dir_.reserve(2 * sz);
  std::copy_n(dir_.begin(), sz, std::back_inserter(dir_));
  global_depth_++;
}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::GrowLocal(size_t index) {
  // 只扩容这个bucket
  auto bucket = dir_[index];
  size_t local_depth = GetLocalDepthInternal(index);
  // 扩容后会增加一位来分桶，mask就是比较该位是否为1，
  size_t mask = (1 << local_depth);

  auto new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
  auto old_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
  // bucket
  auto iter = bucket->GetItems().begin();
  auto end = bucket->GetItems().end();
  for (; iter != end; iter++) {
    size_t idx = Hash(iter->first) & mask;
    if (idx != 0) {
      new_bucket->Insert(iter->first, iter->second);
    } else {
      old_bucket->Insert(iter->first, iter->second);
    }
  }
  // 指向原bucket的目录项指向new bucket
  for (size_t i = index & (mask - 1); i < dir_.size(); i += mask) {
    if ((i & mask) != 0) {
      dir_[i] = new_bucket;
    } else {
      dir_[i] = old_bucket;
    }
  }
  num_buckets_++;
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto const &kv : list_) {
    if (kv.first == key) {
      value = kv.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  auto iter = list_.begin();
  for (; iter != list_.end(); iter++) {
    if (iter->first == key) {
      break;
    }
  }
  if (iter != list_.end()) {
    iter->second = value;
  } else {
    list_.push_back({key, value});
  }
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
