//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::KDistance(frame_id_t frame_id) -> int {
  auto frameinfo = frameinfo_map_.find(frame_id);
  if (frameinfo == frameinfo_map_.end()) {
    return -1;
  }
  if (frameinfo->second->GetHistory().size() < k_) {
    return INT32_MAX;
  }
  // 最后一个是最新的访问时间，第一个是最旧的访问时间
  return *frameinfo->second->GetHistory().rbegin() - (*frameinfo->second->GetHistory().begin());
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  int k_distance = 0;
  auto iter = frameinfo_list_.begin();
  auto evict_iter = frameinfo_list_.end();
  for (; iter != frameinfo_list_.end(); ++iter) {
    // 过滤不可淘汰帧
    if (!iter->get()->GetEvictable()) {
      continue;
    }
    int kdis = KDistance(iter->get()->GetFrameID());
    if (kdis == -1) {
      continue;
    }
    // 头部是最久访问的，尾部是最近访问的，如果都为INT32_MAX，那优先淘汰掉最久访问
    if (evict_iter == frameinfo_list_.end() || kdis > k_distance ||
        (kdis == k_distance && *iter->get()->GetHistory().begin() < *evict_iter->get()->GetHistory().begin())) {
      evict_iter = iter;
      k_distance = kdis;
    }
  }
  *frame_id = evict_iter->get()->GetFrameID();
  frameinfo_list_.erase(evict_iter);
  frameinfo_map_.erase(*frame_id);
  curr_size_--;
  return true;
}
// 在pinned之后使用，会加入new frame
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // replacer_size_等于buffer_pool_size，frame_id从0开始
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "frame_id invalid!!!");
  auto iter = frameinfo_map_.find(frame_id);
  // 如果命中，直接更新状态即可
  if (iter != frameinfo_map_.end()) {
    if (iter->second->GetHistory().size() == k_) {
      iter->second->GetHistory().pop_front();
    }
    // list尾部是最近访问时间
    iter->second->GetHistory().push_back(ticks_++);
    // 放到尾部，表示最近访问的，优先不淘汰
    frameinfo_list_.erase(iter->second->GetIter());
    frameinfo_list_.push_back(iter->second);
    iter->second->SetIter(--frameinfo_list_.end());
    return;
  }
  // 增加新帧
  auto frameinfo = std::make_shared<FrameInfo>();
  frameinfo->GetHistory().push_back(ticks_++);
  frameinfo->SetFrameID(frame_id);

  frameinfo_list_.push_back(frameinfo);
  frameinfo->SetIter(--frameinfo_list_.end());
  frameinfo_map_[frame_id] = frameinfo;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 会影响可淘汰帧的数量
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "frame_id is invalid!!!");
  auto iter = frameinfo_map_.find(frame_id);
  BUSTUB_ASSERT(iter != frameinfo_map_.end(), "frame id not found!!!");

  auto frameinfo = iter->second;
  // 如果该frame原本是可淘汰的，但现在设置为不可淘汰
  if (frameinfo->GetEvictable() && !set_evictable) {
    frameinfo->SetEvictable(set_evictable);
    curr_size_--;
  } else if (!frameinfo->GetEvictable() && set_evictable) {
    // 原本不可淘汰，现在设置为可淘汰
    frameinfo->SetEvictable(set_evictable);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "frame_id is invalid!!!");
  auto iter = frameinfo_map_.find(frame_id);
  if (iter == frameinfo_map_.end()) {
    return;
  }
  BUSTUB_ASSERT(iter->second->GetEvictable(), "the frame not evictable!!!");
  frameinfo_list_.erase(iter->second->GetIter());
  frameinfo_map_.erase(iter);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

LRUKReplacer::FrameInfo::FrameInfo(frame_id_t frame_id) : frame_id_{frame_id}, evictable_{false} {}

}  // namespace bustub
