//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  std::scoped_lock<std::mutex> lock(latch_);
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // 1 获取evictable frame
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }
  // 2 刷盘
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  // 3 分配page_id
  *page_id = AllocatePage();
  // 4 移除old_page_id，增加new_page_id映射
  page_table_->Remove(pages_[frame_id].page_id_);
  page_table_->Insert(*page_id, frame_id);
  // 5 重置page
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 1;

  // 6 访问该frame_id，并设置为不可淘汰
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // 1 判断是否命中缓存
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // 未命中则淘汰帧
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  } else {
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];  // 命中则直接返回
  }

  // 2 脏页刷盘
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  // 3 重置page
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();
  // 4 从disk读取page数据
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  // 5 访问该frame_id，并设置为不可淘汰
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  page_table_->Insert(page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  // 1 如果该page不在buffer pool 或者 pincount==0
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  // 2 设置脏位
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  // 3 设置为可淘汰
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // 1 如果该page不在buffer pool 或者 pincount==0
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // 2 刷盘，不管是否脏页
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
