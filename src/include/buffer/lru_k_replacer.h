//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time
 * between current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward
 * k-distance, classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be
   * required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that
   * frame. Only frames that are marked as 'evictable' are candidates for
   * eviction.
   *
   * A frame with less than k historical references is given +inf as its
   * backward k-distance. If multiple frames have inf backward k-distance,
   * then evict the frame with the earliest timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and
   * remove the frame's access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can
   * be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current
   * timestamp. Create a new entry for access history if frame id has not been
   * seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an
   * exception. You can also use BUSTUB_ASSERT to abort the process if frame
   * id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This
   * function also controls replacer's size. Note that size is equal to number
   * of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable,
   * then size should decrement. If a frame was previously non-evictable and
   * is to be set to evictable, then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying
   * anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access
   * history. This function should also decrement replacer's size if removal
   * is successful.
   *
   * Note that this is different from evicting a frame, which always remove
   * the frame with largest backward k-distance. This function removes
   * specified frame id, no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort
   * the process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable
   * frames.
   * 返回可淘汰帧的数量
   * @return size_t
   */
  auto Size() -> size_t;
  auto KDistance(frame_id_t frame_id) -> int;

  class FrameInfo {
   private:
    frame_id_t frame_id_;
    bool evictable_;
    std::list<int> k_accesstime_list_;
    std::list<std::shared_ptr<FrameInfo>>::iterator iter_;  // 记录该frame在frameinfo_list的迭代器

   public:
    explicit FrameInfo(frame_id_t frame_id);
    FrameInfo() = default;
    inline auto GetFrameID() -> frame_id_t { return frame_id_; }
    inline auto SetFrameID(frame_id_t id) { frame_id_ = id; }

    inline auto GetEvictable() -> bool { return evictable_; }
    inline void SetEvictable(bool set_evictable) { evictable_ = set_evictable; }
    inline auto GetHistory() -> std::list<int> & { return k_accesstime_list_; }
    inline auto GetIter() -> std::list<std::shared_ptr<FrameInfo>>::iterator { return iter_; }
    inline void SetIter(std::list<std::shared_ptr<FrameInfo>>::iterator it) { iter_ = it; }
  };

 private:
  // TODO(student): implement me! You can replace these member variables as
  // you like. Remove maybe_unused if you start using them.
  [[maybe_unused]] size_t curr_size_{0};   // evictable frame number
  [[maybe_unused]] size_t replacer_size_;  // 等于buffer_pool的size
  [[maybe_unused]] size_t k_;
  std::map<frame_id_t, std::shared_ptr<FrameInfo>> frameinfo_map_;  // 用于快速所有frame的状态
  std::list<std::shared_ptr<FrameInfo>> frameinfo_list_;

  int ticks_{0};  // 记录时间
  std::mutex latch_;
};

}  // namespace bustub
